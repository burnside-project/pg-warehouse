"""
AI Q&A Engine — translates natural language questions into DuckDB SQL
using the data model as grounding context, then formats answers.

The data model YAML files describe every entity, column, metric, synonym,
and business rule. This structured knowledge is the "moat" that makes AI
answers accurate and domain-aware.

Flow:
  1. Load data model YAMLs → build system prompt
  2. User asks a question in natural language
  3. Claude generates a DuckDB SQL query grounded in the model
  4. Execute SQL against Parquet files via DuckDB
  5. Claude formats the result into a natural language answer
"""

import os
import glob
import json
import yaml
import duckdb
import anthropic


MODELS_DIR = os.environ.get(
    "MODELS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "models"),
)
DATA_DIR = os.environ.get("DATA_DIR", "/data")


def load_data_model():
    """Load all YAML model files and build a structured context string."""
    sections = []

    # Load manifest
    manifest_path = os.path.join(MODELS_DIR, "manifest.yml")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
        sections.append("# Data Model: " + manifest.get("name", "unknown"))
        sections.append(manifest.get("description", ""))

        # Business rules
        rules = manifest.get("business_rules", [])
        if rules:
            sections.append("\n## Business Rules")
            for rule in rules:
                sections.append(
                    f"- **{rule['name']}**: {rule['description']}"
                )

        # Relationships
        rels = manifest.get("relationships", [])
        if rels:
            sections.append("\n## Relationships")
            for rel in rels:
                sections.append(
                    f"- {rel['name']}: {rel['description']} "
                    f"({rel['from']['entity']}.{rel['from']['key']} → "
                    f"{rel['to']['entity']}.{rel['to']['key']}, {rel['type']})"
                )

    # Load entity definitions
    entity_files = sorted(glob.glob(os.path.join(MODELS_DIR, "entities", "*.yml")))
    for path in entity_files:
        with open(path) as f:
            entity = yaml.safe_load(f)
        sections.append(f"\n## Entity: {entity['display_name']}")
        sections.append(f"Table: `{entity['table']}`")
        sections.append(f"Grain: {entity['grain']}")
        sections.append(f"Description: {entity['description']}")
        sections.append("\nColumns:")
        for col in entity.get("columns", []):
            synonyms = col.get("synonyms", [])
            syn_str = f" (also known as: {', '.join(synonyms)})" if synonyms else ""
            enum_str = f" Values: {col.get('enum')}" if col.get("enum") else ""
            nullable = " [nullable]" if col.get("nullable") else ""
            unit = f" [{col['unit']}]" if col.get("unit") else ""
            sections.append(
                f"  - `{col['name']}` ({col['type']}): "
                f"{col['description']}{unit}{enum_str}{syn_str}{nullable}"
            )

    # Load metric definitions
    metric_files = sorted(glob.glob(os.path.join(MODELS_DIR, "metrics", "*.yml")))
    for path in metric_files:
        with open(path) as f:
            metrics = yaml.safe_load(f)
        sections.append(f"\n## Metrics: {metrics['display_name']}")
        sections.append(f"Table: `{metrics['table']}`")
        sections.append(f"Grain: {metrics['grain']}")

        if metrics.get("time_column"):
            sections.append(f"Time column: `{metrics['time_column']}`")

        dims = metrics.get("dimensions", [])
        if dims:
            sections.append("\nDimensions:")
            for d in dims:
                synonyms = d.get("synonyms", [])
                syn_str = f" (also: {', '.join(synonyms)})" if synonyms else ""
                sections.append(f"  - `{d['name']}`: {d['description']}{syn_str}")

        measures = metrics.get("measures", [])
        if measures:
            sections.append("\nMeasures:")
            for m in measures:
                synonyms = m.get("synonyms", [])
                syn_str = f" (also: {', '.join(synonyms)})" if synonyms else ""
                unit = f" [{m['unit']}]" if m.get("unit") else ""
                agg = f" [agg: {m['aggregation']}]" if m.get("aggregation") else ""
                sections.append(
                    f"  - `{m['name']}`: {m['description']}{unit}{agg}{syn_str}"
                )

        derived = metrics.get("derived_metrics", [])
        if derived:
            sections.append("\nDerived metrics:")
            for dm in derived:
                unit = f" [{dm['unit']}]" if dm.get("unit") else ""
                sections.append(
                    f"  - `{dm['name']}`: {dm['description']}{unit} "
                    f"= `{dm['formula']}`"
                )

    return "\n".join(sections)


def build_system_prompt(model_context):
    """Build the system prompt that grounds the AI in the data model."""
    return f"""You are an analytics AI assistant for an e-commerce data warehouse.
You answer business questions by generating DuckDB SQL queries against Parquet files.

IMPORTANT RULES:
1. ONLY query the feat.* tables listed below — they are the analytics-ready layer.
2. Use DuckDB SQL syntax (not PostgreSQL).
3. The Parquet files are registered as views with these names:
   - sales_summary (from feat.sales_summary)
   - customer_analytics (from feat.customer_analytics)
   - product_performance (from feat.product_performance)
   - promotion_effectiveness (from feat.promotion_effectiveness)
   - inventory_health (from feat.inventory_health)
4. Use the column names EXACTLY as defined in the data model below.
5. Pay attention to synonyms — users may say "revenue" when they mean "gross_revenue" or "lifetime_revenue" depending on context.
6. Apply business rules when relevant (segmentation thresholds, LTV formulas, etc.).
7. For time-based questions, use DuckDB date functions (DATE_TRUNC, DATE_DIFF, CURRENT_DATE, INTERVAL).
8. Keep queries simple and readable. Prefer CTEs over subqueries.
9. Limit results to 20 rows unless the user asks for more.
10. When aggregating across tables, be careful about the grain — don't double-count.

{model_context}
"""


def get_parquet_db():
    """Create a DuckDB connection with Parquet views."""
    db = duckdb.connect(":memory:")
    parquet_files = {
        "sales_summary": "sales_summary.parquet",
        "customer_analytics": "customer_analytics.parquet",
        "product_performance": "product_performance.parquet",
        "promotion_effectiveness": "promotion_effectiveness.parquet",
        "inventory_health": "inventory_health.parquet",
    }
    for table, filename in parquet_files.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            db.execute(
                f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
            )
    return db


class AIQueryEngine:
    """AI-powered Q&A engine that translates questions to SQL and back."""

    def __init__(self):
        self.model_context = load_data_model()
        self.system_prompt = build_system_prompt(self.model_context)
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.client = anthropic.Anthropic(api_key=api_key) if api_key else None

    def is_available(self):
        """Check if the AI engine is configured."""
        return self.client is not None

    def ask(self, question):
        """
        Answer a natural language question about the e-commerce data.

        Returns:
            dict with keys: question, sql, data, answer, error
        """
        if not self.is_available():
            return {
                "question": question,
                "sql": None,
                "data": None,
                "answer": None,
                "error": "ANTHROPIC_API_KEY not set. Set the environment variable to enable AI Q&A.",
            }

        # Step 1: Generate SQL
        sql = self._generate_sql(question)
        if not sql:
            return {
                "question": question,
                "sql": None,
                "data": None,
                "answer": "I couldn't generate a query for that question. Could you rephrase it?",
                "error": None,
            }

        # Step 2: Execute SQL
        try:
            db = get_parquet_db()
            result = db.execute(sql).fetchall()
            columns = [desc[0] for desc in db.description]
            data = [dict(zip(columns, row)) for row in result]
        except Exception as e:
            return {
                "question": question,
                "sql": sql,
                "data": None,
                "answer": None,
                "error": f"Query execution failed: {str(e)}",
            }

        # Step 3: Generate natural language answer
        answer = self._generate_answer(question, sql, columns, data)

        return {
            "question": question,
            "sql": sql,
            "data": data[:50],  # Cap data payload
            "answer": answer,
            "error": None,
        }

    def _generate_sql(self, question):
        """Use Claude to generate a DuckDB SQL query from a natural language question."""
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=self.system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Generate a DuckDB SQL query to answer this question:\n\n"
                            f"\"{question}\"\n\n"
                            f"Return ONLY the SQL query, no explanation. "
                            f"Do not wrap it in markdown code fences."
                        ),
                    }
                ],
            )
            sql = response.content[0].text.strip()
            # Clean up any markdown fences if present
            if sql.startswith("```"):
                lines = sql.split("\n")
                sql = "\n".join(
                    line for line in lines
                    if not line.startswith("```")
                ).strip()
            return sql
        except Exception as e:
            print(f"SQL generation error: {e}")
            return None

    def _generate_answer(self, question, sql, columns, data):
        """Use Claude to format query results into a natural language answer."""
        if not data:
            return "The query returned no results."

        # Truncate data for context window
        data_preview = data[:20]
        data_str = json.dumps(data_preview, indent=2, default=str)

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=(
                    "You are an analytics assistant. Format query results into "
                    "clear, concise natural language answers. Use specific numbers. "
                    "If there's a table of results, format it readably. "
                    "Keep answers under 200 words."
                ),
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Question: \"{question}\"\n\n"
                            f"SQL query used:\n{sql}\n\n"
                            f"Results ({len(data)} rows, showing first {len(data_preview)}):\n"
                            f"Columns: {columns}\n"
                            f"{data_str}\n\n"
                            f"Provide a clear answer to the question based on these results."
                        ),
                    }
                ],
            )
            return response.content[0].text.strip()
        except Exception as e:
            # Fallback: return raw data summary
            return f"Query returned {len(data)} rows. First result: {json.dumps(data[0], default=str)}"

    def get_suggested_questions(self):
        """Return a curated list of example questions organized by domain."""
        return {
            "Sales": [
                "What is our total revenue this month?",
                "What is the average order value trend over the last 90 days?",
                "Which payment method is most popular?",
                "What percentage of orders are delivered?",
                "How much revenue comes from orders with coupons?",
            ],
            "Customers": [
                "How many loyal customers do we have?",
                "What is the churn rate?",
                "Who are the top 5 customers by lifetime value?",
                "Which country generates the most revenue?",
                "What is the average customer lifetime value?",
            ],
            "Products": [
                "What are the top 10 products by revenue?",
                "Which categories have the highest average rating?",
                "Show products with high sales but low ratings",
                "How many products are out of stock?",
                "What is the average revenue per product?",
            ],
            "Promotions": [
                "Which promotion gave the most total discount?",
                "How many active promotions do we have?",
                "What is the average discount per coupon use?",
                "Which promotions reached the most customers?",
                "Show promotions that expired without being used",
            ],
            "Inventory": [
                "Which products urgently need reordering?",
                "How many products have less than 7 days of stock?",
                "What is the total value of available inventory?",
                "Show high-selling products with low inventory",
                "What is the sell-through rate by category?",
            ],
        }
