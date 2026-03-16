// demo.go - Zero-dependency demo of pg-warehouse capabilities
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/burnside-project/pg-warehouse/internal/adapters/duckdb"
	"github.com/spf13/cobra"
)

var demoKeep bool

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Run a zero-dependency demo of pg-warehouse",
	Long: `Runs a self-contained demo that creates a DuckDB warehouse with sample
e-commerce data (customers, orders, products), runs feature queries, and
exports results to Parquet. No PostgreSQL connection is needed.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		// ── Welcome banner ──────────────────────────────────────────────
		fmt.Println("=======================================================")
		fmt.Println("  pg-warehouse demo")
		fmt.Println("=======================================================")
		fmt.Println()
		fmt.Println("This demo will:")
		fmt.Println("  1. Create a temporary DuckDB warehouse")
		fmt.Println("  2. Load sample e-commerce data (customers, orders, products)")
		fmt.Println("  3. Run a feature query (customer lifetime value)")
		fmt.Println("  4. Export results to Parquet")
		fmt.Println()

		// ── Create temp directory ───────────────────────────────────────
		tmpDir, err := os.MkdirTemp("", "pg-warehouse-demo-*")
		if err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		if !demoKeep {
			defer os.RemoveAll(tmpDir)
		}
		fmt.Printf("[1/6] Created temp directory: %s\n", tmpDir)

		// ── Init DuckDB ─────────────────────────────────────────────────
		dbPath := filepath.Join(tmpDir, "demo.duckdb")
		wh := duckdb.NewWarehouse(dbPath)
		if err := wh.Open(ctx); err != nil {
			return fmt.Errorf("failed to open warehouse: %w", err)
		}
		defer wh.Close()

		if err := wh.Bootstrap(ctx); err != nil {
			return fmt.Errorf("failed to bootstrap warehouse: %w", err)
		}
		fmt.Println("[2/6] Initialized DuckDB warehouse")

		// ── Insert sample data ──────────────────────────────────────────
		if err := insertSampleCustomers(ctx, wh); err != nil {
			return fmt.Errorf("failed to insert customers: %w", err)
		}
		if err := insertSampleProducts(ctx, wh); err != nil {
			return fmt.Errorf("failed to insert products: %w", err)
		}
		if err := insertSampleOrders(ctx, wh); err != nil {
			return fmt.Errorf("failed to insert orders: %w", err)
		}

		custCount, _ := wh.CountRows(ctx, "raw.customers")
		prodCount, _ := wh.CountRows(ctx, "raw.products")
		orderCount, _ := wh.CountRows(ctx, "raw.orders")
		fmt.Printf("[3/6] Loaded sample data: %d customers, %d products, %d orders\n",
			custCount, prodCount, orderCount)

		// ── Run feature query ───────────────────────────────────────────
		featureSQL := `
CREATE TABLE feat.customer_lifetime_value AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.segment,
    COUNT(DISTINCT o.id) AS total_orders,
    ROUND(SUM(o.quantity * p.price), 2) AS lifetime_value,
    ROUND(AVG(o.quantity * p.price), 2) AS avg_order_value,
    MIN(o.ordered_at) AS first_order,
    MAX(o.ordered_at) AS last_order,
    DATEDIFF('day', MIN(o.ordered_at), MAX(o.ordered_at)) AS customer_tenure_days
FROM raw.customers c
JOIN raw.orders o ON o.customer_id = c.id
JOIN raw.products p ON p.id = o.product_id
GROUP BY c.id, c.name, c.segment
ORDER BY lifetime_value DESC
`
		if err := wh.ExecuteSQL(ctx, featureSQL); err != nil {
			return fmt.Errorf("failed to run feature query: %w", err)
		}
		featCount, _ := wh.CountRows(ctx, "feat.customer_lifetime_value")
		fmt.Printf("[4/6] Computed feature: customer_lifetime_value (%d rows)\n", featCount)

		// ── Preview results ─────────────────────────────────────────────
		fmt.Println("[5/6] Preview of feat.customer_lifetime_value:")
		fmt.Println()
		previewRows, err := wh.QueryRows(ctx, "SELECT customer_id, customer_name, segment, total_orders, lifetime_value, avg_order_value FROM feat.customer_lifetime_value", 10)
		if err != nil {
			return fmt.Errorf("failed to preview results: %w", err)
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tNAME\tSEGMENT\tORDERS\tLTV\tAVG ORDER")
		fmt.Fprintln(tw, "--\t----\t-------\t------\t---\t---------")
		for _, row := range previewRows {
			fmt.Fprintf(tw, "%v\t%v\t%v\t%v\t$%v\t$%v\n",
				row["customer_id"],
				row["customer_name"],
				row["segment"],
				row["total_orders"],
				row["lifetime_value"],
				row["avg_order_value"],
			)
		}
		tw.Flush()
		fmt.Println()

		// ── Export to Parquet ────────────────────────────────────────────
		parquetPath := filepath.Join(tmpDir, "customer_lifetime_value.parquet")
		if err := wh.ExportTable(ctx, "feat.customer_lifetime_value", parquetPath, "parquet"); err != nil {
			return fmt.Errorf("failed to export to parquet: %w", err)
		}
		info, _ := os.Stat(parquetPath)
		fmt.Printf("[6/6] Exported to Parquet: %s (%d bytes)\n", parquetPath, info.Size())

		// ── Summary ─────────────────────────────────────────────────────
		fmt.Println()
		fmt.Println("=======================================================")
		fmt.Println("  Demo complete!")
		fmt.Println("=======================================================")
		fmt.Println()
		fmt.Println("What happened:")
		fmt.Printf("  - DuckDB warehouse: %s\n", dbPath)
		fmt.Printf("  - Parquet export:   %s\n", parquetPath)
		fmt.Println("  - Schemas created:  raw, stage, feat")
		fmt.Printf("  - Tables: raw.customers (%d), raw.products (%d), raw.orders (%d)\n",
			custCount, prodCount, orderCount)
		fmt.Printf("  - Feature: feat.customer_lifetime_value (%d rows)\n", featCount)
		fmt.Println()

		if demoKeep {
			fmt.Println("Files kept at:", tmpDir)
			fmt.Println()
			fmt.Println("Explore with DuckDB CLI:")
			fmt.Printf("  duckdb %s\n", dbPath)
			fmt.Println("  > SELECT * FROM feat.customer_lifetime_value LIMIT 5;")
		} else {
			fmt.Println("Temp files cleaned up. Use --keep to preserve them.")
		}

		return nil
	},
}

func insertSampleCustomers(ctx context.Context, wh *duckdb.Warehouse) error {
	createSQL := `
CREATE TABLE raw.customers (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    segment VARCHAR,
    city VARCHAR,
    signup_date DATE
)`
	if err := wh.ExecuteSQL(ctx, createSQL); err != nil {
		return err
	}

	names := []string{
		"Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Eva Davis",
		"Frank Miller", "Grace Wilson", "Hank Moore", "Iris Taylor", "Jack Anderson",
		"Karen Thomas", "Leo Jackson", "Mona White", "Nate Harris", "Olivia Martin",
		"Paul Garcia", "Quinn Martinez", "Rita Robinson", "Sam Clark", "Tina Rodriguez",
		"Uma Lewis", "Vic Lee", "Wendy Walker", "Xander Hall", "Yara Allen",
		"Zach Young", "Amy Hernandez", "Brian King", "Cathy Wright", "Derek Lopez",
		"Elena Hill", "Felix Scott", "Gina Green", "Hugo Adams", "Ivy Baker",
		"Jason Gonzalez", "Kelly Nelson", "Liam Carter", "Maya Mitchell", "Noah Perez",
		"Opal Roberts", "Pete Turner", "Ruby Phillips", "Steve Campbell", "Tara Parker",
		"Uriel Evans", "Vera Edwards", "Walt Collins", "Xena Stewart", "Yuri Sanchez",
		"Zoe Morris", "Aaron Rogers", "Beth Murphy", "Carl Rivera", "Dana Cook",
		"Eli Morgan", "Faye Bell", "Glen Bailey", "Hope Howard", "Ivan Ward",
		"Jade Cox", "Kurt Diaz", "Lily Richardson", "Mark Wood", "Nina Watson",
		"Oscar Brooks", "Pia Bennett", "Reed Gray", "Sara James", "Troy Reyes",
		"Una Hughes", "Vern Price", "Willa Sanders", "Xyla Foster", "York Powell",
		"Zara Long", "Adam Patterson", "Bree Hughes", "Clay Jenkins", "Dina Perry",
		"Evan Powell", "Fern Russell", "Grant Griffin", "Hazel Diaz", "Ian Hayes",
		"Jill Myers", "Kane Ford", "Luna Hamilton", "Miles Graham", "Nell Sullivan",
		"Owen Wallace", "Pam West", "Quinn Cole", "Rosa Freeman", "Sean Gibson",
		"Tess McDonald", "Uri Cruz", "Val Marshall", "Wade Owens", "Xia Burns",
	}
	segments := []string{"enterprise", "mid-market", "startup", "individual"}
	cities := []string{
		"New York", "San Francisco", "Chicago", "Austin", "Seattle",
		"Boston", "Denver", "Portland", "Miami", "Atlanta",
	}

	insertSQL := "INSERT INTO raw.customers (id, name, email, segment, city, signup_date) VALUES "
	var values string
	for i, name := range names {
		seg := segments[i%len(segments)]
		city := cities[i%len(cities)]
		email := fmt.Sprintf("user%d@example.com", i+1)
		// Spread signups across 2024
		month := (i % 12) + 1
		day := (i % 28) + 1
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("(%d, '%s', '%s', '%s', '%s', '2024-%02d-%02d')",
			i+1, name, email, seg, city, month, day)
	}
	return wh.ExecuteSQL(ctx, insertSQL+values)
}

func insertSampleProducts(ctx context.Context, wh *duckdb.Warehouse) error {
	createSQL := `
CREATE TABLE raw.products (
    id INTEGER,
    name VARCHAR,
    category VARCHAR,
    price DOUBLE,
    sku VARCHAR
)`
	if err := wh.ExecuteSQL(ctx, createSQL); err != nil {
		return err
	}

	type product struct {
		name     string
		category string
		price    float64
	}
	products := []product{
		{"Laptop Pro 15", "electronics", 1299.99},
		{"Wireless Mouse", "electronics", 29.99},
		{"USB-C Hub", "electronics", 49.99},
		{"Mechanical Keyboard", "electronics", 89.99},
		{"Monitor 27 4K", "electronics", 449.99},
		{"Webcam HD", "electronics", 79.99},
		{"Noise-Cancel Headphones", "electronics", 249.99},
		{"Tablet 10 inch", "electronics", 399.99},
		{"Phone Charger", "electronics", 19.99},
		{"Bluetooth Speaker", "electronics", 59.99},
		{"Standing Desk", "furniture", 599.99},
		{"Ergonomic Chair", "furniture", 449.99},
		{"Monitor Arm", "furniture", 129.99},
		{"Desk Lamp LED", "furniture", 39.99},
		{"Cable Organizer", "furniture", 14.99},
		{"Bookshelf Oak", "furniture", 199.99},
		{"Filing Cabinet", "furniture", 149.99},
		{"Whiteboard 48x36", "furniture", 89.99},
		{"Desk Pad XL", "furniture", 24.99},
		{"Footrest Adjustable", "furniture", 34.99},
		{"Notebook A5", "office-supplies", 9.99},
		{"Pen Set Premium", "office-supplies", 24.99},
		{"Sticky Notes Bulk", "office-supplies", 7.99},
		{"Binder Clips 100pk", "office-supplies", 5.99},
		{"Printer Paper 5000", "office-supplies", 39.99},
		{"Highlighter Set 12", "office-supplies", 8.99},
		{"Tape Dispenser", "office-supplies", 6.99},
		{"Stapler Heavy Duty", "office-supplies", 12.99},
		{"Paper Shredder", "office-supplies", 69.99},
		{"Label Maker", "office-supplies", 29.99},
		{"Project Mgmt SaaS", "software", 49.00},
		{"Design Tool Pro", "software", 29.00},
		{"Cloud Storage 1TB", "software", 9.99},
		{"VPN Annual", "software", 79.99},
		{"Antivirus Suite", "software", 39.99},
		{"IDE License", "software", 199.99},
		{"Email Service Pro", "software", 12.00},
		{"Analytics Platform", "software", 99.00},
		{"CRM Basic", "software", 25.00},
		{"Backup Service", "software", 14.99},
		{"Laptop Sleeve 15", "accessories", 29.99},
		{"Screen Protector", "accessories", 12.99},
		{"Mouse Pad Premium", "accessories", 19.99},
		{"Webcam Cover 3pk", "accessories", 4.99},
		{"USB Flash 64GB", "accessories", 9.99},
		{"HDMI Cable 6ft", "accessories", 11.99},
		{"Ethernet Cable 10ft", "accessories", 8.99},
		{"Power Strip 6out", "accessories", 16.99},
		{"Laptop Stand Alum", "accessories", 39.99},
		{"Wrist Rest Gel", "accessories", 14.99},
		{"Surge Protector", "accessories", 24.99},
		{"Camera Tripod", "accessories", 34.99},
		{"Phone Stand Wood", "accessories", 18.99},
		{"Cable Ties 100pk", "accessories", 5.99},
		{"Dust Blower Can", "accessories", 7.99},
		{"Screen Cleaner Kit", "accessories", 9.99},
		{"Travel Adapter", "accessories", 22.99},
		{"Battery Pack 20k", "accessories", 44.99},
		{"Earbuds Basic", "accessories", 14.99},
		{"Microfiber Cloth 5pk", "accessories", 6.99},
		{"Desk Calendar 2025", "office-supplies", 11.99},
		{"Planner Weekly", "office-supplies", 15.99},
		{"Envelope Box 500", "office-supplies", 19.99},
		{"Correction Tape 6pk", "office-supplies", 8.99},
		{"Glue Sticks 12pk", "office-supplies", 6.99},
		{"Scissors Titanium", "office-supplies", 11.99},
		{"Paper Clips 500pk", "office-supplies", 3.99},
		{"Rubber Bands Assort", "office-supplies", 4.99},
		{"Index Cards 300pk", "office-supplies", 5.99},
		{"Marker Set 8 Color", "office-supplies", 10.99},
		{"Server Rack 42U", "electronics", 899.99},
		{"Network Switch 24p", "electronics", 299.99},
		{"SSD 1TB NVMe", "electronics", 89.99},
		{"RAM 32GB DDR5", "electronics", 119.99},
		{"Graphics Card Mid", "electronics", 349.99},
		{"Docking Station", "electronics", 179.99},
		{"Smart Plug 4pk", "electronics", 29.99},
		{"Digital Thermometer", "electronics", 24.99},
		{"Portable SSD 500GB", "electronics", 59.99},
		{"Wireless Charger Pad", "electronics", 19.99},
		{"Toner Cartridge BK", "office-supplies", 34.99},
		{"Ink Cartridge Color", "office-supplies", 27.99},
		{"Laminating Pouches", "office-supplies", 14.99},
		{"Badge Holders 25pk", "office-supplies", 9.99},
		{"Push Pins 200pk", "office-supplies", 3.99},
		{"Dry Erase Markers 4", "office-supplies", 7.99},
		{"Clipboard Letter", "office-supplies", 4.99},
		{"Document Holder", "office-supplies", 12.99},
		{"Pencil Sharpener", "office-supplies", 6.99},
		{"Expanding File 12", "office-supplies", 13.99},
		{"L-Shaped Desk", "furniture", 399.99},
		{"Guest Chair Set 2", "furniture", 249.99},
		{"Coat Rack Stand", "furniture", 49.99},
		{"Privacy Screen 27", "furniture", 59.99},
		{"Under Desk Drawer", "furniture", 44.99},
		{"Plant Pot Set 3", "furniture", 29.99},
		{"Wall Clock Modern", "furniture", 34.99},
		{"Trash Can Sensor", "furniture", 49.99},
		{"Air Purifier Small", "furniture", 79.99},
		{"Fan Desk USB", "furniture", 19.99},
	}

	insertSQL := "INSERT INTO raw.products (id, name, category, price, sku) VALUES "
	var values string
	for i, p := range products {
		sku := fmt.Sprintf("SKU-%04d", i+1)
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("(%d, '%s', '%s', %.2f, '%s')",
			i+1, p.name, p.category, p.price, sku)
	}
	return wh.ExecuteSQL(ctx, insertSQL+values)
}

func insertSampleOrders(ctx context.Context, wh *duckdb.Warehouse) error {
	createSQL := `
CREATE TABLE raw.orders (
    id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    status VARCHAR,
    ordered_at TIMESTAMP
)`
	if err := wh.ExecuteSQL(ctx, createSQL); err != nil {
		return err
	}

	statuses := []string{"completed", "completed", "completed", "shipped", "pending"}

	insertSQL := "INSERT INTO raw.orders (id, customer_id, product_id, quantity, status, ordered_at) VALUES "
	var values string
	for i := 0; i < 100; i++ {
		custID := (i % 100) + 1
		prodID := (i*7%100) + 1
		qty := (i % 5) + 1
		status := statuses[i%len(statuses)]
		// Spread orders across 2024
		month := (i % 12) + 1
		day := (i % 28) + 1
		hour := (i * 3) % 24
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("(%d, %d, %d, %d, '%s', '2024-%02d-%02d %02d:00:00')",
			i+1, custID, prodID, qty, status, month, day, hour)
	}
	return wh.ExecuteSQL(ctx, insertSQL+values)
}

func init() {
	demoCmd.Flags().BoolVar(&demoKeep, "keep", false, "keep temp directory after demo")
	rootCmd.AddCommand(demoCmd)
}
