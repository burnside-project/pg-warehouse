// ─── Helpers ────────────────────────────────────────────────────────────────

const fmt = (n) => n == null ? '—' : n.toLocaleString('en-US');
const fmtUSD = (n) => n == null ? '—' : '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtPct = (n) => n == null ? '—' : n.toFixed(1) + '%';

async function fetchJSON(url) {
    const res = await fetch(url);
    return res.json();
}

function badgeClass(value, map) {
    return map[value] || 'badge-muted';
}

const segmentColors = {
    loyal: 'badge-green',
    regular: 'badge-blue',
    occasional: 'badge-cyan',
    one_time: 'badge-yellow',
    never_purchased: 'badge-muted',
};

const activityColors = {
    active: 'badge-green',
    cooling: 'badge-yellow',
    at_risk: 'badge-orange',
    churned: 'badge-red',
    never_active: 'badge-muted',
};

const stockColors = {
    in_stock: 'badge-green',
    low_stock: 'badge-yellow',
    out_of_stock: 'badge-red',
};

const reorderColors = {
    healthy: 'badge-green',
    reorder_soon: 'badge-yellow',
    reorder_urgent: 'badge-red',
};

const statusColors = {
    active: 'badge-green',
    expired: 'badge-muted',
    exhausted: 'badge-red',
    scheduled: 'badge-blue',
};

const reachColors = {
    high_reach: 'badge-green',
    medium_reach: 'badge-yellow',
    low_reach: 'badge-orange',
    unused: 'badge-muted',
};

const chartColors = {
    blue: '#3b82f6',
    green: '#22c55e',
    yellow: '#eab308',
    red: '#ef4444',
    orange: '#f97316',
    purple: '#a855f7',
    cyan: '#06b6d4',
    pink: '#ec4899',
    muted: '#64748b',
};

const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 12 } } },
    },
    scales: {
        x: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#1e293b' } },
        y: { ticks: { color: '#64748b', font: { size: 11 } }, grid: { color: '#1e293b' } },
    },
};

// ─── Navigation ─────────────────────────────────────────────────────────────

function showPage(name) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    document.querySelector(`.nav button[onclick="showPage('${name}')"]`).classList.add('active');
    loaders[name]?.();
}

const loaded = {};
const loaders = {
    sales: loadSales,
    customers: loadCustomers,
    products: loadProducts,
    promotions: loadPromotions,
    inventory: loadInventory,
    qa: loadQA,
};

// ─── Sales ──────────────────────────────────────────────────────────────────

async function loadSales() {
    if (loaded.sales) return;
    loaded.sales = true;

    const data = await fetchJSON('/api/sales/overview');
    document.getElementById('sales-kpis').innerHTML = `
        <div class="card">
            <div class="card-title">Total Revenue</div>
            <div class="card-value">${fmtUSD(data.total_revenue)}</div>
        </div>
        <div class="card">
            <div class="card-title">Total Orders</div>
            <div class="card-value">${fmt(data.total_orders)}</div>
            <div class="card-detail">${fmt(data.delivered_orders)} delivered</div>
        </div>
        <div class="card">
            <div class="card-title">Avg Order Value</div>
            <div class="card-value">${fmtUSD(data.avg_order_value)}</div>
        </div>
        <div class="card">
            <div class="card-title">Units Sold</div>
            <div class="card-value">${fmt(data.total_units)}</div>
            <div class="card-detail">${fmt(data.orders_with_coupon)} with coupon</div>
        </div>
    `;

    const trend = await fetchJSON('/api/sales/trend');
    new Chart(document.getElementById('chart-revenue'), {
        type: 'line',
        data: {
            labels: trend.map(r => r.date),
            datasets: [{
                label: 'Revenue',
                data: trend.map(r => r.revenue),
                borderColor: chartColors.blue,
                backgroundColor: 'rgba(59,130,246,0.1)',
                fill: true,
                tension: 0.3,
                pointRadius: 0,
            }, {
                label: 'Orders',
                data: trend.map(r => r.orders),
                borderColor: chartColors.green,
                backgroundColor: 'transparent',
                tension: 0.3,
                pointRadius: 0,
                yAxisID: 'y1',
            }],
        },
        options: {
            ...chartDefaults,
            scales: {
                ...chartDefaults.scales,
                y: { ...chartDefaults.scales.y, position: 'left', title: { display: true, text: 'Revenue ($)', color: '#64748b' } },
                y1: { ...chartDefaults.scales.y, position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Orders', color: '#64748b' } },
            },
        },
    });

    const payments = await fetchJSON('/api/sales/payment-mix');
    new Chart(document.getElementById('chart-payments'), {
        type: 'doughnut',
        data: {
            labels: ['Credit Card', 'PayPal', 'Bank Transfer'],
            datasets: [{ data: [payments.credit_card, payments.paypal, payments.bank_transfer], backgroundColor: [chartColors.blue, chartColors.yellow, chartColors.green] }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8' } } } },
    });

    const fulfill = await fetchJSON('/api/sales/fulfillment');
    new Chart(document.getElementById('chart-fulfillment'), {
        type: 'doughnut',
        data: {
            labels: ['Delivered', 'In Transit', 'Label Created', 'No Shipment'],
            datasets: [{ data: [fulfill.delivered, fulfill.in_transit, fulfill.label_created, fulfill.no_shipment], backgroundColor: [chartColors.green, chartColors.blue, chartColors.yellow, chartColors.muted] }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8' } } } },
    });
}

// ─── Customers ──────────────────────────────────────────────────────────────

async function loadCustomers() {
    if (loaded.customers) return;
    loaded.customers = true;

    const segments = await fetchJSON('/api/customers/segments');
    new Chart(document.getElementById('chart-segments'), {
        type: 'bar',
        data: {
            labels: segments.map(s => s.segment),
            datasets: [{
                label: 'Customers',
                data: segments.map(s => s.count),
                backgroundColor: [chartColors.green, chartColors.blue, chartColors.cyan, chartColors.yellow, chartColors.muted],
            }],
        },
        options: { ...chartDefaults, plugins: { legend: { display: false } } },
    });

    const activity = await fetchJSON('/api/customers/activity');
    new Chart(document.getElementById('chart-activity'), {
        type: 'doughnut',
        data: {
            labels: activity.map(a => a.status),
            datasets: [{ data: activity.map(a => a.count), backgroundColor: [chartColors.green, chartColors.yellow, chartColors.orange, chartColors.red, chartColors.muted] }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8' } } } },
    });

    const top = await fetchJSON('/api/customers/top');
    document.querySelector('#table-top-customers tbody').innerHTML = top.map(c => `
        <tr>
            <td>${c.name}</td>
            <td><span class="badge ${badgeClass(c.segment, segmentColors)}">${c.segment}</span></td>
            <td><span class="badge ${badgeClass(c.activity, activityColors)}">${c.activity}</span></td>
            <td>${c.country || '—'}</td>
            <td class="text-right text-mono">${fmt(c.orders)}</td>
            <td class="text-right text-mono">${fmtUSD(c.revenue)}</td>
            <td class="text-right text-mono">${fmtUSD(c.ltv)}</td>
        </tr>
    `).join('');

    const cohorts = await fetchJSON('/api/customers/cohorts');
    new Chart(document.getElementById('chart-cohorts'), {
        type: 'bar',
        data: {
            labels: cohorts.map(c => c.cohort),
            datasets: [{
                label: 'Avg Revenue per Customer',
                data: cohorts.map(c => c.avg_revenue),
                backgroundColor: chartColors.blue,
            }, {
                label: 'Customers',
                data: cohorts.map(c => c.customers),
                backgroundColor: chartColors.cyan,
                yAxisID: 'y1',
            }],
        },
        options: {
            ...chartDefaults,
            scales: {
                ...chartDefaults.scales,
                y: { ...chartDefaults.scales.y, position: 'left', title: { display: true, text: 'Avg Revenue ($)', color: '#64748b' } },
                y1: { ...chartDefaults.scales.y, position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Customers', color: '#64748b' } },
            },
        },
    });
}

// ─── Products ───────────────────────────────────────────────────────────────

async function loadProducts() {
    if (loaded.products) return;
    loaded.products = true;

    const top = await fetchJSON('/api/products/top');
    document.querySelector('#table-top-products tbody').innerHTML = top.map(p => `
        <tr>
            <td class="text-mono">${p.rank}</td>
            <td>${p.name}</td>
            <td>${p.category || '—'}</td>
            <td class="text-right text-mono">${fmt(p.orders)}</td>
            <td class="text-right text-mono">${fmt(p.units)}</td>
            <td class="text-right text-mono">${fmtUSD(p.revenue)}</td>
            <td class="text-right text-mono">${p.rating != null ? p.rating.toFixed(1) : '—'}</td>
            <td><span class="badge ${badgeClass(p.stock, stockColors)}">${p.stock || '—'}</span></td>
        </tr>
    `).join('');

    const cats = await fetchJSON('/api/products/categories');
    new Chart(document.getElementById('chart-categories'), {
        type: 'bar',
        data: {
            labels: cats.map(c => c.category),
            datasets: [{
                label: 'Revenue',
                data: cats.map(c => c.revenue),
                backgroundColor: chartColors.blue,
            }],
        },
        options: { ...chartDefaults, indexAxis: 'y', plugins: { legend: { display: false } } },
    });
}

// ─── Promotions ─────────────────────────────────────────────────────────────

async function loadPromotions() {
    if (loaded.promotions) return;
    loaded.promotions = true;

    const reach = await fetchJSON('/api/promotions/reach');
    new Chart(document.getElementById('chart-reach'), {
        type: 'doughnut',
        data: {
            labels: reach.map(r => r.tier),
            datasets: [{ data: reach.map(r => r.count), backgroundColor: [chartColors.green, chartColors.yellow, chartColors.orange, chartColors.muted] }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8' } } } },
    });

    new Chart(document.getElementById('chart-reach-discount'), {
        type: 'bar',
        data: {
            labels: reach.map(r => r.tier),
            datasets: [{ label: 'Discount Given', data: reach.map(r => r.discount), backgroundColor: [chartColors.green, chartColors.yellow, chartColors.orange, chartColors.muted] }],
        },
        options: { ...chartDefaults, plugins: { legend: { display: false } } },
    });

    const promos = await fetchJSON('/api/promotions/overview');
    document.querySelector('#table-promos tbody').innerHTML = promos.map(p => `
        <tr>
            <td class="text-mono">${p.code}</td>
            <td>${p.type}</td>
            <td><span class="badge ${badgeClass(p.status, statusColors)}">${p.status}</span></td>
            <td class="text-right text-mono">${fmt(p.redemptions)}</td>
            <td class="text-right text-mono">${fmt(p.customers)}</td>
            <td class="text-right text-mono">${fmtUSD(p.discount_given)}</td>
            <td class="text-right text-mono">${fmtUSD(p.avg_order_total)}</td>
            <td><span class="badge ${badgeClass(p.reach, reachColors)}">${p.reach}</span></td>
        </tr>
    `).join('');
}

// ─── Inventory ──────────────────────────────────────────────────────────────

async function loadInventory() {
    if (loaded.inventory) return;
    loaded.inventory = true;

    const health = await fetchJSON('/api/inventory/health');
    const hColors = health.map(h => {
        if (h.signal === 'healthy') return chartColors.green;
        if (h.signal === 'reorder_soon') return chartColors.yellow;
        return chartColors.red;
    });
    new Chart(document.getElementById('chart-stock-health'), {
        type: 'bar',
        data: {
            labels: health.map(h => h.signal),
            datasets: [{ label: 'Products', data: health.map(h => h.count), backgroundColor: hColors }],
        },
        options: { ...chartDefaults, plugins: { legend: { display: false } } },
    });

    const alerts = await fetchJSON('/api/inventory/alerts');
    document.querySelector('#table-reorder tbody').innerHTML = alerts.length === 0
        ? '<tr><td colspan="8" style="text-align:center;color:#94a3b8;padding:24px">All products are healthy</td></tr>'
        : alerts.map(a => `
        <tr>
            <td><span class="badge ${badgeClass(a.signal, reorderColors)}">${a.signal}</span></td>
            <td>${a.name}</td>
            <td>${a.category || '—'}</td>
            <td class="text-right text-mono">${fmt(a.available)}</td>
            <td class="text-right text-mono">${fmt(a.reserved)}</td>
            <td class="text-right text-mono">${fmt(a.net_available)}</td>
            <td class="text-right text-mono">${fmt(a.sold_30d)}</td>
            <td class="text-right text-mono">${a.days_of_stock != null ? a.days_of_stock.toFixed(1) : '—'}</td>
        </tr>
    `).join('');
}

// ─── AI Q&A ─────────────────────────────────────────────────────────────────

async function loadQA() {
    if (loaded.qa) return;
    loaded.qa = true;

    // Check AI status
    const status = await fetchJSON('/api/qa/status');
    const statusEl = document.getElementById('qa-status');
    if (!status.available) {
        statusEl.textContent = status.message;
        statusEl.className = 'qa-status error';
    }

    // Load suggestions
    const suggestions = await fetchJSON('/api/qa/suggestions');
    const container = document.getElementById('qa-suggestions');
    container.innerHTML = Object.entries(suggestions).map(([category, questions]) => `
        <div class="qa-category">
            <div class="qa-category-title">${category}</div>
            ${questions.map(q => `
                <button class="qa-suggestion" onclick="fillQuestion('${q.replace(/'/g, "\\'")}')">${q}</button>
            `).join('')}
        </div>
    `).join('');

    // Enter key submits
    document.getElementById('qa-input').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') askQuestion();
    });
}

function fillQuestion(q) {
    document.getElementById('qa-input').value = q;
    askQuestion();
}

async function askQuestion() {
    const input = document.getElementById('qa-input');
    const question = input.value.trim();
    if (!question) return;

    const statusEl = document.getElementById('qa-status');
    const answerSection = document.getElementById('qa-answer-section');
    const submitBtn = document.getElementById('qa-submit');

    // Show thinking state
    submitBtn.disabled = true;
    submitBtn.textContent = 'Thinking...';
    statusEl.textContent = 'Analyzing your question and generating SQL...';
    statusEl.className = 'qa-status qa-thinking';
    answerSection.style.display = 'none';

    try {
        const res = await fetch('/api/qa/ask', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ question }),
        });
        const result = await res.json();

        if (result.error) {
            statusEl.textContent = result.error;
            statusEl.className = 'qa-status error';
            if (result.sql) {
                answerSection.style.display = 'block';
                document.getElementById('qa-sql').textContent = result.sql;
                document.getElementById('qa-answer').textContent = 'Query failed — see error above.';
                document.getElementById('qa-data').textContent = '';
            }
        } else {
            statusEl.textContent = '';
            statusEl.className = 'qa-status';
            answerSection.style.display = 'block';
            document.getElementById('qa-answer').textContent = result.answer || 'No answer generated.';
            document.getElementById('qa-sql').textContent = result.sql || 'No SQL generated.';

            if (result.data && result.data.length > 0) {
                // Render as a mini table
                const cols = Object.keys(result.data[0]);
                let html = '<table class="data-table"><thead><tr>';
                html += cols.map(c => `<th>${c}</th>`).join('');
                html += '</tr></thead><tbody>';
                html += result.data.slice(0, 20).map(row =>
                    '<tr>' + cols.map(c => {
                        let v = row[c];
                        if (v == null) v = '—';
                        else if (typeof v === 'number') v = v.toLocaleString('en-US', { maximumFractionDigits: 2 });
                        return `<td>${v}</td>`;
                    }).join('') + '</tr>'
                ).join('');
                html += '</tbody></table>';
                document.getElementById('qa-data').innerHTML = html;
            } else {
                document.getElementById('qa-data').textContent = 'No data returned.';
            }
        }
    } catch (err) {
        statusEl.textContent = 'Request failed: ' + err.message;
        statusEl.className = 'qa-status error';
    }

    submitBtn.disabled = false;
    submitBtn.textContent = 'Ask';
}

// ─── Init ───────────────────────────────────────────────────────────────────

Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#334155';
loadSales();
