// ─── 页面导航 ─────────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', e => {
        e.preventDefault();
        const page = item.dataset.page;
        // 切换导航高亮
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        item.classList.add('active');
        // 切换页面
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById(`page-${page}`).classList.add('active');
        // 加载页面数据
        if (page === 'books') loadBooks();
    });
});

// Tab 切换
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        const tabId = tab.dataset.tab;
        const parent = tab.closest('.rules-detail');
        parent.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        parent.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        parent.querySelector(`#${tabId}`).classList.add('active');
    });
});

// Enter 键触发分析
document.getElementById('stock-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') analyzeStock();
});
document.getElementById('search-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') semanticSearch();
});

// 页面加载时获取书籍列表（填充下拉框）
loadBookFilter();

// ─── 全局变量 ─────────────────────────────────────────────
let lastAnalysisData = null;

// ─── 股票分析 ─────────────────────────────────────────────
async function analyzeStock() {
    const symbol = document.getElementById('stock-input').value.trim();
    if (!symbol) return;

    const dataSource = document.getElementById('data-source').value;
    const book = document.getElementById('book-filter').value || null;
    const btn = document.getElementById('btn-analyze');

    // 显示加载
    btn.disabled = true;
    document.getElementById('loading').classList.remove('hidden');
    document.getElementById('result-area').classList.add('hidden');
    clearError();

    try {
        const resp = await fetch('/api/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ symbol, data_source: dataSource, book }),
        });

        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '分析失败');

        lastAnalysisData = data;
        renderAnalysis(data);
    } catch (err) {
        showError(err.message);
    } finally {
        btn.disabled = false;
        document.getElementById('loading').classList.add('hidden');
    }
}

function renderAnalysis(data) {
    const { stock, rules, passed_rules, failed_rules, unknown_rules } = data;

    // 股票基本信息
    document.getElementById('stock-name').textContent = stock.name || stock.symbol;
    document.getElementById('stock-symbol').textContent = stock.symbol;
    document.getElementById('stock-sector').textContent = `${stock.sector || ''} / ${stock.industry || ''}`;
    document.getElementById('stock-price').textContent = stock.price ? `$${stock.price.toFixed(2)}` : '—';

    // 规则总览
    document.getElementById('stat-passed').textContent = rules.passed;
    document.getElementById('stat-failed').textContent = rules.failed;
    document.getElementById('stat-unknown').textContent = rules.unknown;
    document.getElementById('stat-total').textContent = rules.total;
    document.getElementById('pass-rate').textContent = `${rules.pass_rate}%`;

    // 环形进度
    const ring = document.getElementById('ring-progress');
    const offset = 314 - (314 * rules.pass_rate / 100);
    ring.style.strokeDashoffset = offset;

    // 根据通过率设置颜色
    if (rules.pass_rate >= 70) {
        ring.style.stroke = 'var(--green)';
    } else if (rules.pass_rate >= 40) {
        ring.style.stroke = 'var(--yellow)';
    } else {
        ring.style.stroke = 'var(--red)';
    }

    // 指标面板
    renderMetrics(stock);

    // 规则列表
    renderRulesList('passed-list', passed_rules, 'pass');
    renderRulesList('failed-list', failed_rules, 'fail');
    renderRulesList('unknown-list', unknown_rules, 'skip');

    // 重置 LLM
    document.getElementById('llm-result').classList.add('hidden');
    document.getElementById('btn-llm').disabled = false;

    // 显示结果
    document.getElementById('result-area').classList.remove('hidden');
}

function renderMetrics(s) {
    // 估值
    setMetrics('valuation-metrics', [
        ['PE (TTM)', fmt(s.pe, '1f')],
        ['Forward PE', fmt(s.forward_pe, '1f')],
        ['PB', fmt(s.pb, '2f')],
        ['PS', fmt(s.ps, '2f')],
        ['盈利收益率', s.earnings_yield ? `${s.earnings_yield.toFixed(2)}%` : '—'],
        ['市值', s.market_cap ? fmtMoney(s.market_cap) : '—'],
    ]);

    // 盈利
    setMetrics('profitability-metrics', [
        ['ROE', s.roe ? `${(s.roe * 100).toFixed(1)}%` : '—'],
        ['EPS', s.eps ? `$${s.eps.toFixed(2)}` : '—'],
        ['净利润率', s.profit_margin ? `${(s.profit_margin * 100).toFixed(1)}%` : '—'],
        ['营业利润率', s.operating_margin ? `${(s.operating_margin * 100).toFixed(1)}%` : '—'],
        ['EBIT', s.ebit ? fmtMoney(s.ebit) : '—'],
        ['净利润', s.net_income ? fmtMoney(s.net_income) : '—'],
    ]);

    // 财务
    setMetrics('financial-metrics', [
        ['流动比率', fmt(s.current_ratio, '2f')],
        ['负债权益比', fmt(s.debt_to_equity, '2f')],
        ['利息覆盖率', s.interest_coverage_ratio ? `${s.interest_coverage_ratio.toFixed(1)}x` : '—'],
        ['自由现金流', s.free_cash_flow ? fmtMoney(s.free_cash_flow) : '—'],
        ['股息率', s.dividend_yield ? `${s.dividend_yield.toFixed(2)}%` : '—'],
        ['52周范围', s.price_52w_low && s.price_52w_high ? `$${s.price_52w_low.toFixed(0)} - $${s.price_52w_high.toFixed(0)}` : '—'],
    ]);

    // Graham
    setMetrics('graham-metrics', [
        ['Graham Number', s.graham_number ? `$${s.graham_number.toFixed(2)}` : '—'],
        ['内在价值', s.intrinsic_value ? `$${s.intrinsic_value.toFixed(2)}` : '—'],
        ['安全边际', s.margin_of_safety ? `${(Math.abs(s.margin_of_safety) * 100).toFixed(1)}% (${s.margin_of_safety > 0 ? '折价' : '溢价'})` : '—'],
        ['NCAV/股', s.ncav_per_share ? `$${s.ncav_per_share.toFixed(2)}` : '—'],
        ['有形账面值', s.tangible_book_value ? `$${s.tangible_book_value.toFixed(2)}` : '—'],
    ]);

    // 历史
    setMetrics('history-metrics', [
        ['10年平均EPS', s.avg_eps_10y ? `$${s.avg_eps_10y.toFixed(2)}` : '—'],
        ['3年平均EPS', s.avg_eps_3y ? `$${s.avg_eps_3y.toFixed(2)}` : '—'],
        ['EPS 10年CAGR', s.earnings_growth_10y ? `${(s.earnings_growth_10y * 100).toFixed(1)}%` : '—'],
        ['EPS 5年增长', s.eps_growth_5y ? `${(s.eps_growth_5y * 100).toFixed(1)}%` : '—'],
        ['盈利年数', s.profitable_years ? `${s.profitable_years}/10` : '—'],
        ['连续分红', s.consecutive_dividend_years ? `${s.consecutive_dividend_years}年` : '—'],
        ['收入10年CAGR', s.revenue_cagr_10y ? `${(s.revenue_cagr_10y * 100).toFixed(1)}%` : '—'],
    ]);

    // 技术 + 基准
    setMetrics('technical-metrics', [
        ['RSI (14)', s.rsi_14d ? s.rsi_14d.toFixed(1) : '—'],
        ['MACD', s.macd_line ? s.macd_line.toFixed(2) : '—'],
        ['MA (200)', s.ma_200d ? `$${s.ma_200d.toFixed(2)}` : '—'],
        ['市场PE (S&P500)', s.market_pe ? s.market_pe.toFixed(1) : '—'],
        ['10年国债', s.treasury_yield_10y ? `${s.treasury_yield_10y.toFixed(2)}%` : '—'],
        ['AA企业债', s.aa_bond_yield ? `${s.aa_bond_yield.toFixed(2)}%` : '—'],
    ]);
}

function setMetrics(containerId, items) {
    const el = document.getElementById(containerId);
    el.innerHTML = items
        .filter(([, v]) => v && v !== '—' && v !== '$0.00' && v !== '0.0')
        .map(([name, value]) => `
            <div class="metric-row">
                <span class="metric-name">${name}</span>
                <span class="metric-value">${value}</span>
            </div>
        `).join('') || '<div class="metric-row"><span class="metric-name" style="color:var(--text-muted)">暂无数据</span></div>';
}

function renderRulesList(containerId, rules, type) {
    const el = document.getElementById(containerId);
    if (!rules || rules.length === 0) {
        el.innerHTML = '<div class="empty-state"><p>无</p></div>';
        return;
    }
    el.innerHTML = rules.map(r => `
        <div class="rule-item ${type}">
            <span class="rule-expr">${escapeHtml(r.expression || '')}</span>
            <div class="rule-desc">${escapeHtml(r.description || '')}</div>
            ${r.values ? `<div class="rule-values">${Object.entries(r.values).map(([k, v]) => `${k}=${typeof v === 'number' ? v.toFixed(4) : v}`).join(', ')}</div>` : ''}
            ${r.reason && type === 'skip' ? `<div class="rule-values">${escapeHtml(r.reason)}</div>` : ''}
        </div>
    `).join('');
}

// ─── LLM 深度分析 ─────────────────────────────────────────
async function requestLLMAnalysis() {
    if (!lastAnalysisData) return;

    const btn = document.getElementById('btn-llm');
    const resultEl = document.getElementById('llm-result');
    btn.disabled = true;
    btn.textContent = '正在生成分析报告...';
    resultEl.classList.add('hidden');

    try {
        const resp = await fetch('/api/llm-analysis', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                stock: lastAnalysisData.stock,
                passed_rules: lastAnalysisData.passed_rules,
                failed_rules: lastAnalysisData.failed_rules,
            }),
        });

        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || 'LLM 分析失败');

        resultEl.innerHTML = formatMarkdown(data.analysis);
        resultEl.classList.remove('hidden');
    } catch (err) {
        showError(err.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 2a10 10 0 1 0 10 10A10 10 0 0 0 12 2z"></path>
                <path d="M12 16v-4"></path>
                <path d="M12 8h.01"></path>
            </svg>
            AI 深度分析报告
        `;
    }
}

// ─── 知识库 ───────────────────────────────────────────────
async function loadBooks() {
    const container = document.getElementById('books-list');
    const detailEl = document.getElementById('book-detail');
    detailEl.classList.add('hidden');

    try {
        const resp = await fetch('/api/books');
        const data = await resp.json();

        if (!data.books || data.books.length === 0) {
            container.innerHTML = '<div class="empty-state"><p>暂无已导入的书籍，请使用 CLI 导入</p></div>';
            return;
        }

        container.innerHTML = data.books.map(b => `
            <div class="book-card" onclick="viewBookRules('${escapeHtml(b.name)}')">
                <h3>
                    ${escapeHtml(b.name)}
                    <span class="badge ${b.has_knowledge ? 'badge-green' : 'badge-gray'}">${b.has_knowledge ? '已提取' : '待提取'}</span>
                </h3>
                <div class="stats">
                    <span>${b.rule_count || 0} 条规则</span>
                    <span>${b.indicator_count || 0} 个指标</span>
                </div>
                ${b.summary ? `<div class="summary">${escapeHtml(b.summary)}</div>` : ''}
            </div>
        `).join('');
    } catch (err) {
        container.innerHTML = `<div class="error-msg">${escapeHtml(err.message)}</div>`;
    }
}

async function viewBookRules(bookName) {
    const detailEl = document.getElementById('book-detail');
    detailEl.classList.remove('hidden');
    detailEl.innerHTML = '<div class="loading"><div class="spinner"></div><p>加载规则...</p></div>';

    try {
        const resp = await fetch(`/api/rules/${encodeURIComponent(bookName)}`);
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error);

        const rules = data.rules || [];
        detailEl.innerHTML = `
            <button class="back-btn" onclick="document.getElementById('book-detail').classList.add('hidden')">
                &larr; 返回
            </button>
            <h2>${escapeHtml(data.book_title || bookName)}</h2>
            <p style="color:var(--text-dim);margin-bottom:16px">${rules.length} 条选股规则</p>
            <table class="rule-table">
                <thead>
                    <tr>
                        <th style="width:40%">描述</th>
                        <th style="width:35%">表达式</th>
                        <th>来源章节</th>
                    </tr>
                </thead>
                <tbody>
                    ${rules.map(r => `
                        <tr>
                            <td>${escapeHtml(r.description || '')}</td>
                            <td class="expr-cell">${escapeHtml(r.expression || '—')}</td>
                            <td style="color:var(--text-muted);font-size:12px">${escapeHtml(r.source_chapter || '')}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    } catch (err) {
        detailEl.innerHTML = `<div class="error-msg">${escapeHtml(err.message)}</div>`;
    }
}

// ─── 语义搜索 ─────────────────────────────────────────────
async function semanticSearch() {
    const query = document.getElementById('search-input').value.trim();
    if (!query) return;

    const container = document.getElementById('search-results');
    container.innerHTML = '<div class="loading"><div class="spinner"></div><p>搜索中...</p></div>';

    try {
        const resp = await fetch('/api/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query }),
        });

        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error);

        if (!data.results || data.results.length === 0) {
            container.innerHTML = '<div class="empty-state"><p>未找到相关内容</p></div>';
            return;
        }

        container.innerHTML = data.results.map(r => `
            <div class="search-result-item">
                <div class="meta">
                    <span>${escapeHtml(r.chapter || '')} — ${escapeHtml(r.book || '')}</span>
                    <span class="score">相似度: ${r.score}</span>
                </div>
                <div class="content">${escapeHtml(r.content)}</div>
            </div>
        `).join('');
    } catch (err) {
        container.innerHTML = `<div class="error-msg">${escapeHtml(err.message)}</div>`;
    }
}

// ─── 加载书籍下拉框 ──────────────────────────────────────
async function loadBookFilter() {
    try {
        const resp = await fetch('/api/books');
        const data = await resp.json();
        const select = document.getElementById('book-filter');

        if (data.books) {
            data.books.forEach(b => {
                if (b.has_knowledge) {
                    const opt = document.createElement('option');
                    opt.value = b.name;
                    opt.textContent = b.name.length > 25 ? b.name.substring(0, 25) + '...' : b.name;
                    select.appendChild(opt);
                }
            });
        }
    } catch (err) {
        // 静默失败
    }
}

// ─── 多数据源交叉验证 ─────────────────────────────────
async function crossValidate() {
    const symbol = document.getElementById('stock-input').value.trim();
    if (!symbol) return;

    const btn = document.getElementById('btn-cross');
    btn.disabled = true;
    document.getElementById('loading').classList.remove('hidden');
    document.getElementById('cross-result').classList.add('hidden');
    clearError();

    try {
        const resp = await fetch('/api/cross-validate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ symbol }),
        });

        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '交叉验证失败');

        renderCrossValidation(data);
    } catch (err) {
        showError(err.message);
    } finally {
        btn.disabled = false;
        document.getElementById('loading').classList.add('hidden');
    }
}

function renderCrossValidation(data) {
    const { comparison, sources } = data;

    if (!comparison || !comparison.fields || comparison.fields.length === 0) {
        showError(comparison?.summary || '无法进行交叉验证，需要至少2个可用数据源');
        return;
    }

    // 摘要
    document.getElementById('cross-summary').textContent = comparison.summary;

    // 统计标签
    const stats = comparison.stats;
    document.getElementById('cross-stats').innerHTML = `
        <span class="cross-stat-pill match">一致 ${stats.match}</span>
        <span class="cross-stat-pill close">接近 ${stats.close}</span>
        <span class="cross-stat-pill diverge">有差异 ${stats.diverge}</span>
        <span class="cross-stat-pill conflict">冲突 ${stats.conflict}</span>
    `;

    // 表头
    const sourceNames = comparison.source_names;
    document.getElementById('cross-thead').innerHTML = `
        <tr>
            <th>指标</th>
            ${sourceNames.map(s => `<th>${escapeHtml(s)}</th>`).join('')}
            <th>差异</th>
            <th>状态</th>
        </tr>
    `;

    // 表体
    document.getElementById('cross-tbody').innerHTML = comparison.fields.map(f => {
        const statusLabel = { match: '一致', close: '接近', diverge: '有差异', conflict: '冲突' }[f.status] || '';
        return `
            <tr>
                <td class="label-cell">${escapeHtml(f.label)}</td>
                ${sourceNames.map(s => {
                    const val = f.values[s];
                    if (val === undefined || val === null) return '<td style="color:var(--text-muted)">—</td>';
                    return `<td>${fmtCrossValue(val, f.unit, f.key)}</td>`;
                }).join('')}
                <td class="diff-cell diff-${f.status}">${f.max_diff_pct}%</td>
                <td><span class="status-dot ${f.status}"></span>${statusLabel}</td>
            </tr>
        `;
    }).join('');

    document.getElementById('cross-result').classList.remove('hidden');
}

function fmtCrossValue(val, unit, key) {
    if (val === undefined || val === null) return '—';

    // 大金额
    if (unit === '$' && (key === 'revenue' || key === 'net_income' || key === 'market_cap' || key === 'free_cash_flow')) {
        return fmtMoney(val);
    }

    // 百分比型 (ROE、profit_margin 等是小数)
    const pctFields = ['roe', 'profit_margin', 'operating_margin', 'margin_of_safety', 'earnings_growth_10y'];
    if (pctFields.includes(key)) {
        return `${(val * 100).toFixed(2)}%`;
    }

    if (unit === '%') return `${val.toFixed(2)}%`;
    if (unit === '$') return `$${val.toFixed(2)}`;
    if (unit === 'x') return `${val.toFixed(1)}x`;
    if (unit === '年') return `${val}年`;
    return typeof val === 'number' ? val.toFixed(2) : String(val);
}

// ─── 工具函数 ─────────────────────────────────────────────
function fmt(val, format) {
    if (!val && val !== 0) return '—';
    if (format === '1f') return val.toFixed(1);
    if (format === '2f') return val.toFixed(2);
    return String(val);
}

function fmtMoney(val) {
    if (!val) return '—';
    const abs = Math.abs(val);
    const sign = val < 0 ? '-' : '';
    if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(1)}T`;
    if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
    return `${sign}$${abs.toFixed(0)}`;
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function formatMarkdown(text) {
    if (!text) return '';
    return escapeHtml(text)
        .replace(/^### (.+)$/gm, '<h3>$1</h3>')
        .replace(/^## (.+)$/gm, '<h2>$1</h2>')
        .replace(/^# (.+)$/gm, '<h1>$1</h1>')
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\n/g, '<br>');
}

function showError(msg) {
    // 在结果区域前插入错误
    const area = document.querySelector('.page.active');
    const existing = area.querySelector('.error-msg');
    if (existing) existing.remove();

    const el = document.createElement('div');
    el.className = 'error-msg';
    el.textContent = msg;
    area.querySelector('.search-bar')?.after(el) || area.prepend(el);

    setTimeout(() => el.remove(), 8000);
}

function clearError() {
    document.querySelectorAll('.error-msg').forEach(e => e.remove());
}
