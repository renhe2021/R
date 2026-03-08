// ─── 页面导航 ─────────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', e => {
        e.preventDefault();
        const page = item.dataset.page;
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        item.classList.add('active');
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById(`page-${page}`).classList.add('active');
        if (page === 'books') loadBooks();
        if (page === 'watchlist') loadWatchlist();
    });
});

// Tab 切换 (legacy rules tabs)
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        const tabId = tab.dataset.tab;
        const parent = tab.closest('.rules-detail');
        if (!parent) return;
        parent.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        parent.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        parent.querySelector(`#${tabId}`).classList.add('active');
    });
});

// Detail tab switching (new pipeline)
document.querySelectorAll('.detail-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.detail-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        if (window._currentDetailStock) {
            renderDetailTab(tab.dataset.detailTab, window._currentDetailStock);
        }
    });
});

// Enter 键触发语义搜索
document.getElementById('search-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') semanticSearch();
});

// ─── Basket Manager + 自动补全 ────────────────────────────────
const basket = {
    items: [],   // [{symbol, name, market}]

    add(symbol, name, market) {
        symbol = symbol.toUpperCase().trim();
        if (!symbol || this.items.some(i => i.symbol === symbol)) return false;
        this.items.push({ symbol, name: name || '', market: market || '' });
        this._render();
        return true;
    },

    remove(symbol) {
        this.items = this.items.filter(i => i.symbol !== symbol);
        this._render();
    },

    clear() {
        this.items = [];
        this._render();
    },

    getSymbols() {
        return this.items.map(i => i.symbol);
    },

    _render() {
        const container = document.getElementById('basket-tags');
        const countEl = document.getElementById('basket-count');
        const clearBtn = document.getElementById('basket-clear-btn');
        const n = this.items.length;

        countEl.textContent = `已选 ${n} 只`;
        countEl.classList.toggle('has-items', n > 0);
        clearBtn.style.display = n > 0 ? 'inline-block' : 'none';

        container.innerHTML = this.items.map(item => {
            const mCls = item.market === 'US' ? 'us' : item.market === 'HK' ? 'hk' :
                         item.market.startsWith('CN') ? 'cn' : item.market === 'JP' ? 'jp' : '';
            return `<span class="basket-tag" data-symbol="${escapeHtml(item.symbol)}">
                <span class="bt-market ${mCls}">${escapeHtml(item.market || '')}</span>
                <span class="bt-symbol">${escapeHtml(item.symbol)}</span>
                <span class="bt-name">${escapeHtml(item.name)}</span>
                <button class="bt-remove" onclick="basket.remove('${item.symbol}')" title="移除">&times;</button>
            </span>`;
        }).join('');

        // 动画：新增的最后一个 tag 闪一下
        const lastTag = container.querySelector('.basket-tag:last-child');
        if (lastTag) {
            lastTag.classList.add('just-added');
            setTimeout(() => lastTag.classList.remove('just-added'), 400);
        }
    }
};

function clearBasket() { basket.clear(); }

// 预设池映射
const PRESET_POOLS = {
    buffett_portfolio: ['AAPL','BAC','KO','CVX','OXY','KHC','MCO','CB','DVA','ALLY','SNOW','NU','HPQ','PARA','GM','AMZN','MA','V','AON'],
    sp500_top50: ['AAPL','MSFT','AMZN','NVDA','GOOGL','META','BRK-B','TSLA','UNH','JNJ','V','JPM','XOM','PG','MA','HD','CVX','MRK','ABBV','LLY','PEP','KO','COST','AVGO','WMT','MCD','TMO','ACN','CSCO','ABT','DHR','CRM','ADBE','ORCL','NKE','TXN','NEE','PM','UNP','RTX','UPS','INTC','AMD','LOW','QCOM','BA','AMGN','ISRG','GS','BLK'],
    dividend_kings: ['PG','KO','JNJ','GPC','EMR','MMM','DOV','PH','SWK','BDX','LOW','HRL','TGT','CL','ITW','ABT','ABBV','FRT','AWR','NWN','SJW','SYY','KMB','PPG','GWW','ADM','ADP','AOS','CINF'],
    value_30: ['BRK-B','JPM','BAC','WFC','C','GS','MS','JNJ','PFE','ABBV','MRK','UNH','CVX','XOM','VZ','T','INTC','CSCO','IBM','MMM','CAT','DE','GE','F','GM','KO','PEP','PG','WMT','TGT'],
    hk_value: ['0700.HK','9988.HK','3690.HK','1810.HK','2318.HK','0941.HK','0005.HK','0001.HK','0011.HK','0388.HK','1299.HK','0883.HK','0939.HK','1398.HK','2628.HK','0267.HK','0027.HK','0016.HK','0002.HK','0003.HK'],
    faang_plus: ['AAPL','AMZN','GOOGL','META','NFLX','MSFT','NVDA','TSLA','AVGO','CRM','AMD','ADBE','ORCL','SNOW','PLTR'],
};

// 预设池按钮事件绑定
document.querySelectorAll('.basket-preset-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const preset = btn.dataset.preset;
        const pool = PRESET_POOLS[preset];
        if (!pool) return;

        // toggle：如果当前 basket 正好是这个预设（且一样长），则清空
        const currentSymbols = basket.getSymbols();
        if (currentSymbols.length === pool.length && pool.every(s => currentSymbols.includes(s))) {
            basket.clear();
            btn.classList.remove('active');
            return;
        }

        // 先清空再添加
        basket.clear();
        document.querySelectorAll('.basket-preset-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');

        pool.forEach(s => {
            const market = s.includes('.HK') ? 'HK' : s.match(/^\d/) ? 'CN' : 'US';
            basket.add(s, '', market);
        });
    });
});

(function initAutocomplete() {
    const input = document.getElementById('stock-input');
    const dropdown = document.getElementById('autocomplete-dropdown');
    let debounceTimer = null;
    let activeIndex = -1;
    let currentResults = [];

    // 输入事件 — 防抖搜索
    input.addEventListener('input', () => {
        const q = input.value.trim();
        clearTimeout(debounceTimer);
        activeIndex = -1;

        if (q.length < 1) {
            closeAutocomplete();
            return;
        }

        debounceTimer = setTimeout(() => searchSymbols(q), 200);
    });

    // 键盘导航
    input.addEventListener('keydown', e => {
        // Tab / 逗号 / 分号 → 把当前输入当作 symbol 直接添加
        if (e.key === 'Tab' || e.key === ',' || e.key === ';') {
            const raw = input.value.trim().replace(/[,;]/g, '');
            if (raw.length > 0) {
                e.preventDefault();
                // 如果下拉有高亮项，用它；否则直接添加原文
                if (activeIndex >= 0 && currentResults[activeIndex]) {
                    addFromResult(currentResults[activeIndex]);
                } else {
                    basket.add(raw, '', '');
                }
                input.value = '';
                closeAutocomplete();
            }
            return;
        }

        // Enter — 如果下拉有高亮项，选中；否则如果有输入添加；否则启动分析
        if (e.key === 'Enter') {
            if (!dropdown.classList.contains('hidden') && activeIndex >= 0) {
                e.preventDefault();
                addFromResult(currentResults[activeIndex]);
                input.value = '';
                closeAutocomplete();
            } else if (input.value.trim().length > 0) {
                e.preventDefault();
                basket.add(input.value.trim().toUpperCase(), '', '');
                input.value = '';
                closeAutocomplete();
            } else {
                // 输入框为空按 Enter → 启动分析
                closeAutocomplete();
                runPipeline();
            }
            return;
        }

        // Backspace 空输入时删除最后一个 tag
        if (e.key === 'Backspace' && input.value === '' && basket.items.length > 0) {
            basket.remove(basket.items[basket.items.length - 1].symbol);
            return;
        }

        // 箭头导航
        if (!dropdown.classList.contains('hidden')) {
            if (e.key === 'ArrowDown') {
                e.preventDefault();
                activeIndex = Math.min(activeIndex + 1, currentResults.length - 1);
                highlightItem();
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                activeIndex = Math.max(activeIndex - 1, -1);
                highlightItem();
            } else if (e.key === 'Escape') {
                closeAutocomplete();
            }
        }
    });

    // 点击外部关闭
    document.addEventListener('click', e => {
        if (!e.target.closest('.autocomplete-wrap') && !e.target.closest('.basket-input-area')) {
            closeAutocomplete();
        }
    });

    // 点击 basket-input-area 聚焦输入框
    document.getElementById('basket-input-area').addEventListener('click', () => {
        input.focus();
    });

    // 聚焦时如果有内容则搜索
    input.addEventListener('focus', () => {
        if (input.value.trim().length >= 1 && currentResults.length > 0) {
            dropdown.classList.remove('hidden');
        }
    });

    async function searchSymbols(query) {
        dropdown.innerHTML = '<div class="ac-loading">搜索中...</div>';
        dropdown.classList.remove('hidden');

        try {
            const resp = await fetch(`/api/symbol/search?q=${encodeURIComponent(query)}`);
            const data = await resp.json();
            currentResults = data.results || [];

            // 过滤掉已在 basket 中的
            const existing = new Set(basket.getSymbols());
            const filtered = currentResults.filter(r => !existing.has(r.symbol));

            if (filtered.length === 0) {
                dropdown.innerHTML = currentResults.length > 0
                    ? '<div class="ac-empty">匹配的股票已全部添加</div>'
                    : '<div class="ac-empty">未找到匹配的股票</div>';
                currentResults = [];
                return;
            }

            currentResults = filtered;

            dropdown.innerHTML = currentResults.map((r, i) => {
                const marketClass = getMarketClass(r.market);
                const displayName = r.name_cn || r.name || '';
                const exchangeText = r.exchange ? `<span class="ac-exchange">${escapeHtml(r.exchange)}</span>` : '';
                return `
                    <div class="autocomplete-item" data-index="${i}">
                        <div class="ac-left">
                            <span class="ac-symbol">${escapeHtml(r.symbol)}</span>
                            <span class="ac-name">${escapeHtml(displayName)}</span>
                        </div>
                        <div class="ac-right">
                            <span class="ac-market ${marketClass}">${escapeHtml(r.market)}</span>
                            ${exchangeText}
                            <span class="ac-add-hint">+ 添加</span>
                        </div>
                    </div>
                `;
            }).join('');

            // 绑定点击事件
            dropdown.querySelectorAll('.autocomplete-item').forEach(item => {
                item.addEventListener('click', () => {
                    const idx = parseInt(item.dataset.index);
                    addFromResult(currentResults[idx]);
                    input.value = '';
                    closeAutocomplete();
                    input.focus();
                });
            });
        } catch (err) {
            dropdown.innerHTML = '<div class="ac-empty">搜索失败</div>';
        }
    }

    function addFromResult(item) {
        basket.add(item.symbol, item.name_cn || item.name || '', item.market || '');
    }

    function highlightItem() {
        dropdown.querySelectorAll('.autocomplete-item').forEach((el, i) => {
            el.classList.toggle('active', i === activeIndex);
        });
        const active = dropdown.querySelector('.autocomplete-item.active');
        if (active) active.scrollIntoView({ block: 'nearest' });
    }

    function getMarketClass(market) {
        if (market === 'US') return 'us';
        if (market === 'HK') return 'hk';
        if (market.startsWith('CN')) return 'cn';
        if (market === 'JP') return 'jp';
        return '';
    }
})();

function closeAutocomplete() {
    const dd = document.getElementById('autocomplete-dropdown');
    if (dd) dd.classList.add('hidden');
}

// 页面加载时获取书籍列表（填充下拉框）
loadBookFilter();

// ─── 全局变量 ─────────────────────────────────────────────
let lastAnalysisData = null;

// ═══════════════════════════════════════════════════════════════
//  R-System Unified Pipeline v2 — Frontend Client
// ═══════════════════════════════════════════════════════════════

let pipelineResults = [];
let pipelineReport = null;
window._currentDetailStock = null;

async function runPipeline() {
    const stocks = basket.getSymbols();
    const strategy = document.getElementById('strategy-select').value;

    if (!stocks.length) {
        // 如果输入框有内容，先添加到 basket
        const raw = document.getElementById('stock-input').value.trim();
        if (raw) {
            raw.split(/[,;\s]+/).filter(s => s).forEach(s => basket.add(s.toUpperCase(), '', ''));
            document.getElementById('stock-input').value = '';
            if (basket.getSymbols().length === 0) {
                alert('请添加至少一只股票到分析篮中');
                return;
            }
            return runPipeline(); // retry with newly added
        }
        alert('请添加至少一只股票到分析篮中');
        return;
    }

    // Reset UI
    resetPipelineUI();
    document.getElementById('pipeline-progress').classList.remove('hidden');
    document.getElementById('btn-pipeline').disabled = true;

    const body = { stocks, strategy };

    try {
        const resp = await fetch('/api/v1/agent/pipeline', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });

        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.startsWith('data: ')) continue;
                try {
                    const event = JSON.parse(line.slice(6));
                    handlePipelineEvent(event);
                } catch (e) { /* skip bad JSON */ }
            }
        }
    } catch (err) {
        document.getElementById('pipeline-status').textContent = `错误: ${err.message}`;
    } finally {
        document.getElementById('btn-pipeline').disabled = false;
    }
}

function resetPipelineUI() {
    pipelineResults = [];
    pipelineReport = null;
    window._currentDetailStock = null;
    window._timingSignals = {};
    window._backtestResults = {};
    window._positionAdvice = {};
    window._timingData = {};

    document.querySelectorAll('.pipeline-stage').forEach(s => {
        s.classList.remove('running', 'completed', 'skipped');
    });
    document.querySelectorAll('.pipeline-connector').forEach(c => c.classList.remove('active'));

    document.getElementById('pipeline-funnel').classList.add('hidden');
    document.getElementById('pipeline-results').classList.add('hidden');
    document.getElementById('stock-detail-panel').classList.add('hidden');
    const llmSection = document.getElementById('llm-report-section');
    if (llmSection) llmSection.classList.add('hidden');
    document.getElementById('pipeline-status').textContent = '准备中...';
    const substepEl = document.getElementById('pipeline-substep');
    if (substepEl) substepEl.innerHTML = '';
    document.getElementById('result-cards').innerHTML = '';
    document.getElementById('funnel-bars').innerHTML = '';
}

function handlePipelineEvent(event) {
    const type = event.event || event.type;

    if (type === 'stage_update') {
        updateStageUI(event.stage, event.status, event.stageNameCn, event.data);
    } else if (type === 'substep_update') {
        updateSubstepUI(event.stage, event.substep, event.status, event.message, event.data);
    } else if (type === 'pipeline_done') {
        pipelineReport = event.report;
        onPipelineComplete(event);
    }
}

function updateSubstepUI(stageId, substep, status, message, data) {
    const substepEl = document.getElementById('pipeline-substep');
    if (!substepEl) return;

    const icon = status === 'completed' ? '&#10003;' : '<span class="substep-spinner"></span>';
    substepEl.innerHTML = `<span class="substep-icon">${icon}</span> ${message || ''}`;
    substepEl.className = `pipeline-substep ${status}`;

    // Also update the status text with sub-step info
    const statusEl = document.getElementById('pipeline-status');
    if (status === 'running') {
        statusEl.textContent = message;
    }

    // Process timing data
    if (substep === 'timing' && status === 'completed' && data && data.details) {
        window._timingData = {};
        data.details.forEach(d => { window._timingData[d.symbol] = d; });
    }
}

function updateStageUI(stageId, status, nameCn, data) {
    const stageEl = document.querySelector(`.pipeline-stage[data-stage="${stageId}"]`);
    if (!stageEl) return;

    // Remove old states
    stageEl.classList.remove('running', 'completed', 'skipped');
    stageEl.classList.add(status);

    // Update connector before this stage
    if (status === 'completed' && stageId > 1) {
        const connectors = document.querySelectorAll('.pipeline-connector');
        if (connectors[stageId - 2]) connectors[stageId - 2].classList.add('active');
    }

    // Clear substep when a new stage starts
    if (status === 'running') {
        const substepEl = document.getElementById('pipeline-substep');
        if (substepEl) substepEl.innerHTML = '';
    }

    // Update status text
    const statusText = {
        'running': `阶段 ${stageId}/9: ${nameCn} 进行中...`,
        'completed': `阶段 ${stageId}/9: ${nameCn} 完成`,
        'skipped': `阶段 ${stageId}/9: ${nameCn} 跳过`,
    };
    document.getElementById('pipeline-status').textContent = statusText[status] || '';

    // Process stage data
    if (status === 'completed' && data) {
        processStageData(stageId, data);
    }
}

function processStageData(stageId, data) {
    if (stageId === 3 && data.details) {
        // Stage 3: Knockout — show eliminated
        for (const d of data.details) {
            const existing = pipelineResults.find(r => r.symbol === d.symbol);
            if (existing) {
                existing.eliminated = true;
                existing.eliminatedAtStage = 3;
                existing.eliminationReason = d.reason;
            }
        }
    }

    if (stageId === 4 && data.details) {
        // Stage 4: Forensics
        for (const d of data.details) {
            const r = pipelineResults.find(x => x.symbol === d.symbol);
            if (r) {
                r.fScore = d.fScore;
                r.zScore = d.zScore;
                r.mScore = d.mScore;
                r.riskTier = d.riskTier;
                r.redFlags = d.redFlags || [];
                r.forensicsVerdict = d.verdict;
                if (d.verdict === 'FAIL') {
                    r.eliminated = true;
                    r.eliminatedAtStage = 4;
                }
            }
        }
    }

    if (stageId === 5 && data.details) {
        // Stage 5: Schools
        for (const d of data.details) {
            const r = pipelineResults.find(x => x.symbol === d.symbol);
            if (r) {
                r.bestSchool = d.bestSchool;
                r.strongSchools = d.strongSchools || [];
                r.consensusScore = d.consensusScore;
                r.schools = d.schools || {};
            }
        }
    }

    if (stageId === 6 && data.details) {
        // Stage 6: Valuation
        for (const d of data.details) {
            const r = pipelineResults.find(x => x.symbol === d.symbol);
            if (r) {
                r.price = d.price;
                r.valuations = d.valuations || {};
                r.intrinsicValue = d.intrinsicValue;
                r.marginOfSafety = d.marginOfSafety;
                r.moat = d.moat;
            }
        }
    }

    if (stageId === 8 && data.portfolio) {
        // Stage 8: Final rankings
        for (const p of data.portfolio) {
            const r = pipelineResults.find(x => x.symbol === p.symbol);
            if (r) {
                r.compositeScore = p.compositeScore;
                r.conviction = p.conviction;
                r.verdict = p.verdict;
                r.positionWeight = p.positionWeight;
                r.moat = p.moat;
                r.riskTier = p.riskTier;
                r.bestSchool = p.bestSchool;
                r.selectionReasons = p.selectionReasons || [];
                r.name = p.name || r.name;
            }
        }
        renderResultCards();
    }

    // Initialize results from stage 1 or 2
    if (stageId <= 2 && data.symbols) {
        pipelineResults = data.symbols.map(s => ({
            symbol: s, name: '', eliminated: false, eliminatedAtStage: 0,
            compositeScore: 0, conviction: 'NONE', verdict: 'HOLD',
        }));
    }
}

function onPipelineComplete(event) {
    const report = event.report;
    if (!report) return;

    // Show results section
    document.getElementById('pipeline-results').classList.remove('hidden');

    // Update pipelineResults with final report data
    if (report.portfolio) {
        for (const p of report.portfolio) {
            const r = pipelineResults.find(x => x.symbol === p.symbol);
            if (r) Object.assign(r, p);
        }
    }
    if (report.eliminated) {
        for (const e of report.eliminated) {
            let r = pipelineResults.find(x => x.symbol === e.symbol);
            if (!r) {
                r = { symbol: e.symbol, name: e.name || '', eliminated: true };
                pipelineResults.push(r);
            }
            r.eliminated = true;
            r.eliminatedAtStage = e.eliminatedAtStage || 0;
            r.eliminationReason = e.reason || '';
        }
    }

    // Store per-stock analysis and comparative matrix
    if (report.perStockAnalysis) {
        window._perStockAnalysis = report.perStockAnalysis;
    }
    if (report.comparativeMatrix) {
        window._comparativeMatrix = report.comparativeMatrix;
    }

    // Store new data: timing, backtest, position
    if (report.timingSignals) {
        window._timingSignals = report.timingSignals;
    }
    if (report.backtestResults) {
        window._backtestResults = report.backtestResults;
    }
    if (report.positionAdvice) {
        window._positionAdvice = report.positionAdvice;
    }

    renderResultCards();
    renderFunnel(report.funnel);

    // LLM report — now show comparative matrix instead of single analysis
    const section = document.getElementById('llm-report-section');
    if (report.comparativeMatrix) {
        section.classList.remove('hidden');
        document.getElementById('llm-report-content').innerHTML = formatMarkdown(report.comparativeMatrix);
    } else if (report.llmAnalysis) {
        section.classList.remove('hidden');
        document.getElementById('llm-report-content').innerHTML = formatMarkdown(report.llmAnalysis);
    }

    document.getElementById('pipeline-status').textContent =
        `分析完成 — 输入 ${report.totalInput} 只, 存活 ${report.totalAlive} 只, 淘汰 ${report.totalEliminated} 只`;
}

function renderResultCards() {
    const container = document.getElementById('result-cards');
    document.getElementById('pipeline-results').classList.remove('hidden');

    // Sort: alive first (by score desc), then eliminated
    const alive = pipelineResults.filter(r => !r.eliminated).sort((a, b) => (b.compositeScore || 0) - (a.compositeScore || 0));
    const eliminated = pipelineResults.filter(r => r.eliminated);
    const sorted = [...alive, ...eliminated];

    container.innerHTML = sorted.map(r => {
        const isEliminated = r.eliminated;
        const score = r.compositeScore || 0;
        const verdict = isEliminated ? 'ELIMINATED' : (r.verdict || 'HOLD');
        const verdictCn = {
            'STRONG_BUY': '强力买入', 'BUY': '买入', 'HOLD': '持有',
            'AVOID': '回避', 'REJECT': '拒绝', 'ELIMINATED': '淘汰',
        }[verdict] || verdict;

        const scoreColor = score >= 70 ? 'var(--green)' : score >= 45 ? 'var(--yellow)' : 'var(--red)';

        let metricsHTML = '';
        if (!isEliminated) {
            // Timing signal from window._timingSignals
            const timing = (window._timingSignals || {})[r.symbol] || {};
            const timingScore = timing.timingScore || r.timing_score || 0;
            const timingVerdict = timing.timingVerdict || r.timing_verdict || '';
            const timingCn = { 'BUY_NOW': '立即买入', 'WAIT': '等待时机', 'CAUTION': '谨慎' }[timingVerdict] || '';
            const timingColor = timingVerdict === 'BUY_NOW' ? 'var(--green)' :
                               timingVerdict === 'WAIT' ? 'var(--yellow)' : 'var(--red)';

            metricsHTML = `
                <div class="rc-metrics">
                    <div class="rc-metric"><span class="rc-metric-label">安全边际</span><span class="rc-metric-value">${r.marginOfSafety != null ? (r.marginOfSafety * 100).toFixed(0) + '%' : 'N/A'}</span></div>
                    <div class="rc-metric"><span class="rc-metric-label">护城河</span><span class="rc-metric-value">${r.moat || 'N/A'}</span></div>
                    <div class="rc-metric"><span class="rc-metric-label">F-Score</span><span class="rc-metric-value">${r.fScore != null ? r.fScore + '/9' : 'N/A'}</span></div>
                    <div class="rc-metric"><span class="rc-metric-label">时机</span><span class="rc-metric-value" style="color:${timingColor}">${timingCn || 'N/A'}</span></div>
                </div>`;
        }

        let tagsHTML = '';
        if (!isEliminated) {
            const tags = [];
            if (r.moat === 'Wide') tags.push('<span class="rc-tag moat-wide">宽护城河</span>');
            if (r.riskTier === 'FORTRESS') tags.push('<span class="rc-tag fortress">堡垒级</span>');
            if (r.bestSchool) tags.push(`<span class="rc-tag school">${r.bestSchool}</span>`);
            if ((r.strongSchools || []).length > 0) tags.push(`<span class="rc-tag school">${r.strongSchools.length}流派强推</span>`);
            // Action tag
            const pos = (window._positionAdvice || {})[r.symbol] || {};
            const action = pos.action || r.rebalance_action || '';
            const actionCn = { 'INITIATE': '建仓', 'ADD': '加仓', 'HOLD': '持有', 'TRIM': '减仓', 'EXIT': '清仓' }[action];
            if (actionCn) {
                const actionCls = action === 'INITIATE' || action === 'ADD' ? 'action-buy' :
                                  action === 'HOLD' ? 'action-hold' : 'action-sell';
                tags.push(`<span class="rc-tag ${actionCls}">${actionCn}</span>`);
            }
            // Backtest tag
            const bt = (window._backtestResults || {})[r.symbol] || {};
            if (bt.verdict === 'VALIDATED') tags.push('<span class="rc-tag backtest-ok">回测验证</span>');
            tagsHTML = tags.length ? `<div class="rc-tags">${tags.join('')}</div>` : '';
        }

        let elimHTML = '';
        if (isEliminated) {
            const reason = r.eliminationReason || r.gate_failures?.slice(-1)[0] || `在阶段${r.eliminatedAtStage}淘汰`;
            elimHTML = `<div class="rc-elim-reason">${reason}</div>`;
        }

        const weightHTML = !isEliminated && r.positionWeight ? `<div class="rc-weight">${r.positionWeight.toFixed(1)}%</div>` : '';

        return `
            <div class="result-card ${isEliminated ? 'eliminated' : ''}" onclick="selectStock('${r.symbol}')" data-symbol="${r.symbol}">
                ${weightHTML}
                <div class="rc-header">
                    <div>
                        <div class="rc-symbol">${r.symbol}</div>
                        <div class="rc-name">${r.name || ''}</div>
                    </div>
                    <span class="rc-verdict ${verdict}">${verdictCn}</span>
                </div>
                ${!isEliminated ? `
                <div class="rc-score">
                    <div class="rc-score-num" style="color:${scoreColor}">${score.toFixed(0)}</div>
                    <div class="rc-score-label">综合评分</div>
                </div>` : ''}
                ${metricsHTML}
                ${tagsHTML}
                ${elimHTML}
            </div>`;
    }).join('');
}

function renderFunnel(funnel) {
    if (!funnel) return;
    const container = document.getElementById('funnel-bars');
    const funnelEl = document.getElementById('pipeline-funnel');
    funnelEl.classList.remove('hidden');

    const maxVal = funnel.input || 1;
    const stages = [
        { label: '输入', count: funnel.input, cls: 'stage-1' },
        { label: '数据通过', count: funnel.afterData, cls: 'stage-2' },
        { label: '门槛通过', count: funnel.afterGate, cls: 'stage-3' },
        { label: '最终存活', count: funnel.final, cls: 'stage-4' },
    ];

    container.innerHTML = stages.map(s => {
        const pct = Math.max(5, (s.count / maxVal) * 100);
        return `
            <div class="funnel-row">
                <span class="funnel-label">${s.label}</span>
                <div class="funnel-bar-wrap">
                    <div class="funnel-bar ${s.cls}" style="width:${pct}%">${s.count}</div>
                </div>
                <span class="funnel-count">${s.count}只</span>
            </div>`;
    }).join('');
}

function selectStock(symbol) {
    // Highlight card
    document.querySelectorAll('.result-card').forEach(c => c.classList.remove('active'));
    const card = document.querySelector(`.result-card[data-symbol="${symbol}"]`);
    if (card) card.classList.add('active');

    // Find stock data
    const stock = pipelineResults.find(r => r.symbol === symbol);
    if (!stock) return;

    window._currentDetailStock = stock;

    // Show detail panel
    const panel = document.getElementById('stock-detail-panel');
    panel.classList.remove('hidden');

    // Reset to overview tab
    document.querySelectorAll('.detail-tab').forEach(t => t.classList.remove('active'));
    document.querySelector('.detail-tab[data-detail-tab="overview"]').classList.add('active');
    renderDetailTab('overview', stock);

    // Scroll to panel
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderDetailTab(tabName, stock) {
    const content = document.getElementById('detail-content');

    if (tabName === 'overview') {
        content.innerHTML = renderOverviewDetail(stock);
    } else if (tabName === 'timing') {
        content.innerHTML = renderTimingDetail(stock);
    } else if (tabName === 'schools') {
        content.innerHTML = renderSchoolsDetail(stock);
    } else if (tabName === 'valuation') {
        content.innerHTML = renderValuationDetail(stock);
    } else if (tabName === 'forensics') {
        content.innerHTML = renderForensicsDetail(stock);
    } else if (tabName === 'backtest') {
        content.innerHTML = renderBacktestDetail(stock);
    } else if (tabName === 'position') {
        content.innerHTML = renderPositionDetail(stock);
    } else if (tabName === 'ai-analysis') {
        content.innerHTML = renderDeepAnalysisDetail(stock);
    }
}

function renderOverviewDetail(s) {
    const mos = s.marginOfSafety != null ? (s.marginOfSafety * 100).toFixed(1) + '%' : 'N/A';
    const mosColor = (s.marginOfSafety || 0) >= 0.15 ? 'var(--green)' : (s.marginOfSafety || 0) >= 0 ? 'var(--yellow)' : 'var(--red)';

    return `
        <div class="detail-section">
            <h4>${s.symbol} — ${s.name || ''}</h4>
            <div class="metrics-grid" style="margin-bottom:0">
                <div class="metric-section">
                    <h3>核心指标</h3>
                    <div class="metric-cards">
                        <div class="metric-row"><span class="metric-name">股价</span><span class="metric-value">$${(s.price || 0).toFixed(2)}</span></div>
                        <div class="metric-row"><span class="metric-name">内在价值</span><span class="metric-value">$${(s.intrinsicValue || 0).toFixed(2)}</span></div>
                        <div class="metric-row"><span class="metric-name">安全边际</span><span class="metric-value" style="color:${mosColor}">${mos}</span></div>
                        <div class="metric-row"><span class="metric-name">护城河</span><span class="metric-value">${s.moat || 'N/A'}</span></div>
                    </div>
                </div>
                <div class="metric-section">
                    <h3>评估总结</h3>
                    <div class="metric-cards">
                        <div class="metric-row"><span class="metric-name">综合评分</span><span class="metric-value">${(s.compositeScore || 0).toFixed(0)}/100</span></div>
                        <div class="metric-row"><span class="metric-name">信念等级</span><span class="metric-value">${s.conviction || 'NONE'}</span></div>
                        <div class="metric-row"><span class="metric-name">建议仓位</span><span class="metric-value">${(s.positionWeight || 0).toFixed(1)}%</span></div>
                        <div class="metric-row"><span class="metric-name">风险层级</span><span class="metric-value">${s.riskTier || 'N/A'}</span></div>
                    </div>
                </div>
            </div>
        </div>
        ${s.selectionReasons && s.selectionReasons.length ? `
        <div class="detail-section">
            <h4>选择理由</h4>
            <div class="rc-tags" style="gap:6px">
                ${s.selectionReasons.map(r => `<span class="rc-tag school">${r}</span>`).join('')}
            </div>
        </div>` : ''}`;
}

function renderTimingDetail(s) {
    const t = (window._timingSignals || {})[s.symbol] || {};
    const timingScore = t.timingScore || s.timing_score || 0;
    const timingVerdict = t.timingVerdict || s.timing_verdict || 'N/A';
    const rsi = t.rsi || s.rsi_14d;
    const macd = t.macd || s.macd_signal_str || '';
    const ma200 = t.ma200 || s.ma200_position || '';
    const week52 = t.week52Pct || s.week52_position;
    const vol = t.volumeAnomaly || s.volume_anomaly || '';
    const high52 = t.week52High || s.price_52w_high;
    const low52 = t.week52Low || s.price_52w_low;

    const verdictCn = { 'BUY_NOW': '立即买入 — 时机良好', 'WAIT': '等待更好的入场点', 'CAUTION': '谨慎 — 时机不佳' }[timingVerdict] || timingVerdict;
    const verdictColor = timingVerdict === 'BUY_NOW' ? 'var(--green)' : timingVerdict === 'WAIT' ? 'var(--yellow)' : 'var(--red)';

    const rsiColor = rsi != null ? (rsi < 30 ? 'var(--green)' : rsi > 70 ? 'var(--red)' : 'var(--text)') : 'var(--text-muted)';
    const rsiLabel = rsi != null ? (rsi < 30 ? '超卖 (买入信号)' : rsi > 70 ? '超买 (谨慎)' : '中性') : '';
    const macdCn = { 'BULLISH': '看涨', 'BEARISH': '看跌', 'NEUTRAL': '中性' }[macd] || macd;
    const macdColor = macd === 'BULLISH' ? 'var(--green)' : macd === 'BEARISH' ? 'var(--red)' : 'var(--yellow)';
    const ma200Cn = ma200 === 'ABOVE' ? '在 MA200 上方 (上升趋势)' : ma200 === 'BELOW' ? '在 MA200 下方 (逆向机会)' : 'N/A';
    const volCn = { 'HIGH': '放量 (有催化剂)', 'LOW': '缩量', 'NORMAL': '正常' }[vol] || vol;

    return `
        <div class="detail-section">
            <h4>买入时机信号 — ${s.symbol}</h4>
            <div class="timing-verdict-banner" style="background:${verdictColor}20;border-left:4px solid ${verdictColor};padding:16px;border-radius:8px;margin-bottom:20px">
                <div style="font-size:24px;font-weight:700;color:${verdictColor}">${timingScore.toFixed(0)}/100</div>
                <div style="font-size:14px;color:${verdictColor};margin-top:4px">${verdictCn}</div>
            </div>
            <div class="timing-grid">
                <div class="timing-card">
                    <div class="timing-card-title">RSI (14日)</div>
                    <div class="timing-card-value" style="color:${rsiColor}">${rsi != null ? rsi.toFixed(1) : 'N/A'}</div>
                    <div class="timing-card-note">${rsiLabel}</div>
                    ${rsi != null ? `<div class="timing-bar-wrap"><div class="timing-bar" style="width:${rsi}%;background:${rsiColor}"></div></div>` : ''}
                </div>
                <div class="timing-card">
                    <div class="timing-card-title">MACD</div>
                    <div class="timing-card-value" style="color:${macdColor}">${macdCn}</div>
                    <div class="timing-card-note">${macd === 'BULLISH' ? 'MACD 在信号线上方' : macd === 'BEARISH' ? 'MACD 在信号线下方' : '交叉中'}</div>
                </div>
                <div class="timing-card">
                    <div class="timing-card-title">MA200 趋势</div>
                    <div class="timing-card-value">${ma200 || 'N/A'}</div>
                    <div class="timing-card-note">${ma200Cn}</div>
                </div>
                <div class="timing-card">
                    <div class="timing-card-title">52周位置</div>
                    <div class="timing-card-value">${week52 != null ? week52.toFixed(0) + '%' : 'N/A'}</div>
                    <div class="timing-card-note">低:$${(low52||0).toFixed(2)} — 高:$${(high52||0).toFixed(2)}</div>
                    ${week52 != null ? `<div class="timing-bar-wrap"><div class="timing-bar" style="width:${week52}%;background:${week52 < 30 ? 'var(--green)' : week52 > 80 ? 'var(--red)' : 'var(--accent)'}"></div></div>` : ''}
                </div>
                <div class="timing-card">
                    <div class="timing-card-title">成交量</div>
                    <div class="timing-card-value">${volCn || 'N/A'}</div>
                    <div class="timing-card-note">${vol === 'HIGH' ? '近5日放量异动' : vol === 'LOW' ? '近5日缩量' : '成交正常'}</div>
                </div>
                <div class="timing-card">
                    <div class="timing-card-title">当前价格</div>
                    <div class="timing-card-value">$${(s.price||0).toFixed(2)}</div>
                    <div class="timing-card-note">vs 内在价值 $${(s.intrinsicValue||s.intrinsic_value||0).toFixed(2)}</div>
                </div>
            </div>
        </div>`;
}

function renderBacktestDetail(s) {
    const bt = (window._backtestResults || {})[s.symbol] || {};
    const verdictCn = { 'VALIDATED': '方法论验证通过', 'MIXED': '表现参差', 'FAILED': '方法论未通过' }[bt.verdict] || '暂无数据';
    const verdictColor = bt.verdict === 'VALIDATED' ? 'var(--green)' : bt.verdict === 'MIXED' ? 'var(--yellow)' : 'var(--red)';

    function fmtRet(v) { return v != null ? (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%' : 'N/A'; }
    function retColor(v) { return v != null ? (v >= 0 ? 'var(--green)' : 'var(--red)') : 'var(--text-muted)'; }

    const alphaRows = [
        { label: '1年', stock: bt.return1y, sp: bt.sp5001y, alpha: bt.alpha1y },
        { label: '2年', stock: bt.return2y, sp: bt.sp5002y, alpha: bt.alpha2y },
        { label: '3年', stock: bt.return3y, sp: bt.sp5003y, alpha: bt.alpha3y },
    ];

    return `
        <div class="detail-section">
            <h4>历史回测验证 — ${s.symbol}</h4>
            <p style="font-size:12px;color:var(--text-dim);margin-bottom:16px">
                回溯 2-3 年实际股价数据，验证我们的方法论是否能创造超额收益 (Alpha)
            </p>
            <div class="backtest-verdict-banner" style="background:${verdictColor}20;border-left:4px solid ${verdictColor};padding:16px;border-radius:8px;margin-bottom:20px">
                <div style="font-size:18px;font-weight:700;color:${verdictColor}">${verdictCn}</div>
                <div style="font-size:13px;color:var(--text-dim);margin-top:4px">
                    最大回撤: <span style="color:var(--red)">${bt.maxDrawdown != null ? (bt.maxDrawdown * 100).toFixed(1) + '%' : 'N/A'}</span>
                    | Sharpe: <span style="color:var(--accent-light)">${bt.sharpe != null ? bt.sharpe.toFixed(2) : 'N/A'}</span>
                </div>
            </div>
            <table class="valuation-table">
                <thead>
                    <tr><th>周期</th><th>${s.symbol} 收益率</th><th>S&P 500 收益率</th><th>超额 Alpha</th></tr>
                </thead>
                <tbody>
                    ${alphaRows.map(r => `
                        <tr>
                            <td>${r.label}</td>
                            <td style="color:${retColor(r.stock)};font-weight:600">${fmtRet(r.stock)}</td>
                            <td style="color:${retColor(r.sp)}">${fmtRet(r.sp)}</td>
                            <td style="color:${retColor(r.alpha)};font-weight:700">${fmtRet(r.alpha)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
            <div style="margin-top:16px;padding:12px;background:var(--bg-card);border-radius:8px">
                <div style="font-size:12px;color:var(--text-dim);line-height:1.6">
                    <strong>解读方法:</strong> Alpha > 0 表示股票跑赢大盘，验证我们的选股方法论有效。
                    Sharpe > 1.0 表示风险调整后的收益优秀。最大回撤 > -30% 需要注意风险控制。
                </div>
            </div>
        </div>`;
}

function renderPositionDetail(s) {
    const pos = (window._positionAdvice || {})[s.symbol] || {};
    const actionCn = { 'INITIATE': '建仓买入', 'ADD': '加仓', 'HOLD': '维持持有', 'TRIM': '减仓', 'EXIT': '清仓卖出' }[pos.action] || 'N/A';
    const actionColor = pos.action === 'INITIATE' || pos.action === 'ADD' ? 'var(--green)' :
                        pos.action === 'HOLD' ? 'var(--yellow)' : 'var(--red)';

    const timing = (window._timingSignals || {})[s.symbol] || {};
    const timingCn = { 'BUY_NOW': '时机良好', 'WAIT': '等待', 'CAUTION': '谨慎' }[timing.timingVerdict || s.timing_verdict] || '';

    return `
        <div class="detail-section">
            <h4>调仓建议 — ${s.symbol}</h4>
            <div class="position-action-banner" style="background:${actionColor}20;border-left:4px solid ${actionColor};padding:20px;border-radius:8px;margin-bottom:20px">
                <div style="display:flex;justify-content:space-between;align-items:center">
                    <div>
                        <div style="font-size:24px;font-weight:700;color:${actionColor}">${actionCn}</div>
                        <div style="font-size:13px;color:var(--text-dim);margin-top:4px">下次复查: ${pos.nextReview || 'N/A'} (3个月后)</div>
                    </div>
                    <div style="text-align:right">
                        <div style="font-size:13px;color:var(--text-dim)">建议仓位</div>
                        <div style="font-size:28px;font-weight:700;color:var(--accent-light)">${(pos.sizePct || 0).toFixed(1)}%</div>
                    </div>
                </div>
            </div>
            <div class="position-grid">
                <div class="position-card">
                    <div class="position-card-title">理想买入区间</div>
                    <div class="position-card-value" style="color:var(--green)">$${(pos.buyPriceLow||0).toFixed(2)} — $${(pos.buyPriceHigh||0).toFixed(2)}</div>
                    <div class="position-card-note">当前价: $${(s.price||0).toFixed(2)} ${s.price <= (pos.buyPriceHigh||0) ? '(在买入区间内!)' : '(高于买入区间)'}</div>
                </div>
                <div class="position-card">
                    <div class="position-card-title">止损价格</div>
                    <div class="position-card-value" style="color:var(--red)">$${(pos.stopLoss||0).toFixed(2)}</div>
                    <div class="position-card-note">跌破此价格应止损 (${s.price > 0 ? ((1 - (pos.stopLoss||0)/s.price)*100).toFixed(0) : 0}% 下方)</div>
                </div>
                <div class="position-card">
                    <div class="position-card-title">时机信号</div>
                    <div class="position-card-value">${timingCn}</div>
                    <div class="position-card-note">时机评分 ${(timing.timingScore||0).toFixed(0)}/100</div>
                </div>
                <div class="position-card">
                    <div class="position-card-title">综合评分 / 信念</div>
                    <div class="position-card-value">${(s.compositeScore||s.composite_score||0).toFixed(0)} / ${s.conviction || 'N/A'}</div>
                    <div class="position-card-note">${s.verdict || 'HOLD'}</div>
                </div>
            </div>
            <div style="margin-top:20px;padding:16px;background:var(--bg-card);border-radius:8px;border:1px solid var(--border)">
                <h4 style="margin-bottom:8px;color:var(--accent-light)">季度调仓说明</h4>
                <ul style="font-size:13px;color:var(--text-dim);line-height:1.8;padding-left:16px">
                    <li>建议每 <strong>3 个月</strong> 复查一次仓位 (下次: ${pos.nextReview || 'N/A'})</li>
                    <li>当前建议操作: <strong style="color:${actionColor}">${actionCn}</strong></li>
                    <li>仓位控制: 单只股票不超过总资产的 <strong>${(pos.sizePct||0).toFixed(0)}%</strong></li>
                    <li>止损纪律: 股价跌破 <strong>$${(pos.stopLoss||0).toFixed(2)}</strong> 时果断止损</li>
                    <li>买入策略: 分批建仓, 在 $${(pos.buyPriceLow||0).toFixed(2)}-$${(pos.buyPriceHigh||0).toFixed(2)} 区间内逐步买入</li>
                </ul>
            </div>
        </div>`;
}

function renderSchoolsDetail(s) {
    const schools = s.schools || s.school_results || {};
    if (!Object.keys(schools).length) return '<p style="color:var(--text-muted)">暂无流派评估数据</p>';

    const schoolNamesCn = {
        graham: 'Graham 深度价值', buffett: 'Buffett 护城河',
        quantitative: '量化价值', quality: '品质投资',
        valuation: 'Damodaran 估值', contrarian: '逆向价值', garp: 'GARP 成长',
    };

    return `
        <div class="detail-section">
            <h4>七流派共识度: ${(s.consensusScore || s.school_consensus_score || 0).toFixed(0)}/100</h4>
            <p style="font-size:12px;color:var(--text-dim);margin-bottom:12px">最佳适配: ${schoolNamesCn[s.bestSchool || s.best_school] || s.bestSchool || 'N/A'} | 强推流派: ${(s.strongSchools || s.strong_schools || []).length}个</p>
            <div class="school-grid">
                ${Object.entries(schools).map(([key, val]) => {
                    const name = schoolNamesCn[key] || key;
                    const score = val.score || 0;
                    const maxScore = val.maxScore || val.max_score || 1;
                    const pct = maxScore > 0 ? (score / maxScore * 100) : 0;
                    const rec = val.recommendation || '';
                    const barColor = rec === 'STRONG_PASS' ? 'var(--green)' :
                                    rec === 'PASS' ? 'var(--blue)' :
                                    rec === 'MARGINAL' ? 'var(--yellow)' : 'var(--red)';
                    return `
                        <div class="school-card">
                            <div class="school-card-header">
                                <span class="school-name">${name}</span>
                                <span class="school-badge ${rec}">${val.verdictCn || val.verdict_cn || rec}</span>
                            </div>
                            <div class="school-score">${score}/${maxScore} (${pct.toFixed(0)}%)</div>
                            <div class="school-bar-wrap">
                                <div class="school-bar" style="width:${pct}%;background:${barColor}"></div>
                            </div>
                        </div>`;
                }).join('')}
            </div>
        </div>`;
}

function renderValuationDetail(s) {
    const vals = s.valuations || {};
    if (!Object.keys(vals).length) return '<p style="color:var(--text-muted)">暂无估值数据</p>';

    const modelNames = {
        grahamNumber: 'Graham Number', grahamIntrinsicValue: 'Graham 内在价值',
        epv: 'EPV 盈利能力', dcfValue: 'DCF 两阶段', ddmValue: 'DDM 股息折现',
        netNetValue: 'Net-Net (NCAV)', ownerEarningsValue: 'Owner Earnings',
        graham_number: 'Graham Number', graham_iv: 'Graham 内在价值',
        dcf: 'DCF 两阶段', ddm: 'DDM 股息折现', ncav: 'Net-Net (NCAV)',
        owner_earnings: 'Owner Earnings',
    };

    const price = s.price || 0;

    return `
        <div class="detail-section">
            <h4>7 种估值模型 vs 当前价格 $${price.toFixed(2)}</h4>
            <table class="valuation-table">
                <thead><tr><th>估值模型</th><th>估值</th><th>vs 股价</th></tr></thead>
                <tbody>
                    ${Object.entries(vals).map(([key, val]) => {
                        if (!val || val <= 0) return '';
                        const diff = price > 0 ? ((val - price) / price * 100) : 0;
                        const diffColor = diff > 20 ? 'var(--green)' : diff > 0 ? 'var(--blue)' : 'var(--red)';
                        return `<tr>
                            <td style="font-family:inherit;color:var(--text)">${modelNames[key] || key}</td>
                            <td>$${val.toFixed(2)}</td>
                            <td style="color:${diffColor};font-weight:600">${diff > 0 ? '+' : ''}${diff.toFixed(1)}%</td>
                        </tr>`;
                    }).join('')}
                </tbody>
            </table>
            <div style="margin-top:12px">
                <div class="metric-row"><span class="metric-name">共识内在价值 (中位数)</span><span class="metric-value" style="color:var(--accent-light)">$${(s.intrinsicValue || 0).toFixed(2)}</span></div>
                <div class="metric-row"><span class="metric-name">安全边际</span><span class="metric-value" style="color:${(s.marginOfSafety || 0) >= 0.15 ? 'var(--green)' : 'var(--red)'}">${s.marginOfSafety != null ? (s.marginOfSafety * 100).toFixed(1) + '%' : 'N/A'}</span></div>
            </div>
        </div>`;
}

function renderForensicsDetail(s) {
    const flags = s.redFlags || s.red_flags || [];
    return `
        <div class="detail-section">
            <h4>三重排雷检测</h4>
            <div class="metrics-grid" style="margin-bottom:16px">
                <div class="metric-section">
                    <h3>评分概览</h3>
                    <div class="metric-cards">
                        <div class="metric-row"><span class="metric-name">Piotroski F-Score</span><span class="metric-value" style="color:${(s.fScore || 0) >= 7 ? 'var(--green)' : (s.fScore || 0) >= 4 ? 'var(--yellow)' : 'var(--red)'}">${s.fScore || 0}/9</span></div>
                        <div class="metric-row"><span class="metric-name">Altman Z-Score</span><span class="metric-value" style="color:${(s.zScore || 0) >= 2.99 ? 'var(--green)' : (s.zScore || 0) >= 1.81 ? 'var(--yellow)' : 'var(--red)'}">${s.zScore != null ? s.zScore : 'N/A'}</span></div>
                        <div class="metric-row"><span class="metric-name">Beneish M-Score</span><span class="metric-value">${s.mScore != null ? s.mScore : 'N/A'}</span></div>
                        <div class="metric-row"><span class="metric-name">风险层级</span><span class="metric-value">${s.riskTier || 'N/A'}</span></div>
                    </div>
                </div>
            </div>
            ${flags.length > 0 ? `
            <h4>红旗警告 (${flags.length})</h4>
            <ul class="flag-list">
                ${flags.map(f => `
                    <li class="flag-item ${f.severity || 'MEDIUM'}">
                        <strong>${f.name || ''}</strong>
                        <span>${f.detail || ''}</span>
                    </li>`).join('')}
            </ul>` : '<p style="color:var(--green)">未发现红旗警告</p>'}
        </div>`;
}

function renderDeepAnalysisDetail(s) {
    const analysis = s.llm_analysis || s.llmAnalysis || '';
    const schoolOpinions = s.per_school_opinions || {};
    const perStock = window._perStockAnalysis?.[s.symbol] || {};
    const opinions = perStock.schoolOpinions || schoolOpinions;
    const filing = perStock.filingSummary || s.filing_summary || '';
    const comparative = s.comparative_matrix || window._comparativeMatrix || '';

    let html = '';

    // School opinions section
    if (Object.keys(opinions).length > 0) {
        const schoolNamesCn = {
            graham: 'Graham 深度价值', buffett: 'Buffett 护城河',
            quantitative: '量化价值', quality: '品质投资',
            valuation: 'Damodaran 估值', contrarian: '逆向价值', garp: 'GARP 成长',
        };
        const schoolColors = {
            graham: '#4ecdc4', buffett: '#6c5ce7', quantitative: '#fdcb6e',
            quality: '#00b894', valuation: '#0984e3', contrarian: '#e17055', garp: '#a29bfe',
        };

        html += `<div class="detail-section">
            <h4>七大流派各自怎么看 ${s.symbol}</h4>
            <p style="font-size:12px;color:var(--text-dim);margin-bottom:16px">每个流派的独立分析和买入观点</p>
            <div class="school-opinions-grid">`;

        for (const [key, text] of Object.entries(opinions)) {
            const name = schoolNamesCn[key] || key;
            const color = schoolColors[key] || 'var(--accent)';
            const schoolData = (s.schools || s.school_results || {})[key] || {};
            const rec = schoolData.recommendation || schoolData.rec || '';
            const badgeClass = rec === 'STRONG_PASS' ? 'pass' : rec === 'PASS' ? 'pass' : rec === 'MARGINAL' ? 'warn' : 'fail';

            html += `
                <div class="school-opinion-card" style="border-left: 3px solid ${color}">
                    <div class="so-header">
                        <span class="so-name" style="color:${color}">${name}</span>
                        <span class="so-badge ${badgeClass}">${schoolData.verdictCn || schoolData.verdict_cn || rec}</span>
                    </div>
                    <div class="so-body">${formatMarkdown(text.substring(0, 600))}</div>
                </div>`;
        }

        html += `</div></div>`;
    }

    // Filing data section
    if (filing) {
        html += `<div class="detail-section">
            <h4>最新财报分析</h4>
            <div class="filing-content">${formatMarkdown(filing.substring(0, 3000))}</div>
        </div>`;
    }

    // Full LLM analysis
    if (analysis) {
        html += `<div class="detail-section">
            <h4>老查理深度分析</h4>
            <div class="llm-report-content">${formatMarkdown(analysis)}</div>
        </div>`;
    }

    // Comparative matrix (if available)
    if (comparative) {
        html += `<div class="detail-section">
            <h4>Basket 横向对比</h4>
            <div class="llm-report-content">${formatMarkdown(comparative)}</div>
        </div>`;
    }

    if (!html) {
        return '<p style="color:var(--text-muted)">AI 深度分析将在管线完成后显示（需配置 LLM API Key）</p>';
    }

    return html;
}


// ─── 股票分析 (Legacy — 保留兼容) ────────────────────────────
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

    // 估值仪表盘
    renderValuationDashboard(stock);

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

// ─── 估值可视化仪表盘 ────────────────────────────────────
function renderValuationDashboard(s) {
    const dashboard = document.getElementById('valuation-dashboard');

    // Collect valuation models with values
    const models = [];
    const price = s.price || 0;
    if (s.graham_number > 0) models.push({ name: 'Graham Number', value: s.graham_number });
    if (s.intrinsic_value > 0) models.push({ name: '内在价值 (GGM)', value: s.intrinsic_value });
    if (s.ncav_per_share > 0) models.push({ name: 'Net-Net (NCAV)', value: s.ncav_per_share });
    if (s.tangible_book_value > 0) models.push({ name: '有形账面值', value: s.tangible_book_value });

    if (models.length === 0 && !price) {
        dashboard.style.display = 'none';
        return;
    }
    dashboard.style.display = 'block';

    // 1. Valuation Range Chart (horizontal bar chart)
    renderRangeChart(models, price);

    // 2. Quality Radar
    renderRadarChart(s);

    // 3. Margin of Safety Gauge
    renderMoSGauge(s);
}

function renderRangeChart(models, price) {
    const container = document.getElementById('valuation-range-chart');
    if (models.length === 0) {
        container.innerHTML = '<div style="color:var(--text-muted);padding:20px;text-align:center">估值数据不足</div>';
        return;
    }

    const allVals = models.map(m => m.value).concat(price ? [price] : []);
    const maxVal = Math.max(...allVals) * 1.2;

    const rows = models.map(m => {
        const pct = (m.value / maxVal * 100).toFixed(1);
        const diff = price > 0 ? ((price - m.value) / m.value * 100).toFixed(1) : null;
        const barColor = price > 0 ? (price < m.value ? 'var(--green)' : price < m.value * 1.2 ? 'var(--yellow)' : 'var(--red)') : 'var(--accent)';
        const label = diff !== null ? (diff > 0 ? `溢价 ${diff}%` : `折价 ${Math.abs(diff)}%`) : '';
        return `
            <div class="range-row">
                <span class="range-label">${escapeHtml(m.name)}</span>
                <div class="range-bar-wrap">
                    <div class="range-bar" style="width:${pct}%;background:${barColor}"></div>
                    ${price > 0 ? `<div class="range-price-mark" style="left:${(price / maxVal * 100).toFixed(1)}%"></div>` : ''}
                </div>
                <span class="range-value">$${m.value.toFixed(2)}</span>
                ${label ? `<span class="range-diff" style="color:${diff > 0 ? 'var(--red)' : 'var(--green)'}">${label}</span>` : ''}
            </div>`;
    });

    container.innerHTML = `
        ${price > 0 ? `<div class="range-price-legend">当前股价: <strong>$${price.toFixed(2)}</strong> <span style="color:var(--text-muted)">(竖线标记)</span></div>` : ''}
        ${rows.join('')}
    `;
}

function renderRadarChart(s) {
    const svg = document.getElementById('radar-svg');
    const cx = 150, cy = 150, maxR = 110;

    // 6 dimensions for investment quality
    const dims = [
        { label: '估值', value: _scoreValuation(s) },
        { label: '盈利', value: _scoreProfitability(s) },
        { label: '安全', value: _scoreSafety(s) },
        { label: '成长', value: _scoreGrowth(s) },
        { label: '分红', value: _scoreDividend(s) },
        { label: '质量', value: _scoreQuality(s) },
    ];

    const n = dims.length;
    const angleStep = (2 * Math.PI) / n;

    // Background rings
    let bgRings = '';
    for (let level = 0.25; level <= 1; level += 0.25) {
        const points = [];
        for (let i = 0; i < n; i++) {
            const angle = -Math.PI / 2 + i * angleStep;
            points.push(`${cx + maxR * level * Math.cos(angle)},${cy + maxR * level * Math.sin(angle)}`);
        }
        bgRings += `<polygon points="${points.join(' ')}" fill="none" stroke="var(--border)" stroke-width="1"/>`;
    }

    // Axis lines
    let axes = '';
    for (let i = 0; i < n; i++) {
        const angle = -Math.PI / 2 + i * angleStep;
        const x = cx + maxR * Math.cos(angle);
        const y = cy + maxR * Math.sin(angle);
        axes += `<line x1="${cx}" y1="${cy}" x2="${x}" y2="${y}" stroke="var(--border)" stroke-width="1"/>`;
    }

    // Labels
    let labels = '';
    for (let i = 0; i < n; i++) {
        const angle = -Math.PI / 2 + i * angleStep;
        const lx = cx + (maxR + 22) * Math.cos(angle);
        const ly = cy + (maxR + 22) * Math.sin(angle);
        const score = dims[i].value;
        const color = score >= 70 ? 'var(--green)' : score >= 40 ? 'var(--yellow)' : 'var(--red)';
        labels += `<text x="${lx}" y="${ly}" text-anchor="middle" dominant-baseline="middle" fill="var(--text-dim)" font-size="12">${dims[i].label}</text>`;
        labels += `<text x="${lx}" y="${ly + 14}" text-anchor="middle" dominant-baseline="middle" fill="${color}" font-size="10" font-weight="700">${score}</text>`;
    }

    // Data polygon
    const dataPoints = [];
    for (let i = 0; i < n; i++) {
        const angle = -Math.PI / 2 + i * angleStep;
        const r = maxR * (dims[i].value / 100);
        dataPoints.push(`${cx + r * Math.cos(angle)},${cy + r * Math.sin(angle)}`);
    }

    svg.innerHTML = `
        ${bgRings}
        ${axes}
        <polygon points="${dataPoints.join(' ')}" fill="rgba(108,92,231,0.2)" stroke="var(--accent)" stroke-width="2"/>
        ${dataPoints.map((p, i) => `<circle cx="${p.split(',')[0]}" cy="${p.split(',')[1]}" r="4" fill="var(--accent)"/>`).join('')}
        ${labels}
    `;
}

// Scoring helpers (0-100)
function _scoreValuation(s) {
    let score = 50;
    if (s.pe > 0 && s.pe < 15) score += 25; else if (s.pe > 0 && s.pe < 25) score += 10; else if (s.pe > 25) score -= 15;
    if (s.pb > 0 && s.pb < 1.5) score += 15; else if (s.pb > 3) score -= 10;
    if (s.margin_of_safety > 0.3) score += 20; else if (s.margin_of_safety > 0) score += 10;
    return Math.max(0, Math.min(100, score));
}

function _scoreProfitability(s) {
    let score = 40;
    if (s.roe > 0.15) score += 25; else if (s.roe > 0.10) score += 15; else if (s.roe > 0) score += 5;
    if (s.profit_margin > 0.15) score += 20; else if (s.profit_margin > 0.05) score += 10;
    if (s.operating_margin > 0.15) score += 15;
    return Math.max(0, Math.min(100, score));
}

function _scoreSafety(s) {
    let score = 50;
    if (s.current_ratio >= 2) score += 20; else if (s.current_ratio >= 1.5) score += 10; else if (s.current_ratio < 1) score -= 20;
    if (s.debt_to_equity < 0.5) score += 20; else if (s.debt_to_equity < 1) score += 10; else if (s.debt_to_equity > 2) score -= 20;
    if (s.interest_coverage_ratio > 5) score += 10; else if (s.interest_coverage_ratio < 2) score -= 10;
    return Math.max(0, Math.min(100, score));
}

function _scoreGrowth(s) {
    let score = 40;
    if (s.earnings_growth_10y > 0.10) score += 30; else if (s.earnings_growth_10y > 0.05) score += 15; else if (s.earnings_growth_10y > 0) score += 5;
    if (s.eps_growth_5y > 0.10) score += 15; else if (s.eps_growth_5y > 0) score += 5;
    if (s.revenue_cagr_10y > 0.08) score += 15;
    return Math.max(0, Math.min(100, score));
}

function _scoreDividend(s) {
    let score = 30;
    if (s.dividend_yield > 3) score += 25; else if (s.dividend_yield > 1.5) score += 15; else if (s.dividend_yield > 0) score += 5;
    if (s.consecutive_dividend_years >= 25) score += 30; else if (s.consecutive_dividend_years >= 10) score += 20; else if (s.consecutive_dividend_years >= 5) score += 10;
    if (s.profitable_years >= 10) score += 15;
    return Math.max(0, Math.min(100, score));
}

function _scoreQuality(s) {
    let score = 50;
    if (s.profitable_years >= 10) score += 20; else if (s.profitable_years >= 7) score += 10;
    if (s.roe > 0.12 && s.debt_to_equity < 1) score += 15;
    if (s.eps > 0 && s.pe > 0 && s.pe < 20) score += 15;
    return Math.max(0, Math.min(100, score));
}

function renderMoSGauge(s) {
    const container = document.getElementById('mos-gauge');
    const mos = s.margin_of_safety;

    if (mos === undefined || mos === null) {
        container.innerHTML = '<div style="color:var(--text-muted);text-align:center;padding:16px">安全边际数据不可用</div>';
        return;
    }

    const mosPct = (mos * 100).toFixed(1);
    const isDiscount = mos > 0;
    const absVal = Math.abs(mos * 100);
    const barWidth = Math.min(absVal, 100);
    const color = isDiscount ? (absVal >= 30 ? 'var(--green)' : 'var(--yellow)') : 'var(--red)';
    const label = isDiscount ? `折价 ${mosPct}%` : `溢价 ${Math.abs(mosPct)}%`;
    const verdict = isDiscount && absVal >= 30 ? '具备安全边际' : isDiscount ? '安全边际不足' : '估值偏高';

    container.innerHTML = `
        <div class="mos-bar-wrap">
            <div class="mos-center-line"></div>
            <div class="mos-bar ${isDiscount ? 'mos-left' : 'mos-right'}" style="width:${barWidth / 2}%;background:${color}"></div>
        </div>
        <div class="mos-labels">
            <span style="color:var(--green)">折价</span>
            <span class="mos-value" style="color:${color};font-weight:700;font-size:18px">${label}</span>
            <span style="color:var(--red)">溢价</span>
        </div>
        <div style="text-align:center;margin-top:6px;color:${color};font-size:13px;font-weight:600">${verdict}</div>
    `;}


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

// ─── 老查理对话 ───────────────────────────────────────────
let chatHistory = [];

document.getElementById('chat-input').addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendChatMessage();
    }
});

async function sendChatMessage() {
    const input = document.getElementById('chat-input');
    const msg = input.value.trim();
    if (!msg) return;

    const container = document.getElementById('chat-messages');
    const btn = document.getElementById('btn-chat-send');

    // Append user bubble
    container.innerHTML += `
        <div class="chat-row user">
            <div class="chat-bubble user">${escapeHtml(msg)}</div>
        </div>`;
    container.scrollTop = container.scrollHeight;
    input.value = '';
    btn.disabled = true;

    chatHistory.push({ role: 'user', content: msg });

    // Show typing indicator
    const typingId = 'typing-' + Date.now();
    container.innerHTML += `
        <div class="chat-row assistant" id="${typingId}">
            <div class="chat-avatar">查</div>
            <div class="chat-bubble assistant typing"><span></span><span></span><span></span></div>
        </div>`;
    container.scrollTop = container.scrollHeight;

    try {
        const resp = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: msg, history: chatHistory }),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '对话失败');

        const reply = data.reply || '(无回复)';
        chatHistory.push({ role: 'assistant', content: reply });

        // Replace typing with actual reply
        const typingEl = document.getElementById(typingId);
        if (typingEl) {
            typingEl.innerHTML = `
                <div class="chat-avatar">查</div>
                <div class="chat-bubble assistant">${formatMarkdown(reply)}</div>`;
        }
    } catch (err) {
        const typingEl = document.getElementById(typingId);
        if (typingEl) {
            typingEl.innerHTML = `
                <div class="chat-avatar">查</div>
                <div class="chat-bubble assistant error-bubble">${escapeHtml(err.message)}</div>`;
        }
    } finally {
        btn.disabled = false;
        container.scrollTop = container.scrollHeight;
        input.focus();
    }
}

// ─── 自选股 ───────────────────────────────────────────────
async function loadWatchlist() {
    const container = document.getElementById('watchlist-container');
    try {
        const resp = await fetch('/api/watchlist');
        const data = await resp.json();
        const items = data.items || [];

        if (items.length === 0) {
            container.innerHTML = '<div class="empty-state"><p>暂无自选股，使用上方输入框添加</p></div>';
            return;
        }

        container.innerHTML = items.map(item => `
            <div class="watchlist-card">
                <div class="wl-header">
                    <span class="wl-symbol">${escapeHtml(item.symbol)}</span>
                    <button class="wl-remove" onclick="removeFromWatchlist('${escapeHtml(item.symbol)}')" title="移除">&times;</button>
                </div>
                <div class="wl-name">${escapeHtml(item.name || '')}</div>
                ${item.note ? `<div class="wl-note">${escapeHtml(item.note)}</div>` : ''}
                <div class="wl-actions">
                    <button class="wl-action-btn" onclick="quickAnalyze('${escapeHtml(item.symbol)}')">分析</button>
                    <span class="wl-date">${item.added_at ? item.added_at.split('T')[0] : ''}</span>
                </div>
            </div>
        `).join('');
    } catch (err) {
        container.innerHTML = `<div class="error-msg">${escapeHtml(err.message)}</div>`;
    }
}

async function addToWatchlist() {
    const input = document.getElementById('watchlist-add-input');
    const symbol = input.value.trim().toUpperCase();
    if (!symbol) return;

    try {
        const resp = await fetch('/api/watchlist', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ symbol, name: '', note: '' }),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error || '添加失败');
        input.value = '';
        loadWatchlist();
    } catch (err) {
        showError(err.message);
    }
}

async function removeFromWatchlist(symbol) {
    try {
        await fetch(`/api/watchlist/${encodeURIComponent(symbol)}`, { method: 'DELETE' });
        loadWatchlist();
    } catch (err) {
        showError(err.message);
    }
}

function quickAnalyze(symbol) {
    document.getElementById('stock-input').value = symbol;
    // Switch to analyze page
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelector('[data-page="analyze"]').classList.add('active');
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById('page-analyze').classList.add('active');
    analyzeStock();
}

document.getElementById('watchlist-add-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') addToWatchlist();
});

// ─── 批量筛选 ─────────────────────────────────────────────
const PRESETS = {
    dow30: 'AAPL, MSFT, AMZN, UNH, GS, HD, MCD, CAT, V, CRM, JPM, TRV, AXP, BA, IBM, AMGN, HON, JNJ, DIS, NKE, PG, KO, MRK, WMT, MMM, CSCO, VZ, DOW, INTC, WBA',
    faang: 'AAPL, AMZN, GOOG, META, MSFT, NFLX, NVDA, TSLA',
    value: 'BRK-B, JNJ, PG, KO, PEP, WMT, XOM, CVX, JPM, BAC, WFC, C, MMM, CAT, GE',
    dividend: 'JNJ, PG, KO, PEP, MMM, ABT, T, VZ, XOM, CVX, O, ABBV, MO, IBM, EMR',
    hk_blue: '0700.HK, 9988.HK, 0005.HK, 0941.HK, 1299.HK, 0388.HK, 0001.HK, 0016.HK, 0002.HK, 2318.HK',
};

function loadPreset(name) {
    const ta = document.getElementById('screener-input');
    if (PRESETS[name]) ta.value = PRESETS[name];
}

async function runBatchScreener() {
    const raw = document.getElementById('screener-input').value.trim();
    if (!raw) return;

    const symbols = raw.split(/[\s,;]+/).filter(s => s.length > 0);
    if (symbols.length === 0) return;

    const btn = document.getElementById('btn-screener');
    const loadingEl = document.getElementById('screener-loading');
    const resultsEl = document.getElementById('screener-results');
    btn.disabled = true;
    loadingEl.classList.remove('hidden');
    resultsEl.classList.add('hidden');

    const results = [];
    const progressEl = document.getElementById('screener-progress');

    for (let i = 0; i < symbols.length; i++) {
        const sym = symbols[i];
        progressEl.textContent = `正在筛选 ${sym} (${i + 1}/${symbols.length})...`;
        try {
            const resp = await fetch('/api/analyze', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ symbol: sym, data_source: 'yfinance' }),
            });
            const data = await resp.json();
            if (resp.ok) {
                results.push({
                    symbol: data.stock?.symbol || sym,
                    name: data.stock?.name || '',
                    price: data.stock?.price,
                    pe: data.stock?.pe,
                    pb: data.stock?.pb,
                    roe: data.stock?.roe,
                    de: data.stock?.debt_to_equity,
                    div_yield: data.stock?.dividend_yield,
                    pass_rate: data.rules?.pass_rate,
                    passed: data.rules?.passed,
                    failed: data.rules?.failed,
                    total: data.rules?.total,
                    status: 'ok',
                });
            } else {
                results.push({ symbol: sym, status: 'error', error: data.error || '失败' });
            }
        } catch (err) {
            results.push({ symbol: sym, status: 'error', error: err.message });
        }
    }

    loadingEl.classList.add('hidden');
    btn.disabled = false;
    renderScreenerResults(results);
}

function renderScreenerResults(results) {
    const el = document.getElementById('screener-results');
    const okResults = results.filter(r => r.status === 'ok');
    const errResults = results.filter(r => r.status !== 'ok');

    // Sort by pass_rate descending
    okResults.sort((a, b) => (b.pass_rate || 0) - (a.pass_rate || 0));

    document.getElementById('screener-summary').innerHTML = `
        <span class="cross-stat-pill match">成功 ${okResults.length}</span>
        ${errResults.length > 0 ? `<span class="cross-stat-pill conflict">失败 ${errResults.length}</span>` : ''}
    `;

    document.getElementById('screener-thead').innerHTML = `
        <tr>
            <th>排名</th>
            <th>代码</th>
            <th>名称</th>
            <th>股价</th>
            <th>PE</th>
            <th>PB</th>
            <th>ROE</th>
            <th>D/E</th>
            <th>股息率</th>
            <th>通过率</th>
            <th>通过/总数</th>
        </tr>`;

    document.getElementById('screener-tbody').innerHTML = okResults.map((r, i) => {
        const rateClass = (r.pass_rate || 0) >= 70 ? 'diff-match' : (r.pass_rate || 0) >= 40 ? 'diff-diverge' : 'diff-conflict';
        return `
            <tr class="screener-row" onclick="quickAnalyze('${escapeHtml(r.symbol)}')">
                <td>${i + 1}</td>
                <td><strong>${escapeHtml(r.symbol)}</strong></td>
                <td>${escapeHtml(r.name)}</td>
                <td>${r.price ? '$' + r.price.toFixed(2) : '—'}</td>
                <td>${r.pe ? r.pe.toFixed(1) : '—'}</td>
                <td>${r.pb ? r.pb.toFixed(2) : '—'}</td>
                <td>${r.roe != null ? (r.roe * 100).toFixed(1) + '%' : '—'}</td>
                <td>${r.de != null ? r.de.toFixed(2) : '—'}</td>
                <td>${r.div_yield != null ? r.div_yield.toFixed(2) + '%' : '—'}</td>
                <td class="${rateClass}" style="font-weight:700">${r.pass_rate != null ? r.pass_rate + '%' : '—'}</td>
                <td>${r.passed || 0}/${r.total || 0}</td>
            </tr>`;
    }).join('') + errResults.map(r => `
        <tr style="opacity:0.5">
            <td>—</td>
            <td>${escapeHtml(r.symbol)}</td>
            <td colspan="9" style="color:var(--red)">${escapeHtml(r.error)}</td>
        </tr>`).join('');

    el.classList.remove('hidden');
}
