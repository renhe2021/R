import axios from 'axios'
import type {
  StockQuote, ScreenerResult, NewsArticle, BlogPost,
  TheoryFramework, UserDocument, ResearchResult,
  MonitorAlert, WatchlistItem, AppSettings, SystemStatus, MarketIndex,
  TradingStrategy, TradePosition, TradeOrder, PortfolioSummary, TradeSignal,
  AgentSession, AgentVerdict
} from '../types'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.response.use(
  (res) => res,
  (error) => {
    console.error('[API Error]', error.response?.data || error.message)
    return Promise.reject(error)
  }
)

// Stock APIs
export const stockApi = {
  getQuote: (symbol: string) =>
    api.get<StockQuote>(`/stocks/quote/${symbol}`).then(r => r.data),
  getHistory: (symbol: string, params?: { start?: string; end?: string; interval?: string }) =>
    api.get(`/stocks/history/${symbol}`, { params }).then(r => r.data),
  search: (query: string) =>
    api.get<StockQuote[]>('/stocks/search', { params: { q: query } }).then(r => r.data),
  getMarketOverview: () =>
    api.get<MarketIndex[]>('/stocks/market-overview').then(r => r.data),
}

// Screener APIs
export const screenerApi = {
  run: (params: { markets: string[]; strategyMode: string; weights?: Record<string, number>; theories?: string[] }) =>
    api.post<{ batchId: string; results: ScreenerResult[]; totalCandidates: number; aiSuggestionsCount: number; themes: any[] }>('/screener/run', params).then(r => r.data),
  getResults: (params?: { batchId?: string; limit?: number }) =>
    api.get<ScreenerResult[]>('/screener/results', { params }).then(r => r.data),
  getResult: (id: string) =>
    api.get<ScreenerResult>(`/screener/results/${id}`).then(r => r.data),
  getTrending: (markets?: string) =>
    api.get('/screener/trending', { params: { markets } }).then(r => r.data),
  getUndervalued: (markets?: string) =>
    api.get('/screener/undervalued', { params: { markets } }).then(r => r.data),
  getThemes: () =>
    api.get('/screener/themes').then(r => r.data),
  getStrategyConfigs: () =>
    api.get('/screener/strategy-configs').then(r => r.data),
}

// News APIs
export const newsApi = {
  getList: (params?: { market?: string; sentiment?: string; limit?: number; offset?: number }) =>
    api.get<{ items: NewsArticle[]; total: number }>('/news', { params }).then(r => r.data),
  getDetail: (id: string) =>
    api.get<NewsArticle>(`/news/${id}`).then(r => r.data),
  getSentimentSummary: () =>
    api.get('/news/sentiment/summary').then(r => r.data),
}

// Knowledge APIs
export const knowledgeApi = {
  getTheories: () =>
    api.get<TheoryFramework[]>('/knowledge/theories').then(r => r.data),
  updateTheory: (id: string, data: Partial<TheoryFramework>) =>
    api.put<TheoryFramework>(`/knowledge/theories/${id}`, data).then(r => r.data),
  uploadDocument: (file: File) => {
    const form = new FormData()
    form.append('file', file)
    return api.post<UserDocument>('/knowledge/upload', form, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }).then(r => r.data)
  },
  getDocuments: () =>
    api.get<UserDocument[]>('/knowledge/documents').then(r => r.data),
  search: (query: string, topK?: number) =>
    api.post<{ results: Array<{ content: string; score: number; source: string }> }>(
      '/knowledge/search', { query, top_k: topK }
    ).then(r => r.data),
  research: (query: string, mode: string) =>
    api.post<ResearchResult>('/knowledge/research', { query, mode }).then(r => r.data),
  saveResearch: (id: string) =>
    api.post(`/knowledge/research/${id}/save`).then(r => r.data),
  getResearchHistory: () =>
    api.get<ResearchResult[]>('/knowledge/research').then(r => r.data),
}

// Blog APIs
export const blogApi = {
  getList: (params?: { limit?: number; offset?: number }) =>
    api.get<{ items: BlogPost[]; total: number }>('/blogs', { params }).then(r => r.data),
  getDetail: (id: string) =>
    api.get<BlogPost>(`/blogs/${id}`).then(r => r.data),
  generate: (date?: string) =>
    api.post<BlogPost>('/blogs/generate', { date }).then(r => r.data),
  update: (id: string, data: { title?: string; content?: string }) =>
    api.put<BlogPost>(`/blogs/${id}`, data).then(r => r.data),
}

// Monitor APIs
export const monitorApi = {
  getWatchlist: () =>
    api.get<WatchlistItem[]>('/monitor/watchlist').then(r => r.data),
  addToWatchlist: (symbol: string, market: string) =>
    api.post('/monitor/watchlist', { symbol, market }).then(r => r.data),
  removeFromWatchlist: (symbol: string) =>
    api.delete(`/monitor/watchlist/${symbol}`).then(r => r.data),
  getAlerts: (params?: { unread?: boolean }) =>
    api.get<MonitorAlert[]>('/monitor/alerts', { params }).then(r => r.data),
  markAlertRead: (id: string) =>
    api.put(`/monitor/alerts/${id}/read`).then(r => r.data),
}

// Settings APIs
export const settingsApi = {
  get: () =>
    api.get<AppSettings>('/settings').then(r => r.data),
  update: (data: Partial<AppSettings>) =>
    api.put<AppSettings>('/settings', data).then(r => r.data),
  testConnection: (service: string) =>
    api.post<{ success: boolean; message: string }>('/settings/test-connection', { service }).then(r => r.data),
  getSystemStatus: () =>
    api.get<SystemStatus>('/settings/system-status').then(r => r.data),
  getDataSources: () =>
    api.get<{ current: string; available: Record<string, boolean>; options: string[] }>('/settings/data-sources').then(r => r.data),
  setDataSource: (preference: string) =>
    api.put<{ success: boolean; preference: string }>('/settings/data-sources', { preference }).then(r => r.data),
}

// Trading APIs
export const tradingApi = {
  // Strategies
  getStrategies: (activeOnly?: boolean) =>
    api.get<TradingStrategy[]>('/trading/strategies', { params: { active_only: activeOnly } }).then(r => r.data),
  getStrategy: (id: number) =>
    api.get<TradingStrategy>(`/trading/strategies/${id}`).then(r => r.data),
  createStrategy: (data: Partial<TradingStrategy>) =>
    api.post<TradingStrategy>('/trading/strategies', data).then(r => r.data),
  updateStrategy: (id: number, data: Partial<TradingStrategy>) =>
    api.put<TradingStrategy>(`/trading/strategies/${id}`, data).then(r => r.data),
  deleteStrategy: (id: number) =>
    api.delete(`/trading/strategies/${id}`).then(r => r.data),

  // Orders
  createOrder: (data: {
    symbol: string; market?: string; side: string; quantity: number;
    orderType?: string; price?: number; stopPrice?: number;
    strategyId?: number; signalSource?: string; notes?: string;
    stopLoss?: number; takeProfit?: number; trailingStopPct?: number;
  }) =>
    api.post<TradeOrder>('/trading/orders', data).then(r => r.data),
  getOrders: (params?: { status?: string; limit?: number }) =>
    api.get<TradeOrder[]>('/trading/orders', { params }).then(r => r.data),
  cancelOrder: (id: number) =>
    api.put<TradeOrder>(`/trading/orders/${id}/cancel`).then(r => r.data),

  // Positions
  getPositions: (status?: string) =>
    api.get<TradePosition[]>('/trading/positions', { params: { status } }).then(r => r.data),
  refreshPositions: () =>
    api.post<TradePosition[]>('/trading/positions/refresh').then(r => r.data),
  closePosition: (id: number) =>
    api.post<TradeOrder>(`/trading/positions/${id}/close`).then(r => r.data),

  // Portfolio
  getPortfolio: () =>
    api.get<PortfolioSummary>('/trading/portfolio').then(r => r.data),

  // Signals
  getSignals: (strategyId?: number) =>
    api.post<TradeSignal[]>('/trading/signals/from-screener', null, { params: { strategy_id: strategyId } }).then(r => r.data),
}

// Analysis APIs (Anthropic Financial Services Plugins)
export const analysisApi = {
  getPlugins: () =>
    api.get('/analysis/plugins').then(r => r.data),
  getCategories: () =>
    api.get('/analysis/categories').then(r => r.data),
  run: (pluginId: string, params: Record<string, any>) =>
    api.post('/analysis/run', { pluginId, params }, { timeout: 120000 }).then(r => r.data),
  // Quick access
  comps: (params: { company: string; peers?: string[]; metrics?: string[] }) =>
    api.post('/analysis/comps', params, { timeout: 120000 }).then(r => r.data),
  dcf: (params: { company: string; projectionYears?: number }) =>
    api.post('/analysis/dcf', params, { timeout: 120000 }).then(r => r.data),
  earnings: (params: { company: string; quarter?: string }) =>
    api.post('/analysis/earnings', params, { timeout: 120000 }).then(r => r.data),
  ideas: (params: { direction?: string; style?: string; sector?: string; theme?: string }) =>
    api.post('/analysis/ideas', params, { timeout: 120000 }).then(r => r.data),
  sector: (params: { sector: string; depth?: string }) =>
    api.post('/analysis/sector', params, { timeout: 120000 }).then(r => r.data),
  catalyst: (params: { companies?: string[]; timeframe?: string }) =>
    api.post('/analysis/catalyst', params, { timeout: 120000 }).then(r => r.data),
  thesis: (params: { company: string; position?: string; thesisStatement?: string }) =>
    api.post('/analysis/thesis', params, { timeout: 120000 }).then(r => r.data),
  rebalance: (params: { accounts?: any[]; targetAllocation?: Record<string, number> }) =>
    api.post('/analysis/rebalance', params, { timeout: 120000 }).then(r => r.data),
  tlh: (params: { accounts?: any[] }) =>
    api.post('/analysis/tlh', params, { timeout: 120000 }).then(r => r.data),
  financialPlan: (params: { clientProfile?: Record<string, any> }) =>
    api.post('/analysis/financial-plan', params, { timeout: 120000 }).then(r => r.data),
  investmentProposal: (params: { clientProfile?: Record<string, any>; aum?: number; goals?: string[] }) =>
    api.post('/analysis/investment-proposal', params, { timeout: 120000 }).then(r => r.data),
  dealScreening: (params: { dealInfo?: Record<string, any>; fundCriteria?: Record<string, any> }) =>
    api.post('/analysis/deal-screening', params, { timeout: 120000 }).then(r => r.data),
  returns: (params: { entryEbitda?: number; entryMultiple?: number; leverage?: number; growthRate?: number; holdPeriod?: number }) =>
    api.post('/analysis/returns', params, { timeout: 120000 }).then(r => r.data),
}

// Bloomberg APIs
export const bloombergApi = {
  health: () =>
    api.get('/bloomberg/health').then(r => r.data),
  getHistory: (symbol: string, startDate?: string, endDate?: string, interval?: string) =>
    api.get(`/bloomberg/history/${symbol}`, { params: { start_date: startDate, end_date: endDate, interval } }).then(r => r.data),
  downloadHistory: (params: { symbols: string[]; startDate?: string; endDate?: string; interval?: string }) =>
    api.post('/bloomberg/history/download', { symbols: params.symbols, start_date: params.startDate, end_date: params.endDate, interval: params.interval }, { timeout: 120000 }).then(r => r.data),
  getFinancials: (symbol: string, period?: string, numPeriods?: number) =>
    api.get(`/bloomberg/financials/${symbol}`, { params: { period, num_periods: numPeriods } }).then(r => r.data),
  getFundamentals: (symbol: string) =>
    api.get(`/bloomberg/fundamentals/${symbol}`).then(r => r.data),
  getEarnings: (symbol: string) =>
    api.get(`/bloomberg/earnings/${symbol}`).then(r => r.data),
  getDividends: (symbol: string) =>
    api.get(`/bloomberg/dividends/${symbol}`).then(r => r.data),
  getNews: (query: string, limit?: number) =>
    api.get('/bloomberg/news', { params: { query, limit } }).then(r => r.data),
  screen: (params: { criteria: Record<string, any>; market?: string }) =>
    api.post('/bloomberg/screen', params, { timeout: 120000 }).then(r => r.data),
  batchFundamentals: (symbols: string[]) =>
    api.post('/bloomberg/batch-fundamentals', { symbols }, { timeout: 120000 }).then(r => r.data),
}

// Value Investing APIs
export const valueApi = {
  analyze: (symbol: string) =>
    api.post('/value/analyze', { symbol }, { timeout: 120000 }).then(r => r.data),
  screen: (params: { style: string; sector?: string; market?: string }) =>
    api.post('/value/screen', params, { timeout: 120000 }).then(r => r.data),
  getScreeningStyles: () =>
    api.get('/value/screening-styles').then(r => r.data),
  getLibrary: () =>
    api.get('/value/library').then(r => r.data),
  getLibraryCategories: () =>
    api.get('/value/library/categories').then(r => r.data),
  searchLibrary: (q: string) =>
    api.get('/value/library/search', { params: { q } }).then(r => r.data),
  extractBookKnowledge: (title: string, author?: string) =>
    api.post('/value/library/extract', { title, author }, { timeout: 120000 }).then(r => r.data),
  importPdf: (file: File) => {
    const form = new FormData()
    form.append('file', file)
    return api.post('/value/library/import-pdf', form, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 120000,
    }).then(r => r.data)
  },
  compare: (symbols: string[]) =>
    api.post('/value/compare', { symbols }, { timeout: 120000 }).then(r => r.data),
  addWatchlistNote: (note: { symbol: string; thesis?: string; conviction?: number; targetPrice?: number; positionType?: string }) =>
    api.post('/value/watchlist/note', note).then(r => r.data),
  getWatchlistNotes: (symbol?: string) =>
    api.get('/value/watchlist/notes', { params: { symbol } }).then(r => r.data),
}

// Agent (Old Charlie) APIs
export const agentApi = {
  chatStream: (message: string, sessionId?: string): EventSource | null => {
    // SSE via fetch is handled in the Agent page directly
    return null
  },
  getSessions: (limit?: number) =>
    api.get('/agent/sessions', { params: { limit } }).then(r => r.data.sessions || []),
  getVerdicts: (limit?: number) =>
    api.get('/agent/verdicts', { params: { limit } }).then(r => r.data.verdicts || []),
  getVerdict: (runId: string) =>
    api.get<AgentVerdict>(`/agent/verdicts/${runId}`).then(r => r.data),
  getAdvisorStatus: () =>
    api.get<{ llmAvailable: boolean; mode: string }>('/agent/advisor/status').then(r => r.data),
  getAdvisorHistory: (sessionId: string) =>
    api.get<{ sessionId: string; messages: Array<{ role: string; content: string }> }>(
      `/agent/advisor/history/${sessionId}`
    ).then(r => r.data),
}

// SSE streaming helpers for Agent
export async function* agentChatSSE(
  message: string,
  sessionId?: string
): AsyncGenerator<{ event: string; data: any }> {
  const resp = await fetch('/api/v1/agent/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, sessionId }),
  })
  if (!resp.ok) throw new Error(`Agent chat failed: ${resp.status}`)
  const reader = resp.body!.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() || ''
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const parsed = JSON.parse(line.slice(6))
          yield { event: parsed.event || 'message', data: parsed }
        } catch { /* skip malformed */ }
      }
    }
  }
}

export async function* agentAnalyzeSSE(
  stocks: string[],
  sessionId?: string,
  dataSource?: string,
): AsyncGenerator<{ event: string; data: any }> {
  const resp = await fetch('/api/v1/agent/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stocks, sessionId, dataSource }),
  })
  if (!resp.ok) throw new Error(`Agent analyze failed: ${resp.status}`)
  const reader = resp.body!.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() || ''
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const parsed = JSON.parse(line.slice(6))
          yield { event: parsed.event || 'message', data: parsed }
        } catch { /* skip malformed */ }
      }
    }
  }
}

// Advisor (streaming chat with tool-calling)
export async function* advisorChatSSE(
  message: string,
  sessionId?: string,
  history?: Array<{ role: string; content: string }>,
): AsyncGenerator<{ event: string; data: any }> {
  const resp = await fetch('/api/v1/agent/advisor', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, sessionId, history }),
  })
  if (!resp.ok) throw new Error(`Advisor chat failed: ${resp.status}`)
  const reader = resp.body!.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split('\n')
    buffer = lines.pop() || ''
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const parsed = JSON.parse(line.slice(6))
          yield { event: parsed.event || 'message', data: parsed }
        } catch { /* skip malformed */ }
      }
    }
  }
}

export default api
