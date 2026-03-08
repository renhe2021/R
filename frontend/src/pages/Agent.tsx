import { useState, useRef, useEffect, useCallback } from 'react'
import { Header } from '../components/layout/Header'
import { Card, CardContent } from '../components/ui/card'
import { Badge } from '../components/ui/badge'
import { cn } from '../lib/utils'
import {
  Bot, Loader2, Send, Search, Shield, Target, FileText,
  AlertTriangle, CheckCircle, XCircle, TrendingUp, TrendingDown,
  ChevronDown, ChevronUp, Play, BarChart3, Zap,
} from 'lucide-react'
import { agentApi, agentAnalyzeSSE } from '../services/api'
import { useI18n } from '../i18n'

/* ── Types ── */

interface PhaseInfo {
  phase: string
  total: number
  current: number
  status: 'pending' | 'running' | 'done'
}

interface RedFlag {
  symbol: string
  category: string
  description: string
  severity: 'CRITICAL' | 'HIGH' | 'MEDIUM'
}

interface ScreeningResult {
  passed: string[]
  eliminated: string[]
  criteria: any[]
}

interface FinalPick {
  symbol: string
  recommendation: string
  intrinsicValue?: number
  marginOfSafety?: number
  reasoning: string
}

interface VerdictReport {
  runId: string
  input_stocks: string[]
  screening: any
  interrogation: any
  appraisals: any
  final_picks: FinalPick[]
  final_avoids: Array<{ symbol: string; reason: string }>
  charlie_summary: string
  llm_analysis?: string
}

interface AnalysisMessage {
  id: string
  content: string
  timestamp: Date
}

/* ── Constants ── */

const PRESET_LISTS: Array<{ label: string; stocks: string[] }> = [
  { label: 'FAANG+', stocks: ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA'] },
  { label: '价值蓝筹', stocks: ['BRK-B', 'JNJ', 'PG', 'KO', 'JPM', 'WMT'] },
  { label: '科技成长', stocks: ['TSLA', 'AMD', 'CRM', 'SNOW', 'PLTR', 'NET'] },
  { label: '股息贵族', stocks: ['T', 'VZ', 'MO', 'XOM', 'CVX', 'PFE'] },
]

const PHASE_CONFIG: Record<string, { icon: typeof Search; label: string; color: string }> = {
  screening: { icon: Search, label: '初筛海选', color: 'text-blue-400' },
  interrogation: { icon: Shield, label: '深度排雷', color: 'text-amber-400' },
  appraisal: { icon: Target, label: '极限估值', color: 'text-emerald-400' },
  reporting: { icon: FileText, label: '生成报告', color: 'text-purple-400' },
}

const REC_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  strong_buy: { bg: 'bg-emerald-500/20', text: 'text-emerald-400', label: '强力买入' },
  buy: { bg: 'bg-green-500/20', text: 'text-green-400', label: '买入' },
  hold: { bg: 'bg-yellow-500/20', text: 'text-yellow-400', label: '持有观望' },
  avoid: { bg: 'bg-red-500/20', text: 'text-red-400', label: '回避' },
}

/* ── Component ── */

export default function Agent() {
  const { t } = useI18n()

  // Input state
  const [stockInput, setStockInput] = useState('')
  const [running, setRunning] = useState(false)

  // Pipeline state
  const [phases, setPhases] = useState<Record<string, PhaseInfo>>({})
  const [screeningResult, setScreeningResult] = useState<ScreeningResult | null>(null)
  const [redFlags, setRedFlags] = useState<RedFlag[]>([])
  const [report, setReport] = useState<VerdictReport | null>(null)
  const [messages, setMessages] = useState<AnalysisMessage[]>([])
  const [doneSummary, setDoneSummary] = useState<any>(null)

  // UI state
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['summary']))
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const toggleSection = (key: string) => {
    setExpandedSections(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const resetState = () => {
    setPhases({})
    setScreeningResult(null)
    setRedFlags([])
    setReport(null)
    setMessages([])
    setDoneSummary(null)
  }

  const handleRun = useCallback(async () => {
    const raw = stockInput.trim().toUpperCase()
    if (!raw || running) return

    const stocks = raw.split(/[\s,;]+/).filter(s => /^[A-Z]{1,5}(-[A-Z])?$/.test(s))
    if (stocks.length === 0) return

    setRunning(true)
    resetState()

    try {
      for await (const evt of agentAnalyzeSSE(stocks)) {
        const { data } = evt
        const evtType = data.event || evt.event

        switch (evtType) {
          case 'phase_start':
            setPhases(prev => ({
              ...prev,
              [data.phase]: {
                phase: data.phase,
                total: data.total || 0,
                current: 0,
                status: 'running',
              },
            }))
            // Mark previous phases as done
            setPhases(prev => {
              const updated = { ...prev }
              for (const [k, v] of Object.entries(updated)) {
                if (k !== data.phase && v.status === 'running') {
                  updated[k] = { ...v, status: 'done' }
                }
              }
              updated[data.phase] = {
                phase: data.phase,
                total: data.total || 0,
                current: 0,
                status: 'running',
              }
              return updated
            })
            break

          case 'screening_result':
            setScreeningResult({
              passed: data.passed || [],
              eliminated: data.eliminated || [],
              criteria: data.criteria || [],
            })
            break

          case 'red_flag':
            setRedFlags(prev => [...prev, {
              symbol: data.symbol,
              category: data.category || '',
              description: data.description || '',
              severity: data.severity || 'MEDIUM',
            }])
            break

          case 'message':
            if (data.content) {
              setMessages(prev => [...prev, {
                id: `msg-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
                content: data.content,
                timestamp: new Date(),
              }])
            }
            break

          case 'report':
            if (data.data) {
              setReport(data.data)
              setExpandedSections(new Set(['summary', 'picks', 'avoids']))
            }
            break

          case 'done':
            if (data.summary) setDoneSummary(data.summary)
            // Mark all phases done
            setPhases(prev => {
              const updated = { ...prev }
              for (const k of Object.keys(updated)) {
                updated[k] = { ...updated[k], status: 'done' }
              }
              return updated
            })
            break

          case 'error':
            setMessages(prev => [...prev, {
              id: `err-${Date.now()}`,
              content: `❌ 错误: ${data.message || '未知错误'}`,
              timestamp: new Date(),
            }])
            break
        }
      }
    } catch (err: any) {
      setMessages(prev => [...prev, {
        id: `err-${Date.now()}`,
        content: `❌ 连接错误: ${err.message}`,
        timestamp: new Date(),
      }])
    } finally {
      setRunning(false)
    }
  }, [stockInput, running])

  const handlePreset = (stocks: string[]) => {
    setStockInput(stocks.join(', '))
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleRun()
    }
  }

  // ── Render Helpers ──

  const phaseOrder = ['screening', 'interrogation', 'appraisal', 'reporting']
  const activePhases = phaseOrder.filter(p => phases[p])

  return (
    <div className="flex flex-col h-full">
      <Header title="老查理分析" subtitle="三阶段 SOP 深度价值分析 — 初筛→排雷→估值" />

      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
        {/* ── Input Area ── */}
        <Card className="bg-surface-secondary border-white/5">
          <CardContent className="p-4 space-y-3">
            <div className="flex items-end gap-2">
              <div className="flex-1">
                <label className="text-xs text-text-muted mb-1.5 block">输入股票代码（逗号/空格分隔）</label>
                <div className="relative">
                  <input
                    type="text"
                    value={stockInput}
                    onChange={e => setStockInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="AAPL, MSFT, GOOGL, BRK-B ..."
                    className="w-full bg-black/20 rounded-lg px-4 py-2.5 text-sm text-text-primary placeholder:text-text-muted border border-white/5 focus:border-primary/30 focus:outline-none transition-colors"
                    disabled={running}
                  />
                </div>
              </div>
              <button
                onClick={handleRun}
                disabled={running || !stockInput.trim()}
                className={cn(
                  'flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium transition-all cursor-pointer',
                  running
                    ? 'bg-surface-tertiary text-text-muted'
                    : 'bg-gradient-to-r from-primary to-accent-purple text-white hover:opacity-90'
                )}
              >
                {running ? (
                  <><Loader2 className="w-4 h-4 animate-spin" />分析中...</>
                ) : (
                  <><Play className="w-4 h-4" />开始分析</>
                )}
              </button>
            </div>

            {/* Preset lists */}
            <div className="flex flex-wrap gap-2">
              {PRESET_LISTS.map(p => (
                <button
                  key={p.label}
                  onClick={() => handlePreset(p.stocks)}
                  disabled={running}
                  className="text-[11px] px-2.5 py-1 rounded-md bg-black/20 text-text-muted hover:text-text-secondary hover:bg-black/30 border border-white/5 transition-colors cursor-pointer disabled:opacity-40"
                >
                  {p.label}: {p.stocks.join(', ')}
                </button>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* ── Pipeline Progress (Funnel) ── */}
        {activePhases.length > 0 && (
          <div className="grid grid-cols-4 gap-2">
            {phaseOrder.map((phaseKey, idx) => {
              const info = phases[phaseKey]
              const config = PHASE_CONFIG[phaseKey]
              if (!config) return null
              const Icon = config.icon
              const isActive = info?.status === 'running'
              const isDone = info?.status === 'done'

              return (
                <div
                  key={phaseKey}
                  className={cn(
                    'rounded-lg border px-3 py-2.5 transition-all',
                    isActive ? 'bg-surface-secondary border-primary/30' :
                    isDone ? 'bg-surface-secondary/50 border-white/5' :
                    'bg-surface-primary/30 border-white/3 opacity-40'
                  )}
                >
                  <div className="flex items-center gap-2 mb-1">
                    {isActive ? (
                      <Loader2 className={cn('w-4 h-4 animate-spin', config.color)} />
                    ) : isDone ? (
                      <CheckCircle className="w-4 h-4 text-emerald-400" />
                    ) : (
                      <Icon className="w-4 h-4 text-text-muted" />
                    )}
                    <span className={cn('text-xs font-medium', isActive ? config.color : isDone ? 'text-text-secondary' : 'text-text-muted')}>
                      {config.label}
                    </span>
                  </div>
                  {info && (
                    <div className="text-[10px] text-text-muted">
                      {info.total > 0 ? `${info.total} 只股票` : '处理中...'}
                    </div>
                  )}
                  {/* Mini funnel bar */}
                  <div className="mt-1.5 h-1 rounded-full bg-black/20 overflow-hidden">
                    <div
                      className={cn(
                        'h-full rounded-full transition-all duration-500',
                        isDone ? 'bg-emerald-500/60 w-full' :
                        isActive ? 'bg-primary/60 w-1/2 animate-pulse' :
                        'w-0'
                      )}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        )}

        {/* ── Screening Result ── */}
        {screeningResult && (
          <Card className="bg-surface-secondary border-white/5">
            <CardContent className="p-4">
              <button onClick={() => toggleSection('screening')} className="flex items-center justify-between w-full cursor-pointer">
                <div className="flex items-center gap-2">
                  <Search className="w-4 h-4 text-blue-400" />
                  <span className="text-sm font-medium text-text-primary">阶段1: 初筛结果</span>
                  <Badge variant="secondary" className="text-[10px]">
                    {screeningResult.passed.length} 通过 / {screeningResult.eliminated.length} 淘汰
                  </Badge>
                </div>
                {expandedSections.has('screening') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
              </button>
              {expandedSections.has('screening') && (
                <div className="mt-3 space-y-2">
                  {screeningResult.passed.length > 0 && (
                    <div className="flex flex-wrap gap-1.5">
                      {screeningResult.passed.map(s => (
                        <Badge key={s} variant="green" className="text-xs">{s} ✓</Badge>
                      ))}
                    </div>
                  )}
                  {screeningResult.eliminated.length > 0 && (
                    <div className="flex flex-wrap gap-1.5">
                      {screeningResult.eliminated.map(s => (
                        <Badge key={s} variant="destructive" className="text-xs">{s} ✗</Badge>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* ── Red Flags ── */}
        {redFlags.length > 0 && (
          <Card className="bg-surface-secondary border-accent-red/10">
            <CardContent className="p-4">
              <button onClick={() => toggleSection('redflags')} className="flex items-center justify-between w-full cursor-pointer">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-red-400" />
                  <span className="text-sm font-medium text-text-primary">红旗警告</span>
                  <Badge variant="destructive" className="text-[10px]">{redFlags.length}</Badge>
                </div>
                {expandedSections.has('redflags') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
              </button>
              {expandedSections.has('redflags') && (
                <div className="mt-3 space-y-1.5">
                  {redFlags.map((rf, i) => (
                    <div key={i} className="flex items-start gap-2 text-xs">
                      <Badge
                        variant={rf.severity === 'CRITICAL' ? 'destructive' : rf.severity === 'HIGH' ? 'orange' : 'secondary'}
                        className="text-[9px] shrink-0 mt-0.5"
                      >
                        {rf.severity}
                      </Badge>
                      <span className="font-medium text-text-secondary">{rf.symbol}</span>
                      <span className="text-text-muted">{rf.description}</span>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* ── Final Report ── */}
        {report && (
          <>
            {/* Final Picks */}
            {report.final_picks && report.final_picks.length > 0 && (
              <Card className="bg-surface-secondary border-emerald-500/10">
                <CardContent className="p-4">
                  <button onClick={() => toggleSection('picks')} className="flex items-center justify-between w-full cursor-pointer">
                    <div className="flex items-center gap-2">
                      <TrendingUp className="w-4 h-4 text-emerald-400" />
                      <span className="text-sm font-medium text-text-primary">最终推荐</span>
                      <Badge variant="green" className="text-[10px]">{report.final_picks.length} 只</Badge>
                    </div>
                    {expandedSections.has('picks') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
                  </button>
                  {expandedSections.has('picks') && (
                    <div className="mt-3 space-y-3">
                      {report.final_picks.map(pick => {
                        const style = REC_STYLES[pick.recommendation] || REC_STYLES.hold
                        return (
                          <div key={pick.symbol} className="rounded-lg bg-black/20 p-3 space-y-2">
                            <div className="flex items-center justify-between">
                              <div className="flex items-center gap-2">
                                <span className="text-sm font-bold text-text-primary">{pick.symbol}</span>
                                <Badge className={cn('text-[10px]', style.bg, style.text)}>{style.label}</Badge>
                              </div>
                              {pick.marginOfSafety != null && typeof pick.marginOfSafety === 'number' && (
                                <span className={cn('text-xs font-medium', pick.marginOfSafety >= 0.3 ? 'text-emerald-400' : pick.marginOfSafety >= 0.1 ? 'text-yellow-400' : 'text-red-400')}>
                                  安全边际 {(pick.marginOfSafety * 100).toFixed(1)}%
                                </span>
                              )}
                            </div>
                            {pick.intrinsicValue != null && (
                              <div className="text-xs text-text-muted">
                                内在价值: <span className="text-text-secondary font-medium">${pick.intrinsicValue.toFixed(2)}</span>
                              </div>
                            )}
                            {pick.reasoning && (
                              <p className="text-xs text-text-muted leading-relaxed">{pick.reasoning}</p>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Final Avoids */}
            {report.final_avoids && report.final_avoids.length > 0 && (
              <Card className="bg-surface-secondary border-red-500/10">
                <CardContent className="p-4">
                  <button onClick={() => toggleSection('avoids')} className="flex items-center justify-between w-full cursor-pointer">
                    <div className="flex items-center gap-2">
                      <XCircle className="w-4 h-4 text-red-400" />
                      <span className="text-sm font-medium text-text-primary">淘汰名单</span>
                      <Badge variant="destructive" className="text-[10px]">{report.final_avoids.length} 只</Badge>
                    </div>
                    {expandedSections.has('avoids') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
                  </button>
                  {expandedSections.has('avoids') && (
                    <div className="mt-3 space-y-1.5">
                      {report.final_avoids.map((a, i) => (
                        <div key={i} className="flex items-start gap-2 text-xs">
                          <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0 mt-0.5" />
                          <span className="font-medium text-text-secondary">{a.symbol}</span>
                          <span className="text-text-muted">{a.reason}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Charlie Summary */}
            {report.charlie_summary && (
              <Card className="bg-surface-secondary border-primary/10">
                <CardContent className="p-4">
                  <button onClick={() => toggleSection('summary')} className="flex items-center justify-between w-full cursor-pointer">
                    <div className="flex items-center gap-2">
                      <Bot className="w-4 h-4 text-primary" />
                      <span className="text-sm font-medium text-text-primary">老查理的投资审判书</span>
                    </div>
                    {expandedSections.has('summary') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
                  </button>
                  {expandedSections.has('summary') && (
                    <div className="mt-3">
                      <pre className="text-xs text-text-secondary leading-relaxed whitespace-pre-wrap font-sans">
                        {report.charlie_summary}
                      </pre>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}
          </>
        )}

        {/* ── Done Summary ── */}
        {doneSummary && (
          <Card className="bg-gradient-to-r from-primary/5 to-accent-purple/5 border-primary/10">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-lg bg-primary/20 flex items-center justify-center">
                  <Zap className="w-5 h-5 text-primary" />
                </div>
                <div>
                  <p className="text-sm font-semibold text-text-primary">分析完成</p>
                  <p className="text-xs text-text-muted">
                    输入 {doneSummary.total} 只 →
                    初筛通过 {doneSummary.passedScreening} 只 →
                    排雷通过 {doneSummary.passedInterrogation} 只 →
                    最终推荐 {doneSummary.final} 只
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* ── Message Log ── */}
        {messages.length > 0 && (
          <Card className="bg-surface-secondary border-white/5">
            <CardContent className="p-4">
              <button onClick={() => toggleSection('log')} className="flex items-center justify-between w-full cursor-pointer">
                <div className="flex items-center gap-2">
                  <BarChart3 className="w-4 h-4 text-text-muted" />
                  <span className="text-sm font-medium text-text-primary">分析日志</span>
                  <Badge variant="secondary" className="text-[10px]">{messages.length}</Badge>
                </div>
                {expandedSections.has('log') ? <ChevronUp className="w-4 h-4 text-text-muted" /> : <ChevronDown className="w-4 h-4 text-text-muted" />}
              </button>
              {expandedSections.has('log') && (
                <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
                  {messages.map(msg => (
                    <div key={msg.id} className="text-xs text-text-muted leading-relaxed whitespace-pre-wrap border-l-2 border-white/5 pl-3">
                      {msg.content}
                    </div>
                  ))}
                  <div ref={messagesEndRef} />
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* ── Empty State ── */}
        {!running && !report && messages.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 gap-6">
            <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-primary/20 to-accent-purple/20 flex items-center justify-center">
              <Bot className="w-10 h-10 text-primary" />
            </div>
            <div className="text-center max-w-lg">
              <p className="text-lg font-semibold text-text-secondary">老查理三阶段 SOP 分析</p>
              <p className="text-sm text-text-muted mt-2">
                输入一批股票代码，老查理会执行三阶段严格筛选：
                <strong className="text-blue-400"> 初筛海选</strong>（纯代码过滤）→
                <strong className="text-amber-400"> 深度排雷</strong>（M-Score/Z-Score/F-Score）→
                <strong className="text-emerald-400"> 极限估值</strong>（7种模型）。
                宁可错杀一千，绝不放过一个。
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
