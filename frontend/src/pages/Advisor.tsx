import { useState, useRef, useEffect, useCallback } from 'react'
import { Header } from '../components/layout/Header'
import { Card, CardContent } from '../components/ui/card'
import { Badge } from '../components/ui/badge'
import { cn } from '../lib/utils'
import {
  Bot, Loader2, Send, Trash2, BookOpen, Wrench,
  Sparkles, User, AlertCircle,
} from 'lucide-react'
import { agentApi, advisorChatSSE } from '../services/api'
import { useI18n } from '../i18n'

interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  toolCalls?: Array<{ name: string; arguments: Record<string, any> }>
  toolResults?: Array<{ name: string; result: string }>
  timestamp: Date
}

const TOOL_LABELS: Record<string, { label: string; icon: string }> = {
  search_investment_books: { label: '检索投资书籍', icon: '📚' },
  get_stock_fundamentals: { label: '获取基本面数据', icon: '📊' },
  run_valuation_analysis: { label: '运行估值分析', icon: '⚖️' },
  detect_financial_shenanigans: { label: '财务排雷检测', icon: '🔍' },
  get_stock_news: { label: '获取新闻', icon: '📰' },
  get_price_history: { label: '获取价格历史', icon: '📈' },
  research_topic: { label: '实时研究', icon: '🔬' },
}

const SUGGESTED_QUESTIONS = [
  '帮我分析一下 AAPL 是否值得买入？',
  'Warren Buffett 的 owner earnings 怎么计算？',
  '如何判断一家公司有没有经济护城河？',
  'NVDA 目前的估值合理吗？',
  'Benjamin Graham 的安全边际原则是什么？',
  '帮我比较 MSFT 和 GOOGL 哪个更值得投资',
]

export default function Advisor() {
  const { t } = useI18n()
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [sessionId, setSessionId] = useState<string | null>(null)
  const [llmAvailable, setLlmAvailable] = useState<boolean | null>(null)
  const [currentToolCalls, setCurrentToolCalls] = useState<Array<{ name: string; arguments: Record<string, any> }>>([])
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)

  // Check LLM status on mount
  useEffect(() => {
    agentApi.getAdvisorStatus()
      .then(res => setLlmAvailable(res.llmAvailable))
      .catch(() => setLlmAvailable(false))
  }, [])

  // Auto-scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, currentToolCalls])

  const handleSend = useCallback(async () => {
    const text = input.trim()
    if (!text || loading) return

    const userMsg: ChatMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: text,
      timestamp: new Date(),
    }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    setCurrentToolCalls([])

    // Build history from existing messages (not including the new one — it's in the request)
    const history = messages.map(m => ({ role: m.role, content: m.content }))

    try {
      let fullContent = ''
      const toolCalls: Array<{ name: string; arguments: Record<string, any> }> = []
      const toolResults: Array<{ name: string; result: string }> = []
      let assistantId = `assistant-${Date.now()}`

      for await (const evt of advisorChatSSE(text, sessionId || undefined, history)) {
        const { data } = evt
        const evtType = data.event || evt.event

        if (evtType === 'session') {
          if (data.sessionId && !sessionId) {
            setSessionId(data.sessionId)
          }
        } else if (evtType === 'token') {
          fullContent += data.content || ''
          // Update or create assistant message
          setMessages(prev => {
            const existing = prev.find(m => m.id === assistantId)
            if (existing) {
              return prev.map(m =>
                m.id === assistantId ? { ...m, content: fullContent } : m
              )
            } else {
              return [
                ...prev,
                {
                  id: assistantId,
                  role: 'assistant' as const,
                  content: fullContent,
                  toolCalls,
                  toolResults,
                  timestamp: new Date(),
                },
              ]
            }
          })
        } else if (evtType === 'tool_call') {
          const tc = { name: data.name, arguments: data.arguments || {} }
          toolCalls.push(tc)
          setCurrentToolCalls(prev => [...prev, tc])
        } else if (evtType === 'tool_result') {
          toolResults.push({ name: data.name, result: data.result || '' })
          setCurrentToolCalls(prev => prev.filter(tc => tc.name !== data.name))
        } else if (evtType === 'done') {
          // Ensure final message is saved
          setMessages(prev => {
            const existing = prev.find(m => m.id === assistantId)
            if (existing) {
              return prev.map(m =>
                m.id === assistantId
                  ? { ...m, content: fullContent, toolCalls, toolResults }
                  : m
              )
            } else if (fullContent) {
              return [
                ...prev,
                {
                  id: assistantId,
                  role: 'assistant' as const,
                  content: fullContent,
                  toolCalls,
                  toolResults,
                  timestamp: new Date(),
                },
              ]
            }
            return prev
          })
          setCurrentToolCalls([])
        } else if (evtType === 'error') {
          setMessages(prev => [
            ...prev,
            {
              id: `error-${Date.now()}`,
              role: 'assistant' as const,
              content: `❌ 错误: ${data.message || '未知错误'}`,
              timestamp: new Date(),
            },
          ])
        }
      }
    } catch (err: any) {
      setMessages(prev => [
        ...prev,
        {
          id: `error-${Date.now()}`,
          role: 'assistant' as const,
          content: `❌ 连接错误: ${err.message}`,
          timestamp: new Date(),
        },
      ])
    } finally {
      setLoading(false)
      setCurrentToolCalls([])
    }
  }, [input, loading, messages, sessionId])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const handleNewChat = () => {
    setMessages([])
    setSessionId(null)
    setCurrentToolCalls([])
    inputRef.current?.focus()
  }

  const handleSuggestion = (q: string) => {
    setInput(q)
    inputRef.current?.focus()
  }

  return (
    <div className="flex flex-col h-full">
      <Header title="投资顾问" subtitle="老查理 — 你的深度价值投资顾问" />

      <div className="flex-1 flex flex-col overflow-hidden">
        {/* LLM not configured — setup required */}
        {llmAvailable === false && (
          <div className="flex-1 flex items-center justify-center px-4">
            <Card className="max-w-lg w-full bg-surface-secondary border-accent-red/20">
              <CardContent className="p-6 space-y-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-accent-red/20 flex items-center justify-center">
                    <AlertCircle className="w-5 h-5 text-accent-red" />
                  </div>
                  <div>
                    <p className="font-semibold text-text-primary">需要配置 OpenAI API Key</p>
                    <p className="text-xs text-text-muted">完成以下步骤后刷新页面</p>
                  </div>
                </div>
                <div className="bg-black/30 rounded-lg p-4 space-y-2 text-sm font-mono">
                  <p className="text-text-muted text-xs">1. 编辑 backend/.env 文件，添加：</p>
                  <p className="text-accent-green">OPENAI_API_KEY=sk-your-key-here</p>
                  <p className="text-text-muted text-xs mt-3">2. 如需 Perplexity 实时研究（可选）：</p>
                  <p className="text-accent-green">PERPLEXITY_API_KEY=pplx-your-key-here</p>
                  <p className="text-text-muted text-xs mt-3">3. 重启后端服务</p>
                </div>
                <p className="text-xs text-text-muted">
                  支持 GPT-4o / GPT-4o-mini。可在 .env 中通过 <code className="bg-black/20 px-1 py-0.5 rounded">OPENAI_MODEL</code> 指定模型。
                </p>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Messages + Input area — only when LLM is configured */}
        {llmAvailable !== false && (
        <>
        <div className="flex-1 overflow-y-auto px-4 py-4">
          {messages.length === 0 ? (
            /* Empty state */
            <div className="flex flex-col items-center justify-center h-full gap-6">
              <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-primary/20 to-accent-purple/20 flex items-center justify-center">
                <Bot className="w-10 h-10 text-primary" />
              </div>
              <div className="text-center max-w-lg">
                <p className="text-lg font-semibold text-text-secondary">老查理 — 你的投资顾问</p>
                <p className="text-sm text-text-muted mt-2">
                  我是一位有 30 年华尔街经验的深度价值投资者。我的知识来自 14 本投资经典，
                  可以帮你分析股票、检测财务造假、计算估值，并引用书本原则支持每一个结论。
                </p>
              </div>

              {/* Suggested questions */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-xl w-full">
                {SUGGESTED_QUESTIONS.map((q, i) => (
                  <button
                    key={i}
                    onClick={() => handleSuggestion(q)}
                    className="text-left text-xs px-3 py-2.5 rounded-lg bg-surface-secondary hover:bg-surface-tertiary text-text-secondary hover:text-text-primary border border-white/5 hover:border-primary/20 transition-all cursor-pointer"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            /* Chat messages */
            <div className="max-w-3xl mx-auto space-y-4">
              {messages.map(msg => (
                <div key={msg.id} className={cn('flex gap-3', msg.role === 'user' ? 'justify-end' : 'justify-start')}>
                  {msg.role === 'assistant' && (
                    <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center shrink-0 mt-1">
                      <Bot className="w-4 h-4 text-primary" />
                    </div>
                  )}

                  <div className={cn(
                    'max-w-[80%] rounded-xl px-4 py-3',
                    msg.role === 'user'
                      ? 'bg-primary/20 text-text-primary'
                      : 'bg-surface-secondary text-text-primary'
                  )}>
                    {/* Tool calls badge */}
                    {msg.toolCalls && msg.toolCalls.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 mb-2">
                        {msg.toolCalls.map((tc, i) => {
                          const tool = TOOL_LABELS[tc.name] || { label: tc.name, icon: '🔧' }
                          return (
                            <Badge key={i} variant="secondary" className="text-[10px] gap-1">
                              <span>{tool.icon}</span>
                              {tool.label}
                              {tc.arguments.symbol && <span className="font-semibold ml-0.5">{tc.arguments.symbol}</span>}
                              {tc.arguments.query && <span className="text-text-muted ml-0.5 truncate max-w-[120px]">{tc.arguments.query}</span>}
                            </Badge>
                          )
                        })}
                      </div>
                    )}

                    {/* Message content */}
                    <div className="text-sm leading-relaxed whitespace-pre-wrap break-words">
                      {msg.content}
                    </div>
                  </div>

                  {msg.role === 'user' && (
                    <div className="w-8 h-8 rounded-lg bg-accent-purple/20 flex items-center justify-center shrink-0 mt-1">
                      <User className="w-4 h-4 text-accent-purple" />
                    </div>
                  )}
                </div>
              ))}

              {/* Active tool calls indicator */}
              {currentToolCalls.length > 0 && (
                <div className="flex gap-3">
                  <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center shrink-0 mt-1">
                    <Bot className="w-4 h-4 text-primary" />
                  </div>
                  <div className="bg-surface-secondary rounded-xl px-4 py-3">
                    <div className="flex items-center gap-2 text-xs text-text-muted">
                      <Loader2 className="w-3 h-3 animate-spin" />
                      <span>正在使用工具...</span>
                    </div>
                    <div className="flex flex-wrap gap-1.5 mt-1.5">
                      {currentToolCalls.map((tc, i) => {
                        const tool = TOOL_LABELS[tc.name] || { label: tc.name, icon: '🔧' }
                        return (
                          <Badge key={i} variant="purple" className="text-[10px] gap-1 animate-pulse">
                            <Wrench className="w-3 h-3" />
                            {tool.label}
                            {tc.arguments.symbol && <span className="font-semibold">{tc.arguments.symbol}</span>}
                          </Badge>
                        )
                      })}
                    </div>
                  </div>
                </div>
              )}

              {/* Streaming indicator */}
              {loading && currentToolCalls.length === 0 && !messages.find(m => m.id.startsWith('assistant-') && m.content === '') && (
                <div className="flex gap-3">
                  <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center shrink-0 mt-1">
                    <Bot className="w-4 h-4 text-primary" />
                  </div>
                  <div className="bg-surface-secondary rounded-xl px-4 py-3">
                    <div className="flex items-center gap-2 text-xs text-text-muted">
                      <Sparkles className="w-3 h-3 animate-pulse text-primary" />
                      <span>{t.agent.thinking}</span>
                    </div>
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input area */}
        <div className="border-t border-white/5 bg-surface-primary px-4 py-3">
          <div className="max-w-3xl mx-auto">
            <div className="flex items-end gap-2">
              {/* New chat button */}
              {messages.length > 0 && (
                <button
                  onClick={handleNewChat}
                  className="p-2.5 rounded-lg text-text-muted hover:text-text-secondary hover:bg-surface-secondary transition-colors cursor-pointer shrink-0"
                  title="新对话"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              )}

              {/* Text input */}
              <div className="flex-1 relative">
                <textarea
                  ref={inputRef}
                  value={input}
                  onChange={e => setInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder={t.agent.chatPlaceholder}
                  rows={1}
                  className="w-full bg-surface-secondary rounded-xl px-4 py-3 pr-12 text-sm text-text-primary placeholder:text-text-muted border border-white/5 focus:border-primary/30 focus:outline-none transition-colors resize-none min-h-[44px] max-h-[120px]"
                  disabled={loading}
                  style={{ height: 'auto', overflow: 'hidden' }}
                  onInput={(e) => {
                    const target = e.target as HTMLTextAreaElement
                    target.style.height = 'auto'
                    target.style.height = Math.min(target.scrollHeight, 120) + 'px'
                  }}
                />
                <button
                  onClick={handleSend}
                  disabled={loading || !input.trim()}
                  className="absolute right-2 bottom-2 p-1.5 rounded-lg text-primary hover:bg-primary/10 disabled:opacity-30 disabled:cursor-not-allowed transition-colors cursor-pointer"
                >
                  {loading ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Send className="w-4 h-4" />
                  )}
                </button>
              </div>
            </div>

            <p className="text-[10px] text-text-muted text-center mt-2">
              老查理基于 14 本投资经典的知识库。所有建议仅供参考，不构成投资建议。
            </p>
          </div>
        </div>
        </>
        )}
      </div>
    </div>
  )
}
