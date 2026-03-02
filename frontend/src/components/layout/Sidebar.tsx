import { NavLink } from 'react-router-dom'
import { useAppStore } from '../../stores/appStore'
import { useMarketStore } from '../../stores/marketStore'
import { useI18n } from '../../i18n'
import { cn } from '../../lib/utils'
import {
  LayoutDashboard, Search, Activity, Newspaper,
  BookOpen, FileText, Settings, ChevronLeft, ChevronRight, Zap, TrendingUp, Calculator,
  Database, Scale, Bot, MessageSquare,
} from 'lucide-react'

export function Sidebar() {
  const { sidebarCollapsed, toggleSidebar } = useAppStore()
  const wsConnected = useMarketStore((s) => s.wsConnected)
  const { t } = useI18n()

  const navItems = [
    { path: '/', icon: LayoutDashboard, label: t.nav.dashboard },
    { path: '/screener', icon: Search, label: t.nav.screener },
    { path: '/trading', icon: TrendingUp, label: t.nav.trading },
    { path: '/analysis', icon: Calculator, label: t.nav.analysis },
    { path: '/bloomberg', icon: Database, label: t.nav.bloomberg },
    { path: '/value', icon: Scale, label: t.nav.value },
    { path: '/monitor', icon: Activity, label: t.nav.monitor },
    { path: '/news', icon: Newspaper, label: t.nav.news },
    { path: '/knowledge', icon: BookOpen, label: t.nav.knowledge },
    { path: '/blog', icon: FileText, label: t.nav.blog },
    { path: '/agent', icon: Bot, label: t.nav.agent },
    { path: '/advisor', icon: MessageSquare, label: '投资顾问' },
    { path: '/settings', icon: Settings, label: t.nav.settings },
  ]

  return (
    <aside
      className={cn(
        "fixed left-0 top-0 h-full z-40 flex flex-col bg-surface/90 backdrop-blur-md border-r border-white/5 transition-all duration-300",
        sidebarCollapsed ? "w-16" : "w-60"
      )}
    >
      <div className="flex items-center gap-3 px-4 h-14 border-b border-white/5">
        <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-gradient-to-br from-primary to-accent-purple">
          <Zap className="w-4 h-4 text-white" />
        </div>
        {!sidebarCollapsed && (
          <span className="text-lg font-bold text-gradient">R System</span>
        )}
      </div>

      <nav className="flex-1 py-4 px-2 space-y-1 overflow-y-auto">
        {navItems.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200 group cursor-pointer",
                sidebarCollapsed && "justify-center",
                isActive
                  ? "bg-primary/10 text-primary border-l-2 border-primary"
                  : "text-text-tertiary hover:text-text-primary hover:bg-surface-secondary"
              )
            }
          >
            <item.icon className={cn("w-5 h-5 shrink-0", sidebarCollapsed && "w-5 h-5")} />
            {!sidebarCollapsed && (
              <span className="text-sm font-medium">{item.label}</span>
            )}
          </NavLink>
        ))}
      </nav>

      <div className="p-3 border-t border-white/5">
        <div className={cn("flex items-center gap-2", sidebarCollapsed && "justify-center")}>
          <div className={cn(
            "w-2 h-2 rounded-full",
            wsConnected ? "bg-accent-green animate-pulse" : "bg-accent-red"
          )} />
          {!sidebarCollapsed && (
            <span className="text-xs text-text-muted">
              {wsConnected ? 'Live' : 'Disconnected'}
            </span>
          )}
        </div>
      </div>

      <button
        type="button"
        onClick={toggleSidebar}
        className="absolute -right-3 top-20 w-6 h-6 rounded-full bg-surface-secondary border border-white/10 flex items-center justify-center text-text-muted hover:text-text-primary transition-colors cursor-pointer"
      >
        {sidebarCollapsed ? <ChevronRight className="w-3 h-3" /> : <ChevronLeft className="w-3 h-3" />}
      </button>
    </aside>
  )
}
