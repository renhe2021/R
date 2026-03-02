import { Routes, Route } from 'react-router-dom'
import { AppLayout } from './components/layout/AppLayout'
import Dashboard from './pages/Dashboard'
import Screener from './pages/Screener'
import Trading from './pages/Trading'
import Monitor from './pages/Monitor'
import NewsCenter from './pages/NewsCenter'
import KnowledgeBase from './pages/KnowledgeBase'
import BlogManager from './pages/BlogManager'
import Settings from './pages/Settings'
import Analysis from './pages/Analysis'
import BloombergData from './pages/BloombergData'
import ValueInvesting from './pages/ValueInvesting'
import Agent from './pages/Agent'
import Advisor from './pages/Advisor'

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/screener" element={<Screener />} />
        <Route path="/trading" element={<Trading />} />
        <Route path="/analysis" element={<Analysis />} />
        <Route path="/bloomberg" element={<BloombergData />} />
        <Route path="/value" element={<ValueInvesting />} />
        <Route path="/monitor" element={<Monitor />} />
        <Route path="/news" element={<NewsCenter />} />
        <Route path="/knowledge" element={<KnowledgeBase />} />
        <Route path="/blog" element={<BlogManager />} />
        <Route path="/agent" element={<Agent />} />
        <Route path="/advisor" element={<Advisor />} />
        <Route path="/settings" element={<Settings />} />
      </Route>
    </Routes>
  )
}
