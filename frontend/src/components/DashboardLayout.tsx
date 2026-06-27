/**
 * 大屏基础布局组件
 * 架构原则: Tailwind CSS 暗色调响应式布局
 * 
 * 布局结构:
 *   ┌─────────────────────────────────────┐
 *   │         Header (标题 + 状态)         │
 *   ├──────────┬──────────────┬───────────┤
 *   │          │              │           │
 *   │ 左侧面板  │   中央3D曲面  │  右侧面板  │
 *   │(宏观立场)│  (收益率曲线) │(流动性修补)│
 *   │          │              │           │
 *   ├──────────┴──────────────┴───────────┤
 *   │       底部时间轴 + 状态栏            │
 *   └─────────────────────────────────────┘
 */

import React, { ReactNode } from 'react';
import { useSelector } from 'react-redux';
import type { RootState } from '../stores';

interface DashboardLayoutProps {
  children: ReactNode;
  leftPanel?: ReactNode;
  rightPanel?: ReactNode;
  bottomBar?: ReactNode;
}

export const DashboardLayout: React.FC<DashboardLayoutProps> = ({
  children,
  leftPanel,
  rightPanel,
  bottomBar,
}) => {
  const sseStatus = useSelector((state: RootState) => state.sse.status);
  const sidebarCollapsed = useSelector((state: RootState) => state.ui.sidebarCollapsed);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="h-14 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 shrink-0">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-bold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
            宏观流动性状态识别系统
          </h1>
          <span className="text-xs text-slate-500">v0.4.0</span>
        </div>

        {/* SSE 连接状态指示器 */}
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div
              className={`w-2 h-2 rounded-full ${
                sseStatus.connected
                  ? 'bg-emerald-500 animate-pulse'
                  : sseStatus.fallbackToPolling
                  ? 'bg-yellow-500'
                  : 'bg-red-500'
              }`}
            />
            <span className="text-xs text-slate-400">
              {sseStatus.connected
                ? 'SSE 已连接'
                : sseStatus.fallbackToPolling
                ? '降级轮询中'
                : '连接断开'}
            </span>
          </div>

          {sseStatus.reconnectAttempts > 0 && (
            <span className="text-xs text-slate-600">
              重连 #{sseStatus.reconnectAttempts}
            </span>
          )}
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 flex overflow-hidden">
        {/* Left Panel - Macro Stance Track */}
        {leftPanel && (
          <aside
            className={`${
              sidebarCollapsed ? 'w-16' : 'w-80'
            } bg-slate-900/50 border-r border-slate-800 overflow-y-auto transition-all duration-300 shrink-0`}
          >
            {leftPanel}
          </aside>
        )}

        {/* Center - 3D Surface */}
        <section className="flex-1 relative bg-gradient-to-b from-slate-950 to-slate-900">
          {children}
        </section>

        {/* Right Panel - Liquidity Repair Track */}
        {rightPanel && (
          <aside
            className={`${
              sidebarCollapsed ? 'w-16' : 'w-80'
            } bg-slate-900/50 border-l border-slate-800 overflow-y-auto transition-all duration-300 shrink-0`}
          >
            {rightPanel}
          </aside>
        )}
      </main>

      {/* Bottom Bar - Timeline & Status */}
      {bottomBar && (
        <footer className="h-16 bg-slate-900 border-t border-slate-800 shrink-0">
          {bottomBar}
        </footer>
      )}
    </div>
  );
};

/**
 * 面板卡片容器
 */
export const PanelCard: React.FC<{
  title: string;
  children: ReactNode;
  className?: string;
}> = ({ title, children, className = '' }) => {
  return (
    <div className={`p-4 border-b border-slate-800 ${className}`}>
      <h3 className="text-sm font-semibold text-slate-300 mb-3 uppercase tracking-wider">
        {title}
      </h3>
      {children}
    </div>
  );
};

/**
 * 指标行组件
 */
export const MetricRow: React.FC<{
  label: string;
  value: string | number;
  unit?: string;
  status?: 'success' | 'warning' | 'danger' | 'neutral';
}> = ({ label, value, unit, status = 'neutral' }) => {
  const statusColors = {
    success: 'text-emerald-400',
    warning: 'text-yellow-400',
    danger: 'text-red-400',
    neutral: 'text-slate-200',
  };

  return (
    <div className="flex justify-between items-center py-2">
      <span className="text-sm text-slate-400">{label}</span>
      <span className={`font-mono font-bold ${statusColors[status]}`}>
        {value}
        {unit && <span className="text-xs ml-1 text-slate-500">{unit}</span>}
      </span>
    </div>
  );
};

/**
 * 进度条组件
 */
export const ProgressBar: React.FC<{
  value: number;
  max?: number;
  color?: 'green' | 'yellow' | 'red' | 'blue';
  label?: string;
}> = ({ value, max = 100, color = 'blue', label }) => {
  const percentage = Math.min(100, Math.max(0, (value / max) * 100));

  const colorClasses = {
    green: 'bg-emerald-500',
    yellow: 'bg-yellow-500',
    red: 'bg-red-500',
    blue: 'bg-blue-500',
  };

  return (
    <div className="space-y-1">
      {label && (
        <div className="flex justify-between text-xs">
          <span className="text-slate-400">{label}</span>
          <span className="text-slate-300 font-mono">{value.toFixed(1)}</span>
        </div>
      )}
      <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
        <div
          className={`h-full ${colorClasses[color]} transition-all duration-500`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  );
};

/**
 * 交通灯状态指示器
 */
export const TrafficLight: React.FC<{
  state: 'green' | 'yellow' | 'red' | 'gray';
  size?: 'sm' | 'md' | 'lg';
}> = ({ state, size = 'md' }) => {
  const sizeClasses = {
    sm: 'w-3 h-3',
    md: 'w-4 h-4',
    lg: 'w-6 h-6',
  };

  const colorClasses = {
    green: 'bg-emerald-500 shadow-emerald-500/50',
    yellow: 'bg-yellow-500 shadow-yellow-500/50',
    red: 'bg-red-500 shadow-red-500/50',
    gray: 'bg-slate-600',
  };

  return (
    <div
      className={`${sizeClasses[size]} ${colorClasses[state]} rounded-full shadow-lg transition-all duration-300`}
    />
  );
};
