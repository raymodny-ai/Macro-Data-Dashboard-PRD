/**
 * 双轨状态矩阵 UI 组件
 * - 第一轨: 宏观立场轨道 (通胀加速度 + AI CapEx + 工资动量)
 * - 第二轨: 流动性修补轨道 (SOFR-IORB利差 + MOVE + 认购倍数)
 * - 实时状态解码器弹窗
 */

import React from 'react';
import { useSelector, useDispatch } from 'react-redux';
import type { RootState } from '../stores';
import { PanelCard, MetricRow, ProgressBar, TrafficLight } from './DashboardLayout';
import { closeDecoderModal } from '../stores';

// ==========================================================================
// 第一轨: 宏观立场轨道
// ==========================================================================
export const MacroStancePanel: React.FC = () => {
  const dualTrack = useSelector((state: RootState) => state.dualTrack.track_1_macro_stance);

  if (!dualTrack) {
    return <PanelCard title="宏观立场轨道"><div className="text-slate-500 text-sm">加载中...</div></PanelCard>;
  }

  const index = dualTrack.index;
  const factors = index?.factors || {};

  return (
    <PanelCard title="第一轨: 宏观立场轨道">
      {/* 宏观紧缩指数大数字 */}
      <div className="mb-4 text-center">
        <div className="text-3xl font-bold text-cyan-400">{index?.index_value?.toFixed(1) || '--'}</div>
        <div className="text-xs text-slate-500 mt-1">宏观紧缩指数</div>
      </div>

      {/* 三因子进度条 */}
      <div className="space-y-3">
        <ProgressBar
          label="通胀加速度 (40%)"
          value={factors.inflation_acceleration?.score || 0}
          color={getScoreColor(factors.inflation_acceleration?.score)}
        />
        <ProgressBar
          label="AI CapEx增速 (30%)"
          value={factors.ai_capex_momentum?.score || 0}
          color={getScoreColor(factors.ai_capex_momentum?.score)}
        />
        <ProgressBar
          label="工资动量 (30%)"
          value={factors.wage_momentum?.score || 0}
          color={getScoreColor(factors.wage_momentum?.score)}
        />
      </div>

      {/* 状态标签 */}
      <div className="mt-4 pt-4 border-t border-slate-800">
        <MetricRow
          label="状态"
          value={dualTrack.label || '--'}
          status={getStatusSeverity(dualTrack.state)}
        />
      </div>
    </PanelCard>
  );
};

// ==========================================================================
// 第二轨: 流动性修补轨道
// ==========================================================================
export const LiquidityRepairPanel: React.FC = () => {
  const dualTrack = useSelector((state: RootState) => state.dualTrack.track_2_liquidity_repair);

  if (!dualTrack) {
    return <PanelCard title="流动性修补轨道"><div className="text-slate-500 text-sm">加载中...</div></PanelCard>;
  }

  const score = dualTrack.score;
  const factors = score?.factors || {};

  return (
    <PanelCard title="第二轨: 流动性修补轨道">
      {/* 流动性风险评分大数字 */}
      <div className="mb-4 text-center">
        <div className="text-3xl font-bold text-cyan-400">{score?.risk_score?.toFixed(1) || '--'}</div>
        <div className="text-xs text-slate-500 mt-1">流动性风险评分</div>
      </div>

      {/* 三因子进度条 */}
      <div className="space-y-3">
        <ProgressBar
          label="SOFR-IORB利差 (50%)"
          value={factors.sofr_iorb_spread?.score || 0}
          color={getScoreColor(factors.sofr_iorb_spread?.score)}
        />
        <ProgressBar
          label="MOVE指数 (30%)"
          value={factors.move_index?.score || 0}
          color={getScoreColor(factors.move_index?.score)}
        />
        <ProgressBar
          label="认购倍数 (20%)"
          value={factors.bid_to_cover?.score || 0}
          color={getScoreColor(factors.bid_to_cover?.score)}
        />
      </div>

      {/* 交通灯状态 */}
      <div className="mt-4 pt-4 border-t border-slate-800 space-y-2">
        <div className="flex items-center justify-between">
          <span className="text-sm text-slate-400">状态</span>
          <TrafficLight state={getTrafficLightState(dualTrack.state)} size="lg" />
        </div>
        <MetricRow
          label=""
          value={dualTrack.label || '--'}
          status={getStatusSeverity(dualTrack.state)}
        />
      </div>
    </PanelCard>
  );
};

// ==========================================================================
// 交叉判定解码器弹窗
// ==========================================================================
export const StateDecoderModal: React.FC = () => {
  const dispatch = useDispatch();
  const isOpen = useSelector((state: RootState) => state.ui.decoderModalOpen);
  const message = useSelector((state: RootState) => state.ui.decoderMessage);
  const crossVerdict = useSelector((state: RootState) => state.dualTrack.cross_verdict);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-slate-900 border border-slate-700 rounded-lg p-6 max-w-md w-full shadow-2xl">
        <h3 className="text-lg font-bold text-slate-200 mb-4">⚠️ 状态解码器</h3>

        {crossVerdict && (
          <div className="space-y-3 mb-4">
            <div className={`p-3 rounded ${getVerdictBg(crossVerdict.level)}`}>
              <div className="text-sm font-semibold mb-1">{crossVerdict.message}</div>
              <div className="text-xs opacity-80">建议操作: {crossVerdict.action}</div>
            </div>
          </div>
        )}

        {message && (
          <div className="text-sm text-slate-400 mb-4 p-3 bg-slate-800 rounded">
            {message}
          </div>
        )}

        <button
          onClick={() => dispatch(closeDecoderModal())}
          className="w-full py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded transition-colors"
        >
          关闭
        </button>
      </div>
    </div>
  );
};

// ==========================================================================
// 辅助函数
// ==========================================================================
function getScoreColor(score?: number): 'green' | 'yellow' | 'red' | 'blue' {
  if (score === undefined) return 'blue';
  if (score > 75) return 'red';
  if (score > 50) return 'yellow';
  return 'green';
}

function getStatusSeverity(state?: string): 'success' | 'warning' | 'danger' | 'neutral' {
  if (!state) return 'neutral';
  if (state.includes('restrictive') || state.includes('paralyzed')) return 'danger';
  if (state.includes('stressed') || state.includes('neutral')) return 'warning';
  return 'success';
}

function getTrafficLightState(state?: string): 'green' | 'yellow' | 'red' | 'gray' {
  if (!state) return 'gray';
  if (state.includes('abundant')) return 'green';
  if (state.includes('stressed')) return 'yellow';
  if (state.includes('paralyzed')) return 'red';
  return 'gray';
}

function getVerdictBg(level: string): string {
  switch (level) {
    case 'critical':
      return 'bg-red-900/50 border border-red-700';
    case 'warning':
      return 'bg-yellow-900/50 border border-yellow-700';
    case 'info':
      return 'bg-blue-900/50 border border-blue-700';
    default:
      return 'bg-emerald-900/50 border border-emerald-700';
  }
}
