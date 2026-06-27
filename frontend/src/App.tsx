/**
 * 主应用组件
 * 整合: DashboardLayout + SSE服务 + Redux Store + 3D曲面 + ECharts + 双轨面板
 */

import React, { useEffect, useState } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import type { AppDispatch, RootState } from './stores';
import { getSSEService } from './services/sse';
import {
  setDashboardSummary,
  setDualTrackStatus,
  updateSSEStatus,
  addSSEEvent,
  openDecoderModal,
} from './stores';
import { DashboardLayout } from './components/DashboardLayout';
import { YieldSurface3D } from './components/YieldSurface3D';
import { YieldSurface2D } from './components/YieldSurface2D';
import { SpreadChart, MacroIndexGauge, ContagionHeatmap } from './components/ECharts';
import { MacroStancePanel, LiquidityRepairPanel, StateDecoderModal } from './components/DualTrackPanels';
import { supports3DRendering, detectDeviceCapabilities } from './utils/deviceDetect';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '';

export const App: React.FC = () => {
  const dispatch = useDispatch<AppDispatch>();
  const dashboard = useSelector((state: RootState) => state.dashboard);
  const dualTrack = useSelector((state: RootState) => state.dualTrack);
  const canRender3D = supports3DRendering();
  const [runtimeDegraded, setRuntimeDegraded] = useState(false);

  // 设备性能检测结果 (控制台日志)
  useEffect(() => {
    const caps = detectDeviceCapabilities();
    console.log(`[App] Rendering mode: ${canRender3D ? '3D WebGL' : '2D Fallback'} | GPU: ${caps.gpuRenderer}`);
  }, [canRender3D]);

  // ==========================================================================
  // SSE 连接与事件处理
  // ==========================================================================
  useEffect(() => {
    const sse = getSSEService(API_BASE);

    // 降级轮询回调 (从 API 拉取最新数据)
    const pollingFallback = async () => {
      try {
        const [summaryRes, dualTrackRes] = await Promise.all([
          fetch(`${API_BASE}/api/v1/dashboard/summary`),
          fetch(`${API_BASE}/api/v1/dual-track/status`),
        ]);
        if (summaryRes.ok) {
          const summary = await summaryRes.json();
          dispatch(setDashboardSummary(summary));
        }
        if (dualTrackRes.ok) {
          const status = await dualTrackRes.json();
          dispatch(setDualTrackStatus(status));
        }
      } catch (error) {
        console.error('[Polling] Failed:', error);
      }
    };

    // 注册 SSE 监听器
    sse.on('connected', (event) => {
      dispatch(updateSSEStatus(sse.getStatus()));
      dispatch(addSSEEvent(event));
      // 首次连接时拉取初始数据
      pollingFallback();
    });

    sse.on('data_updated', (event) => {
      dispatch(addSSEEvent(event));
      // 数据更新后重新拉取
      pollingFallback();
    });

    sse.on('cache_invalidated', (event) => {
      dispatch(addSSEEvent(event));
    });

    sse.on('crisis_alert', (event) => {
      dispatch(addSSEEvent(event));
      dispatch(openDecoderModal(`危机警报: ${JSON.stringify(event.data)}`));
    });

    sse.on('rules_updated', (event) => {
      dispatch(addSSEEvent(event));
      console.log('[SSE] Rules updated:', event.data);
    });

    sse.onStatusChange((status) => {
      dispatch(updateSSEStatus(status));
    });

    // 启动 SSE 连接
    sse.connect(pollingFallback);

    return () => {
      sse.disconnect();
    };
  }, [dispatch]);

  // ==========================================================================
  // 初始数据加载
  // ==========================================================================
  useEffect(() => {
    const fetchData = async () => {
      try {
        const [summaryRes, dualTrackRes] = await Promise.all([
          fetch(`${API_BASE}/api/v1/dashboard/summary`),
          fetch(`${API_BASE}/api/v1/dual-track/status`),
        ]);

        if (summaryRes.ok) {
          const summary = await summaryRes.json();
          dispatch(setDashboardSummary(summary));
        }

        if (dualTrackRes.ok) {
          const status = await dualTrackRes.json();
          dispatch(setDualTrackStatus(status));
        }
      } catch (error) {
        console.error('[Initial Fetch] Failed:', error);
      }
    };

    fetchData();
  }, [dispatch]);

  // ==========================================================================
  // 渲染
  // ==========================================================================
  return (
    <DashboardLayout
      leftPanel={
        <>
          <MacroStancePanel />
          <div className="p-4">
            {dashboard.inflation && (
              <MacroIndexGauge value={dashboard.inflation.cpi_acceleration || 50} />
            )}
          </div>
        </>
      }
      rightPanel={
        <>
          <LiquidityRepairPanel />
          <div className="p-4 space-y-4">
            {dashboard.liquidity?.records && (
              <SpreadChart data={dashboard.liquidity.records} />
            )}
            {dashboard.contagion && (
              <ContagionHeatmap data={dashboard.contagion} />
            )}
          </div>
        </>
      }
    >
      {/* 中央收益率曲面 (自适应: 3D WebGL / 2D 热力图 / 折线图) */}
      {canRender3D && !runtimeDegraded ? (
        <YieldSurface3D
          data={null} // TODO: B6 集成收益率曲面 API
          terms={['1M', '3M', '6M', '1Y', '2Y', '5Y', '10Y', '30Y']}
          dates={[]}
          onPerformanceDegraded={() => {
            console.warn('[App] Runtime FPS degradation detected, switching to 2D');
            setRuntimeDegraded(true);
          }}
        />
      ) : (
        <YieldSurface2D
          data={null}
          terms={['1M', '3M', '6M', '1Y', '2Y', '5Y', '10Y', '30Y']}
          dates={[]}
        />
      )}

      {/* 状态解码器弹窗 */}
      <StateDecoderModal />
    </DashboardLayout>
  );
};
