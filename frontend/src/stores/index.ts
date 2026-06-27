/**
 * Redux Toolkit 全局状态管理
 * 架构原则:
 *   - SSE 事件流数据集成到全局 Store
 *   - 五组数据 + 双轨状态 + SSE 连接状态统一管理
 */

import { createSlice, PayloadAction, configureStore } from '@reduxjs/toolkit';
import type { SSEConnectionStatus, SSEEvent } from '../services/sse';

// ==========================================================================
// Dashboard Summary State
// ==========================================================================
interface DashboardState {
  inflation: any | null;
  fiscal: any | null;
  liquidity: any | null;
  ai_capex: any | null;
  contagion: any | null;
  lastUpdated: string | null;
  loading: boolean;
  error: string | null;
}

const initialStateDashboard: DashboardState = {
  inflation: null,
  fiscal: null,
  liquidity: null,
  ai_capex: null,
  contagion: null,
  lastUpdated: null,
  loading: false,
  error: null,
};

const dashboardSlice = createSlice({
  name: 'dashboard',
  initialState: initialStateDashboard,
  reducers: {
    setSummary: (state, action: PayloadAction<any>) => {
      const groups = action.payload.groups || {};
      state.inflation = groups.inflation;
      state.fiscal = groups.fiscal;
      state.liquidity = groups.liquidity;
      state.ai_capex = groups.ai_capex;
      state.contagion = groups.contagion;
      state.lastUpdated = new Date().toISOString();
      state.loading = false;
      state.error = null;
    },
    setLoading: (state) => {
      state.loading = true;
      state.error = null;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.loading = false;
      state.error = action.payload;
    },
  },
});

// ==========================================================================
// Dual Track State
// ==========================================================================
interface DualTrackState {
  track_1_macro_stance: any | null;
  track_2_liquidity_repair: any | null;
  cross_verdict: any | null;
  rules_version: number | null;
  lastUpdated: string | null;
  loading: boolean;
  error: string | null;
}

const initialStateDualTrack: DualTrackState = {
  track_1_macro_stance: null,
  track_2_liquidity_repair: null,
  cross_verdict: null,
  rules_version: null,
  lastUpdated: null,
  loading: false,
  error: null,
};

const dualTrackSlice = createSlice({
  name: 'dualTrack',
  initialState: initialStateDualTrack,
  reducers: {
    setStatus: (state, action: PayloadAction<any>) => {
      state.track_1_macro_stance = action.payload.track_1_macro_stance;
      state.track_2_liquidity_repair = action.payload.track_2_liquidity_repair;
      state.cross_verdict = action.payload.cross_verdict;
      state.rules_version = action.payload.rules_version;
      state.lastUpdated = new Date().toISOString();
      state.loading = false;
      state.error = null;
    },
    setLoading: (state) => {
      state.loading = true;
      state.error = null;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.loading = false;
      state.error = action.payload;
    },
  },
});

// ==========================================================================
// SSE Connection State
// ==========================================================================
interface SSEState {
  status: SSEConnectionStatus;
  recentEvents: SSEEvent[];
  crisisAlerts: any[];
}

const initialStateSSE: SSEState = {
  status: {
    connected: false,
    lastEventId: null,
    reconnectAttempts: 0,
    fallbackToPolling: false,
  },
  recentEvents: [],
  crisisAlerts: [],
};

const sseSlice = createSlice({
  name: 'sse',
  initialState: initialStateSSE,
  reducers: {
    updateStatus: (state, action: PayloadAction<SSEConnectionStatus>) => {
      state.status = action.payload;
    },
    addEvent: (state, action: PayloadAction<SSEEvent>) => {
      state.recentEvents.unshift(action.payload);
      // 保留最近 50 条事件
      if (state.recentEvents.length > 50) {
        state.recentEvents = state.recentEvents.slice(0, 50);
      }

      // 特殊事件处理
      if (action.payload.event === 'crisis_alert') {
        state.crisisAlerts.unshift(action.payload.data);
        if (state.crisisAlerts.length > 10) {
          state.crisisAlerts = state.crisisAlerts.slice(0, 10);
        }
      }
    },
    clearCrisisAlerts: (state) => {
      state.crisisAlerts = [];
    },
  },
});

// ==========================================================================
// UI State (弹窗/时间轴等)
// ==========================================================================
interface UIState {
  decoderModalOpen: boolean;
  decoderMessage: string | null;
  timelineDate: string | null;
  sidebarCollapsed: boolean;
}

const initialStateUI: UIState = {
  decoderModalOpen: false,
  decoderMessage: null,
  timelineDate: null,
  sidebarCollapsed: false,
};

const uiSlice = createSlice({
  name: 'ui',
  initialState: initialStateUI,
  reducers: {
    openDecoderModal: (state, action: PayloadAction<string>) => {
      state.decoderModalOpen = true;
      state.decoderMessage = action.payload;
    },
    closeDecoderModal: (state) => {
      state.decoderModalOpen = false;
      state.decoderMessage = null;
    },
    setTimelineDate: (state, action: PayloadAction<string | null>) => {
      state.timelineDate = action.payload;
    },
    toggleSidebar: (state) => {
      state.sidebarCollapsed = !state.sidebarCollapsed;
    },
  },
});

// ==========================================================================
// Store Configuration
// ==========================================================================
export const store = configureStore({
  reducer: {
    dashboard: dashboardSlice.reducer,
    dualTrack: dualTrackSlice.reducer,
    sse: sseSlice.reducer,
    ui: uiSlice.reducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

// Export actions
export const {
  setSummary: setDashboardSummary,
  setLoading: setDashboardLoading,
  setError: setDashboardError,
} = dashboardSlice.actions;

export const {
  setStatus: setDualTrackStatus,
  setLoading: setDualTrackLoading,
  setError: setDualTrackError,
} = dualTrackSlice.actions;

export const {
  updateStatus: updateSSEStatus,
  addEvent: addSSEEvent,
  clearCrisisAlerts,
} = sseSlice.actions;

export const {
  openDecoderModal,
  closeDecoderModal,
  setTimelineDate,
  toggleSidebar,
} = uiSlice.actions;
