import { createSlice, PayloadAction } from '@reduxjs/toolkit'

interface DashboardState {
  // SSE 连接状态
  sseConnected: boolean
  lastUpdateTime: string | null
  // 双轨状态
  macroRestrictiveIndex: number | null
  liquidityRiskScore: number | null
  // 预警
  alerts: AlertEvent[]
}

interface AlertEvent {
  id: string
  type: 'liquidity_crisis' | 'contagion' | 'inflation_warning' | 'fiscal_warning'
  message: string
  timestamp: string
  severity: 'red' | 'yellow' | 'green'
}

const initialState: DashboardState = {
  sseConnected: false,
  lastUpdateTime: null,
  macroRestrictiveIndex: null,
  liquidityRiskScore: null,
  alerts: [],
}

const dashboardSlice = createSlice({
  name: 'dashboard',
  initialState,
  reducers: {
    setSseConnected(state, action: PayloadAction<boolean>) {
      state.sseConnected = action.payload
    },
    setLastUpdateTime(state, action: PayloadAction<string>) {
      state.lastUpdateTime = action.payload
    },
    setMacroRestrictiveIndex(state, action: PayloadAction<number>) {
      state.macroRestrictiveIndex = action.payload
    },
    setLiquidityRiskScore(state, action: PayloadAction<number>) {
      state.liquidityRiskScore = action.payload
    },
    addAlert(state, action: PayloadAction<AlertEvent>) {
      state.alerts.unshift(action.payload)
      if (state.alerts.length > 50) state.alerts.pop()
    },
    clearAlerts(state) {
      state.alerts = []
    },
  },
})

export const {
  setSseConnected,
  setLastUpdateTime,
  setMacroRestrictiveIndex,
  setLiquidityRiskScore,
  addAlert,
  clearAlerts,
} = dashboardSlice.actions

export default dashboardSlice.reducer
