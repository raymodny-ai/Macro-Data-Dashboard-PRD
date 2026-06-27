/**
 * SSE 事件流服务
 * 架构原则: 废弃轮询, 使用 Server-Sent Events 实时接收服务器推送
 * 含自动重连 + 降级轮询 fallback
 */
import { store } from '../stores/store'
import { setSseConnected, setLastUpdateTime, addAlert } from '../stores/dashboardSlice'

const SSE_URL = import.meta.env.VITE_SSE_URL || 'http://localhost:8000/api/v1/events/stream'
const MAX_RECONNECT_ATTEMPTS = 10
const RECONNECT_BASE_DELAY = 1000
const POLLING_FALLBACK_INTERVAL = 30000 // 30秒降级轮询

let eventSource: EventSource | null = null
let reconnectAttempts = 0
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let pollingTimer: ReturnType<typeof setInterval> | null = null

export function connectSSE(): void {
  if (eventSource) {
    eventSource.close()
  }

  eventSource = new EventSource(SSE_URL)

  eventSource.addEventListener('connected', () => {
    store.dispatch(setSseConnected(true))
    reconnectAttempts = 0
    stopPolling()
  })

  eventSource.addEventListener('heartbeat', () => {
    store.dispatch(setLastUpdateTime(new Date().toISOString()))
  })

  eventSource.addEventListener('cache_invalidated', (e: MessageEvent) => {
    const data = JSON.parse(e.data)
    store.dispatch(setLastUpdateTime(data.timestamp || new Date().toISOString()))
  })

  eventSource.addEventListener('alert', (e: MessageEvent) => {
    const data = JSON.parse(e.data)
    store.dispatch(addAlert({
      id: crypto.randomUUID(),
      type: data.alert_type || 'liquidity_crisis',
      message: data.message || '系统状态变更',
      timestamp: data.timestamp || new Date().toISOString(),
      severity: data.severity || 'yellow',
    }))
  })

  eventSource.onerror = () => {
    store.dispatch(setSseConnected(false))
    eventSource?.close()
    eventSource = null
    scheduleReconnect()
  }
}

function scheduleReconnect(): void {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    startPollingFallback()
    return
  }

  const delay = RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts)
  reconnectAttempts++

  reconnectTimer = setTimeout(() => {
    connectSSE()
  }, Math.min(delay, 30000))
}

function startPollingFallback(): void {
  if (pollingTimer) return
  pollingTimer = setInterval(() => {
    connectSSE()
  }, POLLING_FALLBACK_INTERVAL)
}

function stopPolling(): void {
  if (pollingTimer) {
    clearInterval(pollingTimer)
    pollingTimer = null
  }
}

export function disconnectSSE(): void {
  eventSource?.close()
  eventSource = null
  if (reconnectTimer) clearTimeout(reconnectTimer)
  stopPolling()
}
