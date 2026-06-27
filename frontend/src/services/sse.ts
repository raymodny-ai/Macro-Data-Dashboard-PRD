/**
 * SSE 事件流服务 (EventSource API 封装)
 * 架构原则:
 *   - 废弃轮询, 服务器主动推送
 *   - Last-Event-ID 断线重连恢复
 *   - 多次重连失败后降级为短轮询 fallback
 */

export type SSEEventType =
  | 'connected'
  | 'data_updated'
  | 'cache_invalidated'
  | 'state_change'
  | 'crisis_alert'
  | 'rules_updated'
  | 'heartbeat';

export interface SSEEvent {
  id?: string;
  event: SSEEventType;
  data: any;
  timestamp: string;
}

export interface SSEConnectionStatus {
  connected: boolean;
  lastEventId: string | null;
  reconnectAttempts: number;
  fallbackToPolling: boolean;
}

class SSEService {
  private eventSource: EventSource | null = null;
  private listeners: Map<SSEEventType, Set<(event: SSEEvent) => void>> = new Map();
  private statusListeners: Set<(status: SSEConnectionStatus) => void> = new Set();
  private reconnectTimer: number | null = null;
  private pollingTimer: number | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectDelay = 3000; // 初始重连延迟 3s
  private lastEventId: string | null = null;
  private isFallbackToPolling = false;
  private pollingCallback: (() => Promise<void>) | null = null;

  constructor(private baseUrl: string) {}

  /**
   * 连接 SSE 端点
   * @param onPollingFallback 降级轮询时的回调函数
   */
  connect(onPollingFallback?: () => Promise<void>): void {
    this.pollingCallback = onPollingFallback || null;
    this.initEventSource();
  }

  private initEventSource(): void {
    const url = `${this.baseUrl}/api/v1/events/stream`;
    const headers: Record<string, string> = {};

    if (this.lastEventId) {
      headers['Last-Event-ID'] = this.lastEventId;
    }

    // EventSource 不支持自定义 header, 通过 URL query 传递
    const fullUrl = this.lastEventId
      ? `${url}?last_event_id=${encodeURIComponent(this.lastEventId)}`
      : url;

    this.eventSource = new EventSource(fullUrl);

    this.eventSource.onopen = () => {
      console.log('[SSE] Connection established');
      this.reconnectAttempts = 0;
      this.updateStatus({
        connected: true,
        lastEventId: this.lastEventId,
        reconnectAttempts: 0,
        fallbackToPolling: false,
      });
    };

    this.eventSource.onmessage = (event) => {
      try {
        const parsed: SSEEvent = JSON.parse(event.data);
        this.lastEventId = parsed.id || null;
        this.handleEvent(parsed);
      } catch (error) {
        console.error('[SSE] Failed to parse event:', error);
      }
    };

    this.eventSource.onerror = (error) => {
      console.warn('[SSE] Connection error:', error);
      this.handleDisconnect();
    };
  }

  private handleEvent(event: SSEEvent): void {
    // 触发对应类型的监听器
    const typeListeners = this.listeners.get(event.event);
    if (typeListeners) {
      typeListeners.forEach((cb) => cb(event));
    }

    // 特殊事件处理
    if (event.event === 'connected') {
      console.log('[SSE] Connected, missed events:', event.data.missed_events);
    } else if (event.event === 'cache_invalidated') {
      console.log('[SSE] Cache invalidated:', event.data.data_group);
    } else if (event.event === 'crisis_alert') {
      console.warn('[SSE] CRISIS ALERT:', event.data);
    }
  }

  private handleDisconnect(): void {
    this.updateStatus({
      connected: false,
      lastEventId: this.lastEventId,
      reconnectAttempts: this.reconnectAttempts,
      fallbackToPolling: this.isFallbackToPolling,
    });

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }

    this.reconnectAttempts++;

    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.warn('[SSE] Max reconnect attempts reached, falling back to polling');
      this.isFallbackToPolling = true;
      this.startPolling();
    } else {
      const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), 60000);
      console.log(`[SSE] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
      this.reconnectTimer = window.setTimeout(() => {
        this.initEventSource();
      }, delay);
    }
  }

  private startPolling(): void {
    if (!this.pollingCallback) {
      console.error('[SSE] No polling callback configured');
      return;
    }

    const poll = async () => {
      try {
        await this.pollingCallback();
      } catch (error) {
        console.error('[SSE Polling] Error:', error);
      }
      this.pollingTimer = window.setTimeout(poll, 30000); // 30s 轮询间隔
    };

    poll();
  }

  /**
   * 注册事件监听器
   */
  on(eventType: SSEEventType, callback: (event: SSEEvent) => void): () => void {
    if (!this.listeners.has(eventType)) {
      this.listeners.set(eventType, new Set());
    }
    this.listeners.get(eventType)!.add(callback);

    // 返回取消订阅函数
    return () => {
      this.listeners.get(eventType)?.delete(callback);
    };
  }

  /**
   * 注册连接状态监听器
   */
  onStatusChange(callback: (status: SSEConnectionStatus) => void): () => void {
    this.statusListeners.add(callback);
    return () => {
      this.statusListeners.delete(callback);
    };
  }

  private updateStatus(status: SSEConnectionStatus): void {
    this.statusListeners.forEach((cb) => cb(status));
  }

  /**
   * 断开连接
   */
  disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.pollingTimer) {
      clearTimeout(this.pollingTimer);
      this.pollingTimer = null;
    }
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    this.listeners.clear();
    this.statusListeners.clear();
  }

  /**
   * 获取当前连接状态
   */
  getStatus(): SSEConnectionStatus {
    return {
      connected: this.eventSource?.readyState === EventSource.OPEN,
      lastEventId: this.lastEventId,
      reconnectAttempts: this.reconnectAttempts,
      fallbackToPolling: this.isFallbackToPolling,
    };
  }
}

// 单例实例
let sseServiceInstance: SSEService | null = null;

export function getSSEService(baseUrl: string = import.meta.env.VITE_API_BASE_URL || ''): SSEService {
  if (!sseServiceInstance) {
    sseServiceInstance = new SSEService(baseUrl);
  }
  return sseServiceInstance;
}
