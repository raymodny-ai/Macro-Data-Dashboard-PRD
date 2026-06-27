/**
 * 设备性能侦测器
 * 架构原则:
 *   - 检测用户设备 GPU 能力 (WebGL 可用性 / 硬件并发 / 显存)
 *   - 根据性能等级自动选择渲染策略: 3D曲面 / 2D热力图 / 折线图
 *   - 避免老旧设备 WebGL 帧率过低导致卡顿
 */

export enum PerformanceTier {
  HIGH = 'high',       // GPU 强劲, 可渲染 3D 曲面
  MEDIUM = 'medium',   // GPU 一般, 渲染 2D 热力图
  LOW = 'low',         // 无 WebGL 或极弱, 降级折线图
}

interface DeviceCapabilities {
  tier: PerformanceTier;
  webglAvailable: boolean;
  webgl2Available: boolean;
  maxTextureSize: number;
  gpuRenderer: string;
  gpuVendor: string;
  hardwareConcurrency: number;
  deviceMemory: number;   // GB (Chrome only)
  pixelRatio: number;
}

let _cached: DeviceCapabilities | null = null;

/**
 * 检测当前设备性能等级
 * 结果缓存在模块级别, 避免重复检测
 */
export function detectDeviceCapabilities(): DeviceCapabilities {
  if (_cached) return _cached;

  const webglAvailable = !!document.createElement('canvas').getContext('webgl');
  const webgl2Available = !!document.createElement('canvas').getContext('webgl2');

  let maxTextureSize = 0;
  let gpuRenderer = 'unknown';
  let gpuVendor = 'unknown';

  try {
    const canvas = document.createElement('canvas');
    const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl') as WebGLRenderingContext | null;
    if (gl) {
      maxTextureSize = (gl as WebGLRenderingContext).getParameter(
        (gl as WebGLRenderingContext).MAX_TEXTURE_SIZE
      );

      // 获取 GPU 信息 (via WEBGL_debug_renderer_info extension)
      const debugInfo = (gl as WebGLRenderingContext).getExtension('WEBGL_debug_renderer_info');
      if (debugInfo) {
        gpuRenderer = (gl as WebGLRenderingContext).getParameter(debugInfo.UNMASKED_RENDERER_WEBGL);
        gpuVendor = (gl as WebGLRenderingContext).getParameter(debugInfo.UNMASKED_VENDOR_WEBGL);
      }
    }
  } catch {
    // WebGL context creation failed
  }

  const hardwareConcurrency = navigator.hardwareConcurrency || 2;
  const deviceMemory = (navigator as any).deviceMemory || 4;  // Chrome only
  const pixelRatio = window.devicePixelRatio || 1;

  // ==========================================================================
  // 性能分级判定
  // ==========================================================================
  let tier: PerformanceTier;

  if (!webglAvailable) {
    tier = PerformanceTier.LOW;
  } else if (
    webgl2Available &&
    maxTextureSize >= 8192 &&
    hardwareConcurrency >= 4 &&
    deviceMemory >= 4
  ) {
    tier = PerformanceTier.HIGH;
  } else if (
    maxTextureSize >= 4096 &&
    hardwareConcurrency >= 2
  ) {
    tier = PerformanceTier.MEDIUM;
  } else {
    tier = PerformanceTier.LOW;
  }

  // 已知低性能 GPU 黑名单
  const lowPerfGPUs = ['SwiftShader', 'llvmpipe', 'Software', 'Microsoft Basic'];
  if (lowPerfGPUs.some((gpu) => gpuRenderer.toLowerCase().includes(gpu.toLowerCase()))) {
    tier = PerformanceTier.LOW;
  }

  _cached = {
    tier,
    webglAvailable,
    webgl2Available,
    maxTextureSize,
    gpuRenderer,
    gpuVendor,
    hardwareConcurrency,
    deviceMemory,
    pixelRatio,
  };

  console.log(`[DeviceDetect] Tier: ${tier} | GPU: ${gpuRenderer} | WebGL2: ${webgl2Available} | Cores: ${hardwareConcurrency}`);
  return _cached;
}

/**
 * 判断是否支持 3D 渲染
 */
export function supports3DRendering(): boolean {
  const caps = detectDeviceCapabilities();
  return caps.tier === PerformanceTier.HIGH;
}

/**
 * 判断是否支持 2D 热力图
 */
export function supportsHeatmapRendering(): boolean {
  const caps = detectDeviceCapabilities();
  return caps.tier === PerformanceTier.HIGH || caps.tier === PerformanceTier.MEDIUM;
}
