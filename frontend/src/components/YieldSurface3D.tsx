/**
 * Three.js 3D 收益率曲面渲染器
 * 架构原则:
 *   - 接收压缩二维数组, 浏览器本地生成 BufferGeometry
 *   - WebGL 显式 GC (geometry.dispose/material.dispose)
 *   - LOD 自动降级 (帧率监测 + 网格密度调整)
 */

import { useEffect, useRef, useState } from 'react';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls';

interface YieldSurfaceProps {
  data: number[][] | null; // [time_index][term_index] = yield_value
  terms?: string[];        // ['1M', '3M', ..., '30Y']
  dates?: string[];        // ['2024-01-01', ...]
  onPerformanceDegraded?: () => void; // 运行时帧率过低回调 (触发父组件切换到 2D)
}

export const YieldSurface3D: React.FC<YieldSurfaceProps> = ({
  data,
  terms = [],
  dates = [],
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const rendererRef = useRef<THREE.WebGLRenderer | null>(null);
  const sceneRef = useRef<THREE.Scene | null>(null);
  const cameraRef = useRef<THREE.PerspectiveCamera | null>(null);
  const controlsRef = useRef<OrbitControls | null>(null);
  const meshRef = useRef<THREE.Mesh | null>(null);
  const frameCountRef = useRef(0);
  const lastTimeRef = useRef(performance.now());
  const fpsRef = useRef(60);
  const lowFpsCountRef = useRef(0); // 连续低帧率计数器
  const degradedRef = useRef(false); // 是否已触发降级
  const [fps, setFps] = useState(60);

  // LOD 配置
  const lodLevels = [
    { minFPS: 50, segments: 100 },  // 高质量
    { minFPS: 30, segments: 50 },   // 中等质量
    { minFPS: 0, segments: 25 },    // 低质量
  ];
  const currentSegmentsRef = useRef(100);

  useEffect(() => {
    if (!containerRef.current) return;

    // ==========================================================================
    // 初始化 Three.js 场景
    // ==========================================================================
    const width = containerRef.current.clientWidth;
    const height = containerRef.current.clientHeight;

    // Scene
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0f172a); // slate-950
    sceneRef.current = scene;

    // Camera
    const camera = new THREE.PerspectiveCamera(60, width / height, 0.1, 1000);
    camera.position.set(50, 40, 50);
    camera.lookAt(0, 0, 0);
    cameraRef.current = camera;

    // Renderer
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    containerRef.current.appendChild(renderer.domElement);
    rendererRef.current = renderer;

    // Controls
    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controlsRef.current = controls;

    // Lighting
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
    scene.add(ambientLight);

    const directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
    directionalLight.position.set(50, 100, 50);
    scene.add(directionalLight);

    // Grid helper
    const gridHelper = new THREE.GridHelper(100, 20, 0x334155, 0x1e293b);
    scene.add(gridHelper);

    // ==========================================================================
    // FPS 监测与 LOD 控制
    // ==========================================================================
    const updateFPS = () => {
      frameCountRef.current++;
      const now = performance.now();
      const elapsed = now - lastTimeRef.current;

      if (elapsed >= 1000) {
        const currentFps = Math.round((frameCountRef.current * 1000) / elapsed);
        fpsRef.current = currentFps;
        setFps(currentFps);

        // LOD 自动降级
        for (const level of lodLevels) {
          if (currentFps >= level.minFPS) {
            if (currentSegmentsRef.current !== level.segments) {
              currentSegmentsRef.current = level.segments;
              console.log(`[LOD] Adjusted to ${level.segments} segments (FPS: ${currentFps})`);
              // 触发重新渲染
              if (data) {
                createSurface(data, level.segments);
              }
            }
            break;
          }
        }

        frameCountRef.current = 0;
        lastTimeRef.current = now;

        // 运行时降级检测: 连续 5 秒 FPS < 15 则触发父组件切换
        if (currentFps < 15 && !degradedRef.current) {
          lowFpsCountRef.current++;
          if (lowFpsCountRef.current >= 5) {
            degradedRef.current = true;
            console.warn('[YieldSurface3D] Sustained low FPS detected, triggering 2D fallback');
            onPerformanceDegraded?.();
          }
        } else {
          lowFpsCountRef.current = 0;
        }
      }
    };

    // ==========================================================================
    // 创建曲面几何体
    // ==========================================================================
    const createSurface = (yieldData: number[][], segments: number) => {
      // 清除旧几何体 (WebGL GC)
      if (meshRef.current) {
        scene.remove(meshRef.current);
        meshRef.current.geometry.dispose();
        if (Array.isArray(meshRef.current.material)) {
          meshRef.current.material.forEach((m) => m.dispose());
        } else {
          meshRef.current.material.dispose();
        }
        meshRef.current = null;
      }

      if (!yieldData || yieldData.length === 0) return;

      const timeSteps = yieldData.length;
      const termSteps = yieldData[0]?.length || 0;

      if (timeSteps < 2 || termSteps < 2) return;

      // 创建平面几何体
      const geometry = new THREE.PlaneGeometry(
        termSteps * 2,
        timeSteps * 2,
        segments,
        segments
      );

      // 更新顶点高度 (Z 轴 = 收益率)
      const positions = geometry.attributes.position.array;
      const colors = [];

      for (let i = 0; i < positions.length; i += 3) {
        const x = positions[i];     // term index
        const y = positions[i + 1]; // time index
        const termIdx = Math.round((x + termSteps) / 2);
        const timeIdx = Math.round((y + timeSteps) / 2);

        // 从数据中获取收益率值
        const safeTermIdx = Math.max(0, Math.min(termSteps - 1, termIdx));
        const safeTimeIdx = Math.max(0, Math.min(timeSteps - 1, timeIdx));
        const yieldValue = yieldData[safeTimeIdx]?.[safeTermIdx] || 0;

        // 设置 Z 轴高度 (收益率 * 缩放因子)
        positions[i + 2] = yieldValue * 10;

        // 颜色映射 (收益率高低 → 蓝到红渐变)
        const normalizedYield = Math.max(0, Math.min(1, (yieldValue - 0.02) / 0.04));
        const color = new THREE.Color();
        color.setHSL(0.6 - normalizedYield * 0.6, 1, 0.5); // 蓝→红
        colors.push(color.r, color.g, color.b);
      }

      geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
      geometry.computeVertexNormals();

      // 材质
      const material = new THREE.MeshStandardMaterial({
        vertexColors: true,
        side: THREE.DoubleSide,
        wireframe: false,
        metalness: 0.3,
        roughness: 0.7,
      });

      // 创建 Mesh
      const mesh = new THREE.Mesh(geometry, material);
      mesh.rotation.x = -Math.PI / 3; // 倾斜视角
      scene.add(mesh);
      meshRef.current = mesh;
    };

    // 初始渲染
    if (data) {
      createSurface(data, currentSegmentsRef.current);
    }

    // ==========================================================================
    // 动画循环
    // ==========================================================================
    const animate = () => {
      requestAnimationFrame(animate);

      if (controlsRef.current) {
        controlsRef.current.update();
      }

      if (rendererRef.current && sceneRef.current && cameraRef.current) {
        rendererRef.current.render(sceneRef.current, cameraRef.current);
      }

      updateFPS();
    };

    animate();

    // ==========================================================================
    // Resize handler
    // ==========================================================================
    const handleResize = () => {
      if (!containerRef.current || !cameraRef.current || !rendererRef.current) return;

      const width = containerRef.current.clientWidth;
      const height = containerRef.current.clientHeight;

      cameraRef.current.aspect = width / height;
      cameraRef.current.updateProjectionMatrix();
      rendererRef.current.setSize(width, height);
    };

    window.addEventListener('resize', handleResize);

    // ==========================================================================
    // Cleanup (WebGL GC)
    // ==========================================================================
    return () => {
      window.removeEventListener('resize', handleResize);

      if (controlsRef.current) {
        controlsRef.current.dispose();
      }

      if (meshRef.current) {
        scene.remove(meshRef.current);
        meshRef.current.geometry.dispose();
        if (Array.isArray(meshRef.current.material)) {
          meshRef.current.material.forEach((m) => m.dispose());
        } else {
          meshRef.current.material.dispose();
        }
        meshRef.current = null;
      }

      if (rendererRef.current) {
        rendererRef.current.dispose();
        rendererRef.current.forceContextLoss();
        rendererRef.current.domElement.remove();
        rendererRef.current = null;
      }

      sceneRef.current = null;
      cameraRef.current = null;
      controlsRef.current = null;
    };
  }, []);

  // 数据更新时重新渲染
  useEffect(() => {
    if (data && sceneRef.current) {
      // 使用当前 LOD 级别重新创建曲面
      createSurfaceForUpdate(data);
    }
  }, [data]);

  const createSurfaceForUpdate = (yieldData: number[][]) => {
    if (!sceneRef.current) return;

    // 清除旧几何体
    if (meshRef.current) {
      sceneRef.current.remove(meshRef.current);
      meshRef.current.geometry.dispose();
      if (Array.isArray(meshRef.current.material)) {
        meshRef.current.material.forEach((m) => m.dispose());
      } else {
        meshRef.current.material.dispose();
      }
      meshRef.current = null;
    }

    const segments = currentSegmentsRef.current;
    const timeSteps = yieldData.length;
    const termSteps = yieldData[0]?.length || 0;

    if (timeSteps < 2 || termSteps < 2) return;

    const geometry = new THREE.PlaneGeometry(termSteps * 2, timeSteps * 2, segments, segments);
    const positions = geometry.attributes.position.array;
    const colors = [];

    for (let i = 0; i < positions.length; i += 3) {
      const x = positions[i];
      const y = positions[i + 1];
      const termIdx = Math.round((x + termSteps) / 2);
      const timeIdx = Math.round((y + timeSteps) / 2);

      const safeTermIdx = Math.max(0, Math.min(termSteps - 1, termIdx));
      const safeTimeIdx = Math.max(0, Math.min(timeSteps - 1, timeIdx));
      const yieldValue = yieldData[safeTimeIdx]?.[safeTermIdx] || 0;

      positions[i + 2] = yieldValue * 10;

      const normalizedYield = Math.max(0, Math.min(1, (yieldValue - 0.02) / 0.04));
      const color = new THREE.Color();
      color.setHSL(0.6 - normalizedYield * 0.6, 1, 0.5);
      colors.push(color.r, color.g, color.b);
    }

    geometry.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
    geometry.computeVertexNormals();

    const material = new THREE.MeshStandardMaterial({
      vertexColors: true,
      side: THREE.DoubleSide,
      wireframe: false,
      metalness: 0.3,
      roughness: 0.7,
    });

    const mesh = new THREE.Mesh(geometry, material);
    mesh.rotation.x = -Math.PI / 3;
    sceneRef.current.add(mesh);
    meshRef.current = mesh;
  };

  return (
    <div ref={containerRef} className="w-full h-full relative">
      {/* FPS 指示器 */}
      <div className="absolute top-4 right-4 bg-slate-900/80 px-3 py-1 rounded text-xs font-mono text-slate-400">
        FPS: {fps}
      </div>

      {/* 形态标签 */}
      {data && data.length > 0 && (
        <div className="absolute top-4 left-4 bg-slate-900/80 px-4 py-2 rounded text-sm">
          <span className="text-slate-300">收益率曲面</span>
          <span className="ml-2 text-cyan-400">{dates[dates.length - 1]}</span>
        </div>
      )}
    </div>
  );
};
