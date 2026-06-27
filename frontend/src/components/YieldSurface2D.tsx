/**
 * 2D 收益率热力图 (WebGL 降级替代方案)
 * 架构原则:
 *   - 当设备性能不足 (PerformanceTier.LOW/MEDIUM) 时替代 3D 曲面
 *   - 使用 ECharts heatmap 渲染 (时间 × 期限 × 收益率色阶)
 *   - LOW 级设备进一步降级为多线条折线图
 */

import React, { useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts';
import { detectDeviceCapabilities, PerformanceTier } from '../utils/deviceDetect';

interface YieldSurface2DProps {
  data: number[][] | null; // [time_index][term_index] = yield_value
  terms?: string[];        // ['1M', '3M', ..., '30Y']
  dates?: string[];        // ['2024-01-01', ...]
}

export const YieldSurface2D: React.FC<YieldSurface2DProps> = ({
  data,
  terms = [],
  dates = [],
}) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstanceRef = useRef<echarts.ECharts | null>(null);
  const [tier] = useState(() => detectDeviceCapabilities().tier);

  useEffect(() => {
    if (!chartRef.current || !data || data.length === 0) return;

    const chart = echarts.init(chartRef.current);
    chartInstanceRef.current = chart;

    // LOW 级设备 → 多线条折线图 (最小 GPU 开销)
    if (tier === PerformanceTier.LOW) {
      renderLineChart(chart, data, terms, dates);
    } else {
      // MEDIUM 级设备 → 2D 热力图
      renderHeatmap(chart, data, terms, dates);
    }

    const handleResize = () => chart.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.dispose();
    };
  }, [data, terms, dates, tier]);

  return (
    <div className="w-full h-full relative">
      <div ref={chartRef} className="w-full h-full" />
      {/* 降级标识 */}
      <div className="absolute top-4 left-4 bg-slate-900/80 px-4 py-2 rounded text-sm flex items-center gap-2">
        <span className="text-amber-400">⚡</span>
        <span className="text-slate-300">
          {tier === PerformanceTier.LOW ? '折线图模式 (设备降级)' : '热力图模式'}
        </span>
        {dates.length > 0 && (
          <span className="ml-2 text-cyan-400">{dates[dates.length - 1]}</span>
        )}
      </div>
    </div>
  );
};

// ==========================================================================
// 2D 热力图渲染 (MEDIUM tier)
// ==========================================================================
function renderHeatmap(
  chart: echarts.ECharts,
  data: number[][],
  terms: string[],
  dates: string[]
) {
  const heatmapData: number[][] = [];

  for (let t = 0; t < data.length; t++) {
    for (let m = 0; m < (data[t]?.length || 0); m++) {
      const val = data[t][m];
      if (val != null && !isNaN(val)) {
        heatmapData.push([m, t, val]);
      }
    }
  }

  const option: echarts.EChartsOption = {
    backgroundColor: 'transparent',
    title: {
      text: '收益率曲面 (2D 热力图)',
      textStyle: { color: '#94a3b8', fontSize: 14 },
    },
    tooltip: {
      position: 'top',
      backgroundColor: 'rgba(15, 23, 42, 0.9)',
      borderColor: '#334155',
      textStyle: { color: '#e2e8f0' },
      formatter: (params: any) => {
        const termIdx = params.data[0];
        const timeIdx = params.data[1];
        const val = params.data[2];
        return `${dates[timeIdx] || ''}<br/>${terms[termIdx] || ''}: ${(val * 100).toFixed(2)}%`;
      },
    },
    grid: { top: '15%', left: '8%', right: '12%', bottom: '12%' },
    xAxis: {
      type: 'category',
      data: terms,
      splitArea: { show: true },
      axisLabel: { color: '#94a3b8' },
      axisLine: { lineStyle: { color: '#475569' } },
    },
    yAxis: {
      type: 'category',
      data: dates.length > 50
        ? dates.filter((_, i) => i % Math.ceil(dates.length / 20) === 0)
        : dates,
      splitArea: { show: true },
      axisLabel: { color: '#94a3b8', fontSize: 10 },
      axisLine: { lineStyle: { color: '#475569' } },
    },
    visualMap: {
      min: 0,
      max: 6,
      calculable: true,
      orient: 'vertical',
      right: 0,
      top: 'center',
      inRange: {
        color: ['#1e40af', '#3b82f6', '#06b6d4', '#fbbf24', '#f97316', '#ef4444'],
      },
      textStyle: { color: '#94a3b8' },
      text: ['高收益率', '低'],
    },
    series: [
      {
        name: '收益率',
        type: 'heatmap',
        data: heatmapData,
        label: { show: false },
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowColor: 'rgba(0, 0, 0, 0.5)',
          },
        },
      },
    ],
  };

  chart.setOption(option);
}

// ==========================================================================
// 多线条折线图渲染 (LOW tier — 最大降级)
// ==========================================================================
function renderLineChart(
  chart: echarts.ECharts,
  data: number[][],
  terms: string[],
  dates: string[]
) {
  // 为每个期限生成一条折线
  const series: echarts.SeriesOption[] = [];
  const colors = ['#3b82f6', '#06b6d4', '#10b981', '#fbbf24', '#f97316', '#ef4444', '#a855f7', '#ec4899'];

  // 采样: 最多取 8 个期限 + 时间维度抽样 (减少渲染点)
  const sampleTerms = terms.length <= 8 ? terms : terms.filter((_, i) => i % Math.ceil(terms.length / 6) === 0);
  const step = Math.max(1, Math.ceil(dates.length / 100)); // 最多 100 个时间点

  sampleTerms.forEach((term, termGlobalIdx) => {
    const lineData: (number | null)[] = [];
    for (let t = 0; t < data.length; t += step) {
      const val = data[t]?.[termGlobalIdx];
      lineData.push(val != null && !isNaN(val) ? val * 100 : null); // 转为百分比
    }

    series.push({
      name: term,
      type: 'line',
      data: lineData,
      smooth: true,
      symbol: 'none',
      lineStyle: { width: 1.5, color: colors[termGlobalIdx % colors.length] },
      itemStyle: { color: colors[termGlobalIdx % colors.length] },
    });
  });

  const sampledDates = dates.filter((_, i) => i % step === 0);

  const option: echarts.EChartsOption = {
    backgroundColor: 'transparent',
    title: {
      text: '收益率曲线族 (折线图)',
      textStyle: { color: '#94a3b8', fontSize: 14 },
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(15, 23, 42, 0.9)',
      borderColor: '#334155',
      textStyle: { color: '#e2e8f0', fontSize: 11 },
    },
    legend: {
      data: sampleTerms,
      textStyle: { color: '#94a3b8', fontSize: 10 },
      top: 30,
    },
    grid: { top: '20%', left: '5%', right: '5%', bottom: '10%', containLabel: true },
    xAxis: {
      type: 'category',
      data: sampledDates,
      axisLabel: { color: '#94a3b8', fontSize: 9, rotate: 30 },
      axisLine: { lineStyle: { color: '#475569' } },
    },
    yAxis: {
      type: 'value',
      name: '收益率 (%)',
      nameTextStyle: { color: '#94a3b8' },
      axisLabel: { color: '#94a3b8' },
      splitLine: { lineStyle: { color: '#1e293b' } },
    },
    series,
  };

  chart.setOption(option);
}
