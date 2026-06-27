/**
 * ECharts 图表集成组件
 * - SOFR-IORB 利差趋势图
 * - 宏观紧缩指数仪表盘
 * - 市场传染警报热力图
 */

import React, { useEffect, useRef } from 'react';
import * as echarts from 'echarts';

// ==========================================================================
// SOFR-IORB 利差趋势图
// ==========================================================================
export const SpreadChart: React.FC<{
  data: Array<{ date: string; spread: number | null; system_state?: number }>;
}> = ({ data }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstanceRef = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!chartRef.current) return;

    const chart = echarts.init(chartRef.current);
    chartInstanceRef.current = chart;

    const option: echarts.EChartsOption = {
      backgroundColor: 'transparent',
      title: {
        text: 'SOFR-IORB 利差走势',
        textStyle: { color: '#94a3b8', fontSize: 14 },
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(15, 23, 42, 0.9)',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0' },
      },
      grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
      xAxis: {
        type: 'category',
        data: data.map((d) => d.date),
        axisLine: { lineStyle: { color: '#475569' } },
        axisLabel: { color: '#94a3b8' },
      },
      yAxis: {
        type: 'value',
        axisLine: { lineStyle: { color: '#475569' } },
        axisLabel: { color: '#94a3b8' },
        splitLine: { lineStyle: { color: '#1e293b' } },
      },
      series: [
        {
          name: '利差',
          type: 'line',
          data: data.map((d) => d.spread),
          smooth: true,
          lineStyle: { color: '#06b6d4', width: 2 },
          itemStyle: { color: '#06b6d4' },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(6, 182, 212, 0.3)' },
              { offset: 1, color: 'rgba(6, 182, 212, 0.05)' },
            ]),
          },
          markLine: {
            silent: true,
            data: [
              { yAxis: -0.03, label: { formatter: '充裕', color: '#10b981' }, lineStyle: { color: '#10b981', type: 'dashed' } },
              { yAxis: 0, label: { formatter: '紧张', color: '#f59e0b' }, lineStyle: { color: '#f59e0b', type: 'dashed' } },
            ],
          },
        },
      ],
    };

    chart.setOption(option);

    const handleResize = () => chart.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.dispose();
    };
  }, [data]);

  return <div ref={chartRef} className="w-full h-64" />;
};

// ==========================================================================
// 宏观紧缩指数仪表盘
// ==========================================================================
export const MacroIndexGauge: React.FC<{ value: number }> = ({ value }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstanceRef = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!chartRef.current) return;

    const chart = echarts.init(chartRef.current);
    chartInstanceRef.current = chart;

    const getColor = (val: number) => {
      if (val > 75) return '#ef4444'; // red
      if (val > 50) return '#f59e0b'; // yellow
      return '#10b981'; // green
    };

    const option: echarts.EChartsOption = {
      backgroundColor: 'transparent',
      series: [
        {
          type: 'gauge',
          min: 0,
          max: 100,
          startAngle: 200,
          endAngle: -20,
          radius: '90%',
          axisLine: {
            lineStyle: {
              width: 10,
              color: [
                [0.4, '#10b981'],
                [0.75, '#f59e0b'],
                [1, '#ef4444'],
              ],
            },
          },
          pointer: {
            itemStyle: { color: getColor(value) },
            length: '70%',
            width: 4,
          },
          axisTick: { distance: -10, length: 6, lineStyle: { color: '#fff', width: 1 } },
          splitLine: { distance: -10, length: 12, lineStyle: { color: '#fff', width: 2 } },
          axisLabel: { color: '#94a3b8', distance: 15, fontSize: 10 },
          detail: {
            valueAnimation: true,
            fontSize: 24,
            fontWeight: 'bold',
            color: getColor(value),
            offsetCenter: [0, '30%'],
            formatter: '{value}',
          },
          data: [{ value: Math.round(value) }],
        },
      ],
    };

    chart.setOption(option);

    const handleResize = () => chart.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.dispose();
    };
  }, [value]);

  return <div ref={chartRef} className="w-full h-48" />;
};

// ==========================================================================
// 市场传染警报热力图
// ==========================================================================
export const ContagionHeatmap: React.FC<{
  data: Array<{ date: string; corr_30d: number | null; alert: boolean }>;
}> = ({ data }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstanceRef = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!chartRef.current || data.length === 0) return;

    const chart = echarts.init(chartRef.current);
    chartInstanceRef.current = chart;

    const heatmapData = data.map((d, i) => [i, 0, d.corr_30d || 0]);
    const dates = data.map((d) => d.date);

    const option: echarts.EChartsOption = {
      backgroundColor: 'transparent',
      title: {
        text: 'SPY-TLT 滚动相关系数',
        textStyle: { color: '#94a3b8', fontSize: 14 },
      },
      tooltip: {
        position: 'top',
        backgroundColor: 'rgba(15, 23, 42, 0.9)',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0' },
        formatter: (params: any) => {
          const idx = params.data[0];
          const alert = data[idx]?.alert ? ' ⚠️ 警报' : '';
          return `${dates[idx]}: ${params.data[2].toFixed(3)}${alert}`;
        },
      },
      grid: { height: '70%', top: '15%' },
      xAxis: {
        type: 'category',
        data: dates,
        splitArea: { show: true },
        axisLabel: { color: '#94a3b8', rotate: 45 },
      },
      yAxis: {
        type: 'category',
        data: ['相关系数'],
        splitArea: { show: true },
        axisLabel: { color: '#94a3b8' },
      },
      visualMap: {
        min: -1,
        max: 1,
        calculable: true,
        orient: 'horizontal',
        left: 'center',
        bottom: '5%',
        inRange: {
          color: ['#3b82f6', '#fbbf24', '#ef4444'],
        },
        textStyle: { color: '#94a3b8' },
      },
      series: [
        {
          name: '相关系数',
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

    const handleResize = () => chart.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.dispose();
    };
  }, [data]);

  return <div ref={chartRef} className="w-full h-48" />;
};
