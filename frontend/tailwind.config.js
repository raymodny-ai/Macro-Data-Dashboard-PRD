/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // 暗色调主题: 深蓝/黑色背景
        'dashboard-bg': '#0a0e17',
        'dashboard-panel': '#111827',
        'dashboard-border': '#1f2937',
        // 霓虹蓝/红色警示色
        'neon-blue': '#00d4ff',
        'neon-green': '#00ff88',
        'neon-yellow': '#ffcc00',
        'neon-red': '#ff3366',
        'neon-purple': '#a855f7',
      },
      fontFamily: {
        'mono': ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
    },
  },
  plugins: [],
}
