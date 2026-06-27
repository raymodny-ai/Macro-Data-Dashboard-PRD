-- =============================================================================
-- 宏观流动性与资产定价状态识别系统 - TimescaleDB 初始化脚本
-- 架构原则: UPSERT 无锁覆写 (ON CONFLICT DO UPDATE)
-- =============================================================================

-- 启用 TimescaleDB 扩展
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =============================================================================
-- 1. inflation_data: 通胀二阶导组
-- =============================================================================
CREATE TABLE IF NOT EXISTS inflation_data (
    record_date     DATE            NOT NULL,
    symbol          VARCHAR(32)     NOT NULL,
    -- symbol 示例: 'CPILFESL', 'CES0500000003', 'CES3000000008', etc.
    value           NUMERIC(16,6),
    mom_growth      NUMERIC(12,8),  -- 环比增速 (MoM)
    acceleration    NUMERIC(12,8),  -- 二阶加速度
    three_mma       NUMERIC(12,8),  -- 三个月移动平均
    warning_flag    BOOLEAN         DEFAULT FALSE,  -- "薪柴复燃"预警
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_inflation UNIQUE (record_date, symbol)
);

SELECT create_hypertable('inflation_data', 'record_date', if_not_exists => TRUE);

-- =============================================================================
-- 2. fiscal_auction_data: 财政增量组
-- =============================================================================
CREATE TABLE IF NOT EXISTS fiscal_auction_data (
    auction_date        DATE            NOT NULL,
    security_type       VARCHAR(16)     NOT NULL,
    -- security_type: '10Y', '30Y'
    bid_to_cover_ratio  NUMERIC(8,4),
    high_yield          NUMERIC(8,6),
    expected_yield      NUMERIC(8,6),
    tail_spread         NUMERIC(8,6),   -- high_yield - expected_yield
    acm_term_premium    NUMERIC(8,6),   -- ACM 期限溢价 (THREEFYTP10)
    fiscal_warning_flag BOOLEAN         DEFAULT FALSE,
    updated_at          TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_fiscal UNIQUE (auction_date, security_type)
);

SELECT create_hypertable('fiscal_auction_data', 'auction_date', if_not_exists => TRUE);

-- =============================================================================
-- 3. liquidity_corridor: 流动性走廊组
-- =============================================================================
CREATE TABLE IF NOT EXISTS liquidity_corridor (
    record_date     DATE            NOT NULL,
    symbol          VARCHAR(16)     NOT NULL DEFAULT 'SPREAD',
    -- symbol: 'SOFR', 'IORB', 'SPREAD'
    sofr_rate       NUMERIC(8,6),
    iorb_rate       NUMERIC(8,6),
    spread          NUMERIC(8,6),   -- SOFR - IORB
    system_state    SMALLINT,       -- 0: 充裕, 1: 紧张, 2: 瘫痪
    crisis_alert    BOOLEAN         DEFAULT FALSE,  -- "水管爆裂"预警
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_liquidity UNIQUE (record_date, symbol)
);

SELECT create_hypertable('liquidity_corridor', 'record_date', if_not_exists => TRUE);

-- =============================================================================
-- 4. ai_capex_data: AI 资本开支组
-- =============================================================================
CREATE TABLE IF NOT EXISTS ai_capex_data (
    report_date     DATE            NOT NULL,
    company_cik     VARCHAR(16)     NOT NULL,
    -- company_cik: '320193' (AAPL), '1045810' (NVDA), '789019' (MSFT), etc.
    company_name    VARCHAR(64),
    capex           NUMERIC(18,2),  -- 资本支出 (绝对值)
    rd_expense      NUMERIC(18,2),  -- 研发费用
    capex_mom       NUMERIC(12,6),  -- CapEx 环比增速
    capex_yoy       NUMERIC(12,6),  -- CapEx 同比增速
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_ai_capex UNIQUE (report_date, company_cik)
);

SELECT create_hypertable('ai_capex_data', 'report_date', if_not_exists => TRUE);

-- =============================================================================
-- 5. market_contagion: 市场传染组
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_contagion (
    trade_date          DATE            NOT NULL,
    symbol              VARCHAR(16)     NOT NULL,
    -- symbol: 'SPY', 'TLT', 'MOVE', 'CORR_30D', 'CORR_60D'
    close_price         NUMERIC(12,4),
    log_return          NUMERIC(12,8),  -- 对数收益率 ln(P_t / P_{t-1})
    move_index          NUMERIC(10,4),  -- ICE BofAML MOVE Index
    rolling_corr_30d    NUMERIC(10,8),  -- 30日滚动皮尔逊相关系数
    rolling_corr_60d    NUMERIC(10,8),  -- 60日滚动皮尔逊相关系数
    contagion_alert     BOOLEAN         DEFAULT FALSE,
    updated_at          TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_contagion UNIQUE (trade_date, symbol)
);

SELECT create_hypertable('market_contagion', 'trade_date', if_not_exists => TRUE);

-- =============================================================================
-- 索引优化
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_inflation_date ON inflation_data (record_date DESC);
CREATE INDEX IF NOT EXISTS idx_fiscal_date ON fiscal_auction_data (auction_date DESC);
CREATE INDEX IF NOT EXISTS idx_liquidity_date ON liquidity_corridor (record_date DESC);
CREATE INDEX IF NOT EXISTS idx_liquidity_state ON liquidity_corridor (system_state, record_date DESC);
CREATE INDEX IF NOT EXISTS idx_ai_capex_date ON ai_capex_data (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_contagion_date ON market_contagion (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_contagion_alert ON market_contagion (contagion_alert, trade_date DESC);

-- =============================================================================
-- 连续聚合视图 (按需刷新, 由 Airflow DAG 完成后触发 refresh_continuous_aggregate)
-- =============================================================================

-- SOFR-IORB 利差 30日/90日历史分位数
CREATE MATERIALIZED VIEW IF NOT EXISTS spread_percentiles_30d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', record_date) AS bucket,
    percentile_agg(spread, 0.30) AS spread_p30_30d,
    percentile_agg(spread, 0.50) AS spread_p50_30d,
    percentile_agg(spread, 0.70) AS spread_p70_30d,
    percentile_agg(spread, 0.90) AS spread_p90_30d
FROM liquidity_corridor
WHERE symbol = 'SPREAD'
GROUP BY bucket
WITH NO DATA;

-- =============================================================================
-- 数据库维护日志表 (VACUUM ANALYZE DAG 写入)
-- =============================================================================
CREATE TABLE IF NOT EXISTS db_maintenance_log (
    id              SERIAL          PRIMARY KEY,
    operation       VARCHAR(32)     NOT NULL,
    -- operation: 'VACUUM_ANALYZE', 'BACKUP', 'WAL_ARCHIVE'
    table_name      VARCHAR(64),
    status          VARCHAR(128)    NOT NULL,
    executed_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    details         TEXT,
    duration_ms     INTEGER
);

CREATE INDEX IF NOT EXISTS idx_maintenance_log_date
    ON db_maintenance_log (executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_maintenance_log_op
    ON db_maintenance_log (operation, executed_at DESC);

-- =============================================================================
-- 备份策略配置表
-- =============================================================================
CREATE TABLE IF NOT EXISTS backup_schedule (
    id              SERIAL          PRIMARY KEY,
    schedule_name   VARCHAR(32)     NOT NULL UNIQUE,
    -- schedule_name: 'daily_full', 'wal_continuous'
    cron_expr       VARCHAR(64)     NOT NULL,
    -- cron_expr: '0 2 * * *' (每日 UTC 02:00)
    retention_days  INTEGER         NOT NULL DEFAULT 7,
    backup_method   VARCHAR(32)     NOT NULL DEFAULT 'pg_basebackup',
    -- backup_method: 'pg_basebackup', 'pg_dump', 'wal_archive'
    enabled         BOOLEAN         NOT NULL DEFAULT TRUE,
    last_run        TIMESTAMPTZ,
    next_run        TIMESTAMPTZ,
    config_json     JSONB,
    -- config_json: 备份额外参数 (如压缩级别, 并行度等)
    updated_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- 插入默认备份策略
INSERT INTO backup_schedule (schedule_name, cron_expr, retention_days, backup_method, config_json)
VALUES
    ('daily_full', '0 2 * * *', 7, 'pg_basebackup', '{"compress": true, "parallel": 2}'::jsonb),
    ('wal_continuous', '* * * * *', 7, 'wal_archive', '{"archive_mode": "on"}'::jsonb)
ON CONFLICT (schedule_name) DO NOTHING;

-- =============================================================================
-- 规则引擎配置表 (热更新, B4 实现)
-- 架构原则: 判定阈值抽离为独立表, 支持运行时热更新
-- =============================================================================
CREATE TABLE IF NOT EXISTS rules_config (
    rule_name       VARCHAR(64)     PRIMARY KEY,
    rule_value      NUMERIC(16,8)   NOT NULL,
    description     TEXT,
    rule_group      VARCHAR(32),    -- liquidity / fiscal / dual_track / contagion / quality
    version         INTEGER         NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- 国债收益率曲线表 (B6 3D 曲面数据源)
-- =============================================================================
CREATE TABLE IF NOT EXISTS treasury_yields (
    record_date     DATE            NOT NULL,
    symbol          VARCHAR(16)     NOT NULL,
    -- symbol: 'DGS1MO', 'DGS3MO', 'DGS6MO', 'DGS1', 'DGS2', 'DGS5', 'DGS10', 'DGS30'
    value           NUMERIC(8,6),   -- 收益率 (%)
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    CONSTRAINT uq_treasury_yield UNIQUE (record_date, symbol)
);

SELECT create_hypertable('treasury_yields', 'record_date', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_treasury_yield_date ON treasury_yields (record_date DESC);
CREATE INDEX IF NOT EXISTS idx_treasury_yield_symbol ON treasury_yields (symbol, record_date DESC);

-- =============================================================================
-- Airflow 元数据表空间 (使用独立 schema 隔离)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS airflow;
