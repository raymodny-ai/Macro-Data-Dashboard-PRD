"""
数据库备份工具
架构原则: 自动备份策略 + WAL 归档配置

备份策略:
  1. pg_basebackup: 物理全量备份 (每日凌晨 02:00 UTC)
  2. WAL 归档: 连续归档 (实时)
  3. pg_dump: 逻辑备份 (可选, 用于跨版本迁移)

使用方式 (在 TimescaleDB 容器内或宿主机):
  docker exec timescaledb pg_basebackup -U dashboard -D /backups/$(date +%Y%m%d) -Ft -z -P

也可通过 Airflow DAG 调度 (B7 批次完善自动化备份 DAG)
"""
import os
import subprocess
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# 备份配置 (从环境变量或 backup_schedule 表读取)
BACKUP_DIR = os.getenv('BACKUP_DIR', '/backups')
RETENTION_DAYS = int(os.getenv('BACKUP_RETENTION_DAYS', '7'))
PG_HOST = os.getenv('POSTGRES_HOST', 'timescaledb')
PG_PORT = int(os.getenv('POSTGRES_PORT', 5432))
PG_USER = os.getenv('POSTGRES_USER', 'dashboard')
PG_DB = os.getenv('POSTGRES_DB', 'macro_dashboard')


def run_pg_basebackup(
    backup_label: Optional[str] = None,
    compress: bool = True,
    parallel: int = 2,
) -> dict:
    """
    执行 pg_basebackup 物理全量备份

    Args:
        backup_label: 备份标签 (默认使用日期)
        compress: 是否启用 gzip 压缩
        parallel: 并行度

    Returns:
        {'status': 'success'/'error', 'path': str, 'duration_ms': int}
    """
    if backup_label is None:
        backup_label = datetime.now().strftime('%Y%m%d_%H%M%S')

    backup_path = os.path.join(BACKUP_DIR, backup_label)
    os.makedirs(backup_path, exist_ok=True)

    cmd = [
        'pg_basebackup',
        '-h', PG_HOST,
        '-p', str(PG_PORT),
        '-U', PG_USER,
        '-D', backup_path,
        '-Ft',   # tar 格式
        '-P',    # 显示进度
        '-j', str(parallel),
    ]
    if compress:
        cmd.append('-z')

    logger.info(f"Starting pg_basebackup: {backup_label} -> {backup_path}")
    start_time = datetime.now()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1小时超时
        )
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        if result.returncode == 0:
            logger.info(f"Backup completed: {backup_path} ({duration_ms}ms)")
            return {
                'status': 'success',
                'path': backup_path,
                'label': backup_label,
                'duration_ms': duration_ms,
            }
        else:
            logger.error(f"Backup failed: {result.stderr}")
            return {
                'status': f'error: {result.stderr[:200]}',
                'path': backup_path,
                'duration_ms': duration_ms,
            }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error: timeout (3600s)',
            'path': backup_path,
            'duration_ms': 3600000,
        }
    except FileNotFoundError:
        return {
            'status': 'error: pg_basebackup not found (not in container?)',
            'path': backup_path,
            'duration_ms': 0,
        }


def run_pg_dump(
    backup_label: Optional[str] = None,
    format: str = 'custom',
) -> dict:
    """
    执行 pg_dump 逻辑备份

    Args:
        backup_label: 备份标签
        format: 输出格式 ('custom', 'directory', 'plain')

    Returns:
        {'status': str, 'path': str, 'duration_ms': int}
    """
    if backup_label is None:
        backup_label = datetime.now().strftime('%Y%m%d_%H%M%S')

    format_flag = {'custom': '-Fc', 'directory': '-Fd', 'plain': '-Fp'}.get(format, '-Fc')
    ext = {'custom': '.dump', 'directory': '', 'plain': '.sql'}.get(format, '.dump')
    backup_path = os.path.join(BACKUP_DIR, f"{backup_label}{ext}")

    os.makedirs(os.path.dirname(backup_path), exist_ok=True)

    cmd = [
        'pg_dump',
        '-h', PG_HOST,
        '-p', str(PG_PORT),
        '-U', PG_USER,
        '-d', PG_DB,
        format_flag,
        '-f', backup_path,
    ]

    logger.info(f"Starting pg_dump: {backup_label} -> {backup_path}")
    start_time = datetime.now()

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        if result.returncode == 0:
            return {'status': 'success', 'path': backup_path, 'duration_ms': duration_ms}
        else:
            return {'status': f'error: {result.stderr[:200]}', 'path': backup_path, 'duration_ms': duration_ms}
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return {'status': f'error: {e}', 'path': backup_path, 'duration_ms': 0}


def cleanup_old_backups(retention_days: Optional[int] = None) -> int:
    """
    清理过期备份文件

    Args:
        retention_days: 保留天数 (默认从环境变量读取)

    Returns:
        清理的备份目录数量
    """
    if retention_days is None:
        retention_days = RETENTION_DAYS

    cutoff = datetime.now().timestamp() - (retention_days * 86400)
    removed = 0

    if not os.path.exists(BACKUP_DIR):
        return 0

    for entry in os.listdir(BACKUP_DIR):
        entry_path = os.path.join(BACKUP_DIR, entry)
        try:
            mtime = os.path.getmtime(entry_path)
            if mtime < cutoff:
                if os.path.isdir(entry_path):
                    import shutil
                    shutil.rmtree(entry_path)
                else:
                    os.remove(entry_path)
                removed += 1
                logger.info(f"Removed expired backup: {entry_path}")
        except OSError as e:
            logger.warning(f"Failed to remove {entry_path}: {e}")

    logger.info(f"Cleanup: removed {removed} expired backups (retention: {retention_days} days)")
    return removed
