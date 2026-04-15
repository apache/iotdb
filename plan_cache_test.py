"""
Plan Cache 功能测试 (IoTDB 表模型)
==================================
该脚本通过 IoTDB Session 接口：
  1. 建库建表 (2 TAG + 80 FIELD)
  2. 插入 5000 行数据 (50 device × 100 timestamps)
  3. 正面用例: LAST 点查 —— 规划重、执行轻，应被缓存 (ACTIVE)
  4. 反面用例: 全表扫描聚合 —— 执行重、规划轻，应旁路 (BYPASS)
  5. 用 EXPLAIN ANALYZE 观察 Plan Cache 状态变化并保存结果

依赖: pip install apache-iotdb
"""

import time
import random
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.table_session import TableSession, TableSessionConfig

# ---------- 配置 ----------
HOST = "127.0.0.1"
PORT = "6667"
USER = "root"
PASSWORD = "root"
DATABASE = "test_plan_cache"
TABLE = "sensor_data"
NUM_DEVICES = 50
NUM_TIMESTAMPS = 100  # per device → 50 × 100 = 5000 rows
NUM_FIELDS = 80
REGIONS = ["east", "west", "north", "south", "central"]
# ---------------------------

results_log: list[str] = []


def log(msg: str):
    print(msg)
    results_log.append(msg)


def execute_and_log(session, sql: str, label: str = ""):
    """Execute a SQL statement and log the result."""
    start = time.perf_counter_ns()
    ds = session.execute_query_statement(sql)
    elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
    rows = []
    if ds:
        col_names = ds.getColumnNames()
        while ds.hasNext():
            row = ds.next()
            fields = [str(row.getFields()[i]) for i in range(len(col_names))]
            rows.append(dict(zip(col_names, fields)))
        ds.closeOperationHandle()
    tag = f"[{label}] " if label else ""
    log(f"{tag}SQL took {elapsed_ms:.2f} ms, returned {len(rows)} rows")
    return rows, elapsed_ms


def execute_non_query(session, sql: str):
    session.execute_non_query_statement(sql)


def setup_schema(session):
    """Create database and table."""
    log("=" * 60)
    log("STEP 1: 创建数据库和表")
    log("=" * 60)
    execute_non_query(session, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    execute_non_query(session, f"USE {DATABASE}")

    field_defs = ", ".join([f"f_{i:02d} DOUBLE FIELD" for i in range(NUM_FIELDS)])
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        f"  region STRING TAG,"
        f"  device_id STRING TAG,"
        f"  {field_defs}"
        f")"
    )
    execute_non_query(session, ddl)
    log(f"  表 {TABLE} 已创建: 2 TAG + {NUM_FIELDS} FIELD 列")


def insert_data(session):
    """Insert 5000 rows: 50 devices × 100 timestamps."""
    log("")
    log("=" * 60)
    log("STEP 2: 插入数据")
    log("=" * 60)

    base_ts = 1700000000000  # 2023-11-14 approx 毫秒时间戳
    total_inserted = 0
    start = time.perf_counter_ns()

    for d in range(NUM_DEVICES):
        device_id = f"d_{d:03d}"
        region = REGIONS[d % len(REGIONS)]

        # 每个 device 分批插 100 行
        col_names = ["time", "region", "device_id"] + [f"f_{i:02d}" for i in range(NUM_FIELDS)]
        col_list = ", ".join(col_names)

        batch_values = []
        for t in range(NUM_TIMESTAMPS):
            ts = base_ts + t * 1000  # 每秒一个点
            vals = [str(round(random.uniform(-100, 100), 4)) for _ in range(NUM_FIELDS)]
            row_str = f"({ts}, '{region}', '{device_id}', {', '.join(vals)})"
            batch_values.append(row_str)

        # 一个 device 一次性插入
        sql = f"INSERT INTO {TABLE} ({col_list}) VALUES {', '.join(batch_values)}"
        execute_non_query(session, sql)
        total_inserted += NUM_TIMESTAMPS

        if (d + 1) % 10 == 0:
            log(f"  已插入 {total_inserted} / {NUM_DEVICES * NUM_TIMESTAMPS} 行 ...")

    elapsed_s = (time.perf_counter_ns() - start) / 1_000_000_000
    log(f"  总计插入 {total_inserted} 行, 耗时 {elapsed_s:.2f}s")


def test_last_query(session, iterations=10):
    """
    正面用例: LAST 点查
    SELECT device_id, last(f_00), last(f_01), ..., last(f_09)
    FROM sensor_data WHERE device_id = 'd_001'
    
    特征: 只查一个 device 最新一行, 执行极快, 但 FE 规划(80列表结构 + 优化器) 相对较重。
    预期: benefitRatio 高 → MONITOR → ACTIVE
    """
    log("")
    log("=" * 60)
    log("STEP 3: 正面用例 — LAST 点查 (应被缓存)")
    log("=" * 60)

    last_cols = ", ".join([f"last(f_{i:02d})" for i in range(10)])
    base_sql = (
        f"SELECT device_id, {last_cols} "
        f"FROM {TABLE} WHERE device_id = '{{dev}}'"
    )

    timings = []
    for i in range(iterations):
        # 使用不同字面量以测试参数化命中
        dev = f"d_{(i % 5):03d}"
        sql = base_sql.format(dev=dev)
        _, elapsed = execute_and_log(session, sql, f"LAST #{i+1}")
        timings.append(elapsed)

    avg = sum(timings) / len(timings)
    log(f"\n  LAST 查询平均耗时: {avg:.2f} ms (共 {iterations} 次)")
    log(f"  最小 / 最大: {min(timings):.2f} / {max(timings):.2f} ms")

    # 用 EXPLAIN ANALYZE 观察 Plan Cache 状态
    log("\n  --- EXPLAIN ANALYZE (LAST 查询) ---")
    explain_sql = (
        f"EXPLAIN ANALYZE VERBOSE "
        f"SELECT device_id, {last_cols} "
        f"FROM {TABLE} WHERE device_id = 'd_001'"
    )
    rows, _ = execute_and_log(session, explain_sql, "EXPLAIN-LAST")
    for row in rows:
        line = list(row.values())[0] if row else ""
        if any(kw in line.lower() for kw in ["cache", "plan", "saved", "lookup", "cost", "status"]):
            log(f"    {line}")


def test_heavy_aggregation(session, iterations=10):
    """
    反面用例: 全表扫描聚合
    SELECT region, avg(f_00), avg(f_01), ..., avg(f_79)
    FROM sensor_data GROUP BY region
    
    特征: 扫描全部 5000 行 × 80 列, 执行时间远大于规划时间。
    预期: benefitRatio 低 → MONITOR → BYPASS
    """
    log("")
    log("=" * 60)
    log("STEP 4: 反面用例 — 全表聚合 (不应被缓存)")
    log("=" * 60)

    agg_cols = ", ".join([f"avg(f_{i:02d})" for i in range(NUM_FIELDS)])
    base_sql = (
        f"SELECT region, {agg_cols} "
        f"FROM {TABLE} WHERE time >= {{ts}} GROUP BY region"
    )

    timings = []
    base_ts = 1700000000000
    for i in range(iterations):
        # 每次稍微变动 time 字面量以命中同一模板
        ts = base_ts + i
        sql = base_sql.format(ts=ts)
        _, elapsed = execute_and_log(session, sql, f"AGG #{i+1}")
        timings.append(elapsed)

    avg = sum(timings) / len(timings)
    log(f"\n  全表聚合平均耗时: {avg:.2f} ms (共 {iterations} 次)")
    log(f"  最小 / 最大: {min(timings):.2f} / {max(timings):.2f} ms")

    # EXPLAIN ANALYZE
    log("\n  --- EXPLAIN ANALYZE (全表聚合) ---")
    explain_sql = (
        f"EXPLAIN ANALYZE VERBOSE "
        f"SELECT region, {agg_cols} "
        f"FROM {TABLE} WHERE time >= {base_ts} GROUP BY region"
    )
    rows, _ = execute_and_log(session, explain_sql, "EXPLAIN-AGG")
    for row in rows:
        line = list(row.values())[0] if row else ""
        if any(kw in line.lower() for kw in ["cache", "plan", "saved", "lookup", "cost", "status"]):
            log(f"    {line}")


def save_results():
    output_path = r"d:\ly\iotdb\plan_cache_test_results.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(results_log))
    print(f"\n结果已保存到: {output_path}")


def main():
    log("Plan Cache 功能测试")
    log(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"配置: {NUM_DEVICES} devices × {NUM_TIMESTAMPS} ts = "
        f"{NUM_DEVICES * NUM_TIMESTAMPS} rows, {NUM_FIELDS} fields")
    log("")

    config = (
        TableSessionConfig.builder()
        .node_urls(["127.0.0.1:6667"])
        .username(USER)
        .password(PASSWORD)
        .database(DATABASE)
        .build()
    )

    with TableSession(config) as session:
        # 建表
        setup_schema(session)

        # 插入数据
        insert_data(session)

        # 测试 LAST 查询 (正面: 应缓存)
        test_last_query(session, iterations=10)

        # 测试全表聚合 (反面: 不应缓存)
        test_heavy_aggregation(session, iterations=10)

    log("")
    log("=" * 60)
    log("测试完成")
    log("=" * 60)

    save_results()


if __name__ == "__main__":
    main()
