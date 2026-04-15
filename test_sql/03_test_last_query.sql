-- ============================================================
-- 正面用例: LAST 点查 (应被缓存 → ACTIVE)
-- ============================================================
-- 表: test.t2  (1 TAG device_id + 50 FIELD s01~s50, 1000 行)
-- 特征: 指定 device_id 点查最新值, 只返回一行
--   执行时间极短 (扫描量小), 但 FE 规划相对较重
--   (50 列的 Schema 绑定 + 优化器链路)
--   → benefitRatio = planCost / firstRpcCost 应该 >= 20%
--   → 预期: MONITOR → ACTIVE
-- ============================================================

USE test;

-- === 第 1~5 次: 观察期 (MONITOR, sampleCount 积累) ===
-- 每次变换 device_id 字面量, 验证参数化归一化后命中同一模板

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd1';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd2';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd3';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd4';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd5';

-- === 第 6~10 次: 此时应已晋升为 ACTIVE, 后续查询走缓存 ===

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd6';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd7';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd8';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd9';

SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd10';

-- === EXPLAIN ANALYZE: 检查 Plan Cache 状态 ===
-- 如果模板已进入 ACTIVE, 应该能看到 HIT + Saved Cost

EXPLAIN ANALYZE VERBOSE SELECT last(s01), last(s02), last(s03), last(s04), last(s05), last(s06), last(s07), last(s08), last(s09), last(s10) FROM t2 WHERE device_id = 'd11';
