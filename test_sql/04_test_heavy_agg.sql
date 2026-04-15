-- ============================================================
-- 反面用例: 全表扫描 + 多列聚合 (不应被缓存 -> BYPASS)
-- ============================================================
-- 表: test.t2  (1 TAG device_id + 50 FIELD s01~s50)
-- 前置条件: 需要先导入大量数据 (100K 行)
--   python generate_test_data.py -> 生成 insert_heavy_100k.sql
--   然后在 CLI 中 source insert_heavy_100k.sql
-- 特征: 全表扫描 100K 行 x 50 列, 无 GROUP BY, 只输出 1 行
--   执行时间远大于规划时间 (IO/计算密集)
--   -> benefitRatio = planCost / firstRpcCost 应该 < 10%%
--   -> 预期: MONITOR -> BYPASS
-- ============================================================

USE test;

-- === 第 1~5 次: 观察期 (MONITOR, sampleCount 积累) ===
-- 每次变换 time 字面量, 验证参数化归一化后命中同一模板
-- 无 GROUP BY -> 输出只有 1 行, 但扫描全部 100K 行

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000000;

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000001;

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000002;

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000003;

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000004;


-- === 第 6~10 次: 若收益低, 应已降级为 BYPASS ===

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000005;

SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000006;


-- === EXPLAIN ANALYZE: 检查 Plan Cache 状态 ===
-- 预期看到 BYPASS (Low_Benefit): 因为执行耗时远大于规划耗时

EXPLAIN ANALYZE VERBOSE SELECT avg(s01), avg(s02), avg(s03), avg(s04), avg(s05), avg(s06), avg(s07), avg(s08), avg(s09), avg(s10), avg(s11), avg(s12), avg(s13), avg(s14), avg(s15), avg(s16), avg(s17), avg(s18), avg(s19), avg(s20), avg(s21), avg(s22), avg(s23), avg(s24), avg(s25), avg(s26), avg(s27), avg(s28), avg(s29), avg(s30), avg(s31), avg(s32), avg(s33), avg(s34), avg(s35), avg(s36), avg(s37), avg(s38), avg(s39), avg(s40), avg(s41), avg(s42), avg(s43), avg(s44), avg(s45), avg(s46), avg(s47), avg(s48), avg(s49), avg(s50) FROM t2 WHERE time >= 1700000000007;
