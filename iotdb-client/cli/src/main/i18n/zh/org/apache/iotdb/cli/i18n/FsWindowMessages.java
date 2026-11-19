/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.cli.i18n;

public final class FsWindowMessages {
  private FsWindowMessages() {}
  public static final String EXCEPTION_INVALID_WINDOW_GRID_REQUIRE_INCLUSIVE_START_END_ORIGIN_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_POSITIVE_WIDTH_STEP_UNIT_MS_DAY_MONTH_AND_TIMEZONE_LOCAL_BOUNDARIES_MUST_BE_UNAMBIGUOUS_AND_EXISTENT_AT_MOST_256_STARTS_WITHIN_THE_REQUESTED_RANGE_OVERFLOW_IS_REJECTED_BEBD1447 = "窗口网格无效：需提供包含端点的 start/end、origin（epoch 毫秒或 ISO 本地/偏移日期时间）、正数 width/step、unit ms|day|month 和 timezone。本地边界必须存在且无歧义；范围内最多 256 个起点，拒绝溢出。";
  public static final String EXCEPTION_INVALID_REDUCTION_REQUIRE_A_FINITE_NUMERIC_FIELD_GROUP_BY_ENTITY_POOLED_METRICS_COUNT_SUM_AVG_MIN_MAX_FIRST_LAST_DELTA_ELAPSED_MS_RATE_PER_MS_RANK_BY_MUST_BE_A_SELECTED_METRIC_ORDER_ASC_DESC_POSITIVE_TOP_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_3742D176 = "归约无效：需指定有限数值字段；group-by entity|pooled；metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms；rank-by 必须为选中指标，order asc|desc，top 为正数；最多 65536 个非空实体/窗口分组。";
  public static final String MESSAGE_GENERATE_EXPLICIT_WINDOWS_WITHOUT_READING_DATA_REQUIRED_START_END_ARE_INCLUSIVE_EPOCH_MS_ORIGIN_IS_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_WIDTH_IS_POSITIVE_STEP_DEFAULTS_TO_WIDTH_UNIT_MS_DAY_MONTH_DEFAULTS_TO_MS_TIMEZONE_DEFAULTS_TO_UTC_SELECT_GRID_STARTS_INSIDE_THE_RANGE_CLIP_EACH_FINAL_END_TO_THE_REQUESTED_END_OUTPUT_NOMINAL_HALF_OPEN_AND_EFFECTIVE_INCLUSIVE_BOUNDS_LOCAL_LABELS_ORIGIN_UNIT_AND_TIMEZONE_CALENDAR_OFFSETS_ARE_ANCHORED_TO_ORIGIN_NO_ITERATIVE_MONTH_DRIFT_AMBIGUOUS_NONEXISTENT_LOCAL_BOUNDARIES_AND_OVERFLOW_ARE_ERRORS_MAXIMUM_256_WINDOWS_PAGING_USES_N_OFFSET_90AB8B84 = "生成显式窗口，不读取数据。必需 --start/--end 为包含端点的 epoch 毫秒，--origin 为 epoch 毫秒或 ISO 本地/偏移日期时间，--width 为正数，--step 默认等于 width，--unit ms|day|month 默认 ms，--timezone 默认 UTC。仅选择范围内的网格起点，结束边界裁剪到指定 end。返回名义左闭右开及有效包含端点的边界、本地标签、起点、单位和时区。日历偏移以 origin 为锚点，避免逐月漂移；歧义、不存在的本地边界及溢出均报错。最多 256 窗口，支持 -n/--offset 分页。";
  public static final String MESSAGE_REDUCE_RAW_FINITE_NUMERIC_OBSERVATIONS_LOCALLY_FIELD_REQUIRED_METRICS_DEFAULTS_TO_COUNT_FIRST_LAST_DELTA_GROUP_BY_ENTITY_DEFAULT_KEEPS_TREE_DEVICE_OR_FULL_TABLE_TAG_TUPLE_POOLED_EXPLICITLY_MERGES_OBSERVATIONS_PER_WINDOW_NEVER_AVERAGES_ENTITY_AVERAGES_NULL_TARGET_SKIPPED_EMPTY_GROUPS_OMITTED_DELTA_LAST_FIRST_ELAPSED_MS_LAST_AT_FIRST_AT_RATE_PER_MS_DELTA_ELAPSED_MS_NULL_FOR_ZERO_SPAN_ENDPOINT_TIES_USE_LEXICOGRAPHIC_ENTITY_IDENTITY_NULL_TAGS_FIRST_RETURN_ENDPOINT_IDENTITIES_TIMES_OPTIONAL_RANK_BY_SELECTED_METRIC_ORDER_ASC_DESC_DEFAULT_DESC_TOP_POSITIVE_TIES_USE_WINDOW_INDEX_THEN_ENTITY_IDENTITY_NULL_SCORES_LAST_RANK_IS_ROW_NUMBER_RANKING_PRECEDES_TOP_THEN_OUTPUT_PAGING_USE_EXPLICIT_INCLUSIVE_WINDOWS_OR_START_END_GRID_PARAMETERS_FOLLOW_WINDOWS_COMMAND_AND_EXCLUDE_EXPLICIT_WINDOWS_ALL_RESULTS_RETAIN_GROUP_WINDOW_TIMEZONE_DESCRIPTORS_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_NO_SQL_AGGREGATION_PUSHDOWN_F01EECDA = "在本地归约原始有限数值观测：必需 --field；--metrics 默认 count,first,last,delta；--group-by entity（默认）保留 Tree 设备或完整 Table TAG 元组，pooled 显式按窗口合并观测，不计算实体均值的平均。跳过目标 NULL，省略空分组。delta=last-first；elapsed_ms=last_at-first_at；rate_per_ms=delta/elapsed_ms，跨度为零返回 NULL。端点同时间以实体身份字典序决胜（NULL TAG 优先），返回端点身份与时间。可指定 --rank-by 选中指标、--order asc|desc（默认 desc）、正数 --top；同分按窗口序号和实体身份排序，NULL 分数末尾，rank 为行号。先排名再 top 再分页。可用包含端点的 --windows 或 --start/--end；网格参数同 windows 命令且与显式 windows 互斥。结果保留分组、窗口、时区描述。最多 65536 非空实体/窗口分组，不下推 SQL 聚合。";
  public static final String MESSAGE_GENERATE_AN_EXPLICIT_FIXED_DURATION_OR_CALENDAR_WINDOW_GRID_CC82BDC7 = "生成显式固定时长或日历窗口网格。";
  public static final String MESSAGE_REDUCE_OBSERVATIONS_AT_AN_EXPLICIT_ENTITY_GRAIN_AND_RANK_RESULT_ROWS_6D9CCB5B = "按明确实体粒度归约观测并对结果行排名。";
}
