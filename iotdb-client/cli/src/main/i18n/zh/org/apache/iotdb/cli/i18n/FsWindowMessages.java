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
}
