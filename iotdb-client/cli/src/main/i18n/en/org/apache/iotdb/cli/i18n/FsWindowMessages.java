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
  public static final String EXCEPTION_INVALID_WINDOW_GRID_REQUIRE_INCLUSIVE_START_END_ORIGIN_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_POSITIVE_WIDTH_STEP_UNIT_MS_DAY_MONTH_AND_TIMEZONE_LOCAL_BOUNDARIES_MUST_BE_UNAMBIGUOUS_AND_EXISTENT_AT_MOST_256_STARTS_WITHIN_THE_REQUESTED_RANGE_OVERFLOW_IS_REJECTED_BEBD1447 = "Invalid window grid: require inclusive start/end, origin (epoch ms or ISO local/offset datetime), positive width/step, unit ms|day|month and timezone. Local boundaries must be unambiguous and existent; at most 256 starts within the requested range; overflow is rejected.";
  public static final String EXCEPTION_INVALID_REDUCTION_REQUIRE_A_FINITE_NUMERIC_FIELD_GROUP_BY_ENTITY_POOLED_METRICS_COUNT_SUM_AVG_MIN_MAX_FIRST_LAST_DELTA_ELAPSED_MS_RATE_PER_MS_RANK_BY_MUST_BE_A_SELECTED_METRIC_ORDER_ASC_DESC_POSITIVE_TOP_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_3742D176 = "Invalid reduction: require a finite numeric field; group-by entity|pooled; metrics count,sum,avg,min,max,first,last,delta,elapsed_ms,rate_per_ms; rank-by must be a selected metric, order asc|desc, positive top; at most 65536 nonempty entity/window groups.";
}
