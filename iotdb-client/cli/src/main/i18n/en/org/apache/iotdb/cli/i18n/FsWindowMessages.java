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
  public static final String MESSAGE_GENERATE_EXPLICIT_WINDOWS_WITHOUT_READING_DATA_REQUIRED_START_END_ARE_INCLUSIVE_EPOCH_MS_ORIGIN_IS_EPOCH_MS_OR_ISO_LOCAL_OFFSET_DATETIME_WIDTH_IS_POSITIVE_STEP_DEFAULTS_TO_WIDTH_UNIT_MS_DAY_MONTH_DEFAULTS_TO_MS_TIMEZONE_DEFAULTS_TO_UTC_SELECT_GRID_STARTS_INSIDE_THE_RANGE_CLIP_EACH_FINAL_END_TO_THE_REQUESTED_END_OUTPUT_NOMINAL_HALF_OPEN_AND_EFFECTIVE_INCLUSIVE_BOUNDS_LOCAL_LABELS_ORIGIN_UNIT_AND_TIMEZONE_CALENDAR_OFFSETS_ARE_ANCHORED_TO_ORIGIN_NO_ITERATIVE_MONTH_DRIFT_AMBIGUOUS_NONEXISTENT_LOCAL_BOUNDARIES_AND_OVERFLOW_ARE_ERRORS_MAXIMUM_256_WINDOWS_PAGING_USES_N_OFFSET_90AB8B84 = "Generate explicit windows without reading data. Required --start/--end are inclusive epoch ms, --origin is epoch ms or ISO local/offset datetime, --width is positive, --step defaults to width, --unit ms|day|month defaults to ms, --timezone defaults to UTC. Select grid starts inside the range; clip each final end to the requested end. Output nominal half-open and effective inclusive bounds, local labels, origin, unit and timezone. Calendar offsets are anchored to origin (no iterative month drift); ambiguous/nonexistent local boundaries and overflow are errors. Maximum 256 windows. Paging uses -n/--offset.";
  public static final String MESSAGE_REDUCE_RAW_FINITE_NUMERIC_OBSERVATIONS_LOCALLY_FIELD_REQUIRED_METRICS_DEFAULTS_TO_COUNT_FIRST_LAST_DELTA_GROUP_BY_ENTITY_DEFAULT_KEEPS_TREE_DEVICE_OR_FULL_TABLE_TAG_TUPLE_POOLED_EXPLICITLY_MERGES_OBSERVATIONS_PER_WINDOW_NEVER_AVERAGES_ENTITY_AVERAGES_NULL_TARGET_SKIPPED_EMPTY_GROUPS_OMITTED_DELTA_LAST_FIRST_ELAPSED_MS_LAST_AT_FIRST_AT_RATE_PER_MS_DELTA_ELAPSED_MS_NULL_FOR_ZERO_SPAN_ENDPOINT_TIES_USE_LEXICOGRAPHIC_ENTITY_IDENTITY_NULL_TAGS_FIRST_RETURN_ENDPOINT_IDENTITIES_TIMES_OPTIONAL_RANK_BY_SELECTED_METRIC_ORDER_ASC_DESC_DEFAULT_DESC_TOP_POSITIVE_TIES_USE_WINDOW_INDEX_THEN_ENTITY_IDENTITY_NULL_SCORES_LAST_RANK_IS_ROW_NUMBER_RANKING_PRECEDES_TOP_THEN_OUTPUT_PAGING_USE_EXPLICIT_INCLUSIVE_WINDOWS_OR_START_END_GRID_PARAMETERS_FOLLOW_WINDOWS_COMMAND_AND_EXCLUDE_EXPLICIT_WINDOWS_ALL_RESULTS_RETAIN_GROUP_WINDOW_TIMEZONE_DESCRIPTORS_AT_MOST_65536_NONEMPTY_ENTITY_WINDOW_GROUPS_NO_SQL_AGGREGATION_PUSHDOWN_F01EECDA = "Reduce raw finite numeric observations locally: --field required; --metrics defaults to count,first,last,delta; --group-by entity (default) keeps Tree device or full Table TAG tuple, pooled explicitly merges observations per window, never averages entity averages. NULL target skipped; empty groups omitted. delta=last-first; elapsed_ms=last_at-first_at; rate_per_ms=delta/elapsed_ms, NULL for zero span. Endpoint ties use lexicographic entity identity (NULL tags first); return endpoint identities/times. Optional --rank-by selected metric --order asc|desc (default desc), --top positive; ties use window index then entity identity; NULL scores last, rank is row number. Ranking precedes top then output paging. Use explicit inclusive --windows or --start/--end; grid parameters follow windows command and exclude explicit windows. All results retain group/window/timezone descriptors. At most 65536 nonempty entity/window groups; no SQL aggregation pushdown.";
  public static final String MESSAGE_GENERATE_AN_EXPLICIT_FIXED_DURATION_OR_CALENDAR_WINDOW_GRID_CC82BDC7 = "Generate an explicit fixed-duration or calendar window grid.";
  public static final String MESSAGE_REDUCE_OBSERVATIONS_AT_AN_EXPLICIT_ENTITY_GRAIN_AND_RANK_RESULT_ROWS_6D9CCB5B = "Reduce observations at an explicit entity grain and rank result rows.";
}
