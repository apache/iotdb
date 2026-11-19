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

public final class FsTemporalMessages {
  private FsTemporalMessages() {}
  public static final String EXCEPTION_EXPECTED_GAPS_SESSIONS_EXTREME_WITH_FIELD_SESSIONS_REQUIRES_POSITIVE_GAP_MS_AND_NO_WINDOWS_EXTREME_REQUIRES_MODE_MIN_MAX_BOUNDS_ARE_INCLUSIVE_WINDOWS_EXCLUDE_START_END_AND_OUTPUT_LIMIT_OFFSET_MUST_BE_NONNEGATIVE_BACA632C = "需要 gaps/sessions/extreme 和 --field；sessions 需要正数 --gap-ms，且不接受 windows；extreme 需要 --mode min|max。时间端点均包含，windows 与 start/end 互斥，输出 limit/offset 必须非负。";
  public static final String EXCEPTION_UNKNOWN_OR_UNSUPPORTED_ANALYSIS_FIELD_ARG_SESSIONS_EXTREME_REQUIRE_FINITE_NUMERIC_FIELD_VALUES_B9ADCF22 = "未知或不支持的分析字段：%s；sessions/extreme 需要有限数值 FIELD。";
  public static final String EXCEPTION_ANALYSIS_REQUIRES_STRICTLY_INCREASING_TIMESTAMPS_WITHIN_EACH_ENTITY_655D0DE5 = "分析要求每个实体内的时间戳严格递增。";
}
