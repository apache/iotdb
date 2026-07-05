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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.library.i18n;

public final class LibraryUdfMessages {

  // UDTFRange, UDTFTwoSidedFilter
  public static final String NO_SUCH_DATA_TYPE = "不支持此数据类型。";

  // UDTFLOF
  public static final String FAIL_TO_GET_LOF = "获取 LOF 失败 ";

  // TimeSeriesQuality, ValueRepair
  public static final String AT_LEAST_TWO_NON_NAN_VALUES_NEEDED = "至少需要两个非 NaN 值";

  // dmatch/util/CrossCorrelation, dprofile/util/CrossCorrelation, dprofile/util/Segment, Util
  public static final String UTILITY_CLASS = "工具类";

  // LinearRegression
  public static final String EMPTY_INPUT_ARRAYS = "输入数组为空。";
  public static final String DIFFERENT_INPUT_ARRAY_LENGTH = "输入数组长度不一致。";
  public static final String INPUT_SERIES_SHOULD_BE_LONGER_THAN_1 = "输入序列长度应大于 1。";
  public static final String ALL_INPUT_X_ARE_SAME = "所有输入 x 值相同。";

  // Util
  public static final String FAIL_TO_GET_DATA_TYPE_IN_ROW = "获取行数据类型失败，行时间戳：";

  // CircularQueue, LongCircularQueue, DoubleCircularQueue, BooleanCircularQueue
  public static final String QUEUE_IS_EMPTY = "错误：队列为空！";

  // UDTFValueFill, UDTFTimestampRepair, UDTFValueRepair
  public static final String ILLEGAL_METHOD = "非法方法";
  public static final String ILLEGAL_METHOD_WITH_DOT = "非法方法。";
  public static final String INVALID_TIME_FORMAT_FOR_INTERVAL = "时间间隔格式无效。";

  // ARFill
  public static final String CANNOT_FIT_AR1_MODEL = "无法拟合 AR(1) 模型，请尝试其他方法。";

  // ValueFill
  public static final String ALL_VALUES_ARE_NAN = "所有值均为 NaN";

  // UDFEnvelopeAnalysis
  public static final String UNSUPPORTED_TIME_UNIT = "不支持的时间单位。";

  // UDTFAR
  public static final String ILLEGAL_INPUT = "非法输入。";

  // UDTFDeconv
  public static final String DIVIDED_BY_ZERO = "除以零。";

  // DWTUtil
  public static final String DATA_VECTOR_SIZE_NOT_POWER_OF_2 = "数据向量大小不是 2 的幂。";

  // UDTFCluster
  public static final String UNSUPPORTED_METHOD = "不支持的方法：";

  // FFTUtil
  public static final String ITS_IMPOSSIBLE = "这不可能发生";

  // KShape, MedoidShape, KMeans
  public static final String SAMPLES_MUST_BE_NON_EMPTY = "样本不能为空。";
  public static final String K_MUST_SATISFY_RANGE = "k 必须满足 2 <= k <= samples.length。";
  public static final String MAX_ITERATIONS_MUST_BE_AT_LEAST_1 = "maxIterations 至少为 1。";
  public static final String SAMPLE_DIMENSION_MUST_BE_POSITIVE = "样本维度必须为正数。";
  public static final String ALL_SAMPLES_MUST_HAVE_SAME_LENGTH = "所有样本必须具有相同的长度。";

  // MedoidShape
  public static final String SAMPLE_RATE_MUST_BE_IN_RANGE = "sampleRate 必须在 (0, 1] 范围内。";
  public static final String FAST_KSHAPE_EMPTY_CANDIDATE_POOL = "fastKShape：候选池为空。";
  public static final String FAST_KSHAPE_NO_CANDIDATE_SELECTED = "fastKShape：未选出候选项。";
  public static final String K_MUST_BE_AT_LEAST_2 = "k 至少为 2。";
  public static final String K_MUST_NOT_EXCEED_SAMPLES = "k 不能超过样本数量。";

  // UDAFPatternMatch, UDAFDTWMatch
  public static final String UNSUPPORTED_DATATYPE = "不支持的数据类型 %s";

  // LinearScale
  public static final String DOMAIN_START_MUST_BE_LESS_THAN_DOMAIN_END =
      "domainStart 必须小于 domainEnd";
  public static final String VALUE_OUT_OF_DOMAIN_RANGE = "值超出域范围";

  // MADSketch
  public static final String NO_VALUES_IN_TIME_SERIES = "时间序列中没有值";

  // SlidingCollector
  public static final String COMBINING_NOT_POSSIBLE = "无法合并";

  // Resampler
  public static final String ILLEGAL_AGGREGATION_ALGORITHM = "错误：非法聚合算法。";
  public static final String ILLEGAL_INTERPOLATION_ALGORITHM = "错误：非法插值算法。";

  // Util
  public static final String EXCEPTION_THE_PROVIDED_TIME_PRECISION_IS_HIGHER_THAN_THE_SYSTEM_S_TIME_PRECISION_MS_PLEASE_CHECK_YOUR_INPUT_92667537 =
      "提供的时间精度高于系统时间精度（ms）。请检查输入。";
  public static final String EXCEPTION_THE_PROVIDED_TIME_PRECISION_IS_HIGHER_THAN_THE_SYSTEM_S_TIME_PRECISION_US_PLEASE_CHECK_YOUR_INPUT_07596E70 =
      "提供的时间精度高于系统时间精度（us）。请检查输入。";

  // DWTUtil
  public static final String EXCEPTION_THE_DATA_VECTOR_SIZE_IS_LESS_THAN_WAVELET_COEFFICIENT_SIZE_AC6652FF =
      "数据向量大小小于 wavelet 系数大小。";

  // UDTFCluster
  public static final String EXCEPTION_TIME_SERIES_LENGTH_MUST_BE_AT_LEAST_L_GOT_322C35E3 =
      "时间序列长度必须至少为 l，实际为 ";
  public static final String EXCEPTION_NOT_ENOUGH_NON_OVERLAPPING_WINDOWS_GOT_192388D0 =
      "非重叠窗口数量不足，实际为 ";
  public static final String EXCEPTION_TIME_SERIES_LENGTH_MUST_BE_AT_LEAST_L_GOT_ARG_POINTS_L_ARG_8C796580 =
      "时间序列长度必须至少为 l，实际为 %d 个点，l=%d。";
  public static final String EXCEPTION_NOT_ENOUGH_NON_OVERLAPPING_WINDOWS_GOT_ARG_WINDOWS_NEED_AT_LEAST_K_ARG_9E42BA1F =
      "非重叠窗口数量不足，实际为 %d 个窗口，至少需要 k=%d。";

  // UDAFPatternMatch
  public static final String EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_MUST_BE_LONG_LONG_B2DEE922 =
      "非法参数，timePattern 必须为 long,long...";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_VALUEPATTERN_MUST_BE_DOUBLE_DOUBLE_BAD1419C =
      "非法参数，valuePattern 必须为 double,double...";

  // OnePassBucketizer
  public static final String EXCEPTION_CAN_T_PRODUCE_1EF5D1BB = "无法生成 ";
  public static final String EXCEPTION_CAN_T_PRODUCE_ARG_BUCKETS_FROM_AN_INPUT_SERIES_OF_ARG_ELEMENTS_D17D0135 =
      "无法生成 %d 个 bucket，输入序列包含 %d 个元素";

  // UDFEnvelopeAnalysis
  public static final String EXCEPTION_FAIL_TO_GET_DATA_TYPE_IN_ROW_8CD82629 =
      "获取该行数据类型失败，行时间戳为 ";

  private LibraryUdfMessages() {}
}
