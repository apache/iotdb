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
  public static final String EXCEPTION_THE_PARAM_FREQUENCY_MUST_BE_GREATER_THAN_0_45820CF9 = "参数 'frequency' 必须大于 0。";
  public static final String EXCEPTION_THE_PARAM_AMPLIFICATION_MUST_BE_GREATER_THAN_OR_EQUAL_TO_1_D64050EB =
      "参数 'amplification' 必须大于或等于 1。";
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

  // ExactOrderStatistics
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E = "不支持的数据类型：%s";

  // UDAFQuantile
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_A8CA7BE7 = "不支持的数据类型";
  public static final String EXCEPTION_SIZE_K_HAS_TO_BE_GREATER_THAN_OR_EQUAL_TO_100_C514D1C3 = "K 的大小必须大于或等于 100。";
  public static final String EXCEPTION_RANK_HAS_TO_BE_GREATER_THAN_0_AND_LESS_THAN_OR_EQUAL_TO_1_0F16AF94 =
      "rank 必须大于 0 且小于或等于 1。";

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

  // Parameter validation and utility exceptions added for UDF edge cases.
  public static final String EXCEPTION_PARAMETER_Q1_AND_Q3_SHOULD_BE_FINITE_AND_Q1_SHOULD_BE_SMALLER_THAN_Q3_EB122E6C = "参数 $q1$ 和 $q3$ 必须为有限值，且 $q1$ 必须小于 $q3$";
  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_FINITE_AND_LARGER_THAN_0_ECE1B163 = "参数 k 必须为有限且大于 0。";
  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_A_POSITIVE_INTEGER_62AC8458 = "参数 k 必须为正整数。";
  public static final String EXCEPTION_PARAMETER_WINDOW_SHOULD_BE_A_POSITIVE_INTEGER_54FB7DBB = "参数 window 必须为正整数。";
  public static final String EXCEPTION_METHOD_SHOULD_BE_DEFAULT_OR_SERIES_181EEECF = "method 必须为 default 或 series。";
  public static final String EXCEPTION_PARAMETER_R_SHOULD_BE_FINITE_AND_NON_NEGATIVE_4FFDF663 = "参数 r 必须为有限且非负。";
  public static final String EXCEPTION_PARAMETER_W_SHOULD_BE_A_POSITIVE_INTEGER_C6C838D2 = "参数 w 必须为正整数。";
  public static final String EXCEPTION_PARAMETER_S_SHOULD_BE_A_POSITIVE_INTEGER_B941BE85 = "参数 s 必须为正整数。";
  public static final String EXCEPTION_PARAMETER_LOWER_BOUND_AND_UPPER_BOUND_SHOULD_BE_FINITE_AND_LOWER_BOUND_SHOULD_BE_SMALLER_THAN_UPPER_BOUND_06A9FCA8 = "参数 lower_bound 和 upper_bound 必须为有限值，且 lower_bound 必须小于 upper_bound。";
  public static final String EXCEPTION_PARAMETER_LEN_SHOULD_BE_FINITE_AND_LARGER_THAN_0_F93F1EC9 = "参数 len 必须为有限且大于 0。";
  public static final String EXCEPTION_PARAMETER_THRESHOLD_SHOULD_BE_FINITE_AND_NON_NEGATIVE_93B393D7 = "参数 threshold 必须为有限且非负。";
  public static final String EXCEPTION_PARAMETER_P_SHOULD_BE_A_POSITIVE_INTEGER_D7914C8A = "参数 p 必须为正整数。";
  public static final String EXCEPTION_THRESHOLD_HAS_TO_BE_FINITE_AND_NON_NEGATIVE_45C26FD0 = "threshold 必须为有限且非负。";
  public static final String EXCEPTION_PARAMETER_END_SHOULD_BE_FINITE_AND_LARGER_THAN_START_A5FB7AEC = "参数 $end$ 必须为有限值且大于 $start$。";
  public static final String EXCEPTION_PARAMETER_MIN_AND_MAX_SHOULD_BE_FINITE_AND_MIN_SHOULD_BE_SMALLER_THAN_MAX_1EDB33AF = "参数 $min$ 和 $max$ 必须为有限值，且 $min$ 必须小于 $max$。";
  public static final String EXCEPTION_ERROR_BOUND_SHOULD_BE_FINITE_AND_NO_LESS_THAN_0_E2C80B2C = "误差界必须为有限且不小于 0。";
  public static final String EXCEPTION_PARAMETER_POINTS_SHOULD_BE_AT_LEAST_2_A4F55048 = "参数 points 至少为 2。";
  public static final String EXCEPTION_PARAMETER_AVG_IS_ILLEGAL_IT_SHOULD_BE_FINITE_D4B6B4FF = "参数 avg 无效，必须为有限值。";
  public static final String EXCEPTION_PARAMETER_SD_IS_ILLEGAL_IT_SHOULD_BE_FINITE_AND_LARGER_THAN_0_CCE0B275 = "参数 sd 无效，必须为有限且大于 0。";
  public static final String EXCEPTION_PARAMETER_WINDOW_SHOULD_BE_A_POSITIVE_DURATION_OR_A_POSITIVE_INTEGER_41E4F4DB = "参数 window 必须为正时长或正整数。";
  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_IN_0_INTEGER_MAX_VALUE_52F02FF6 = "窗口大小必须在 (0, Integer.MAX_VALUE] 范围内。";
  public static final String EXCEPTION_METHOD_SHOULD_BE_MEDIAN_MODE_OR_CLUSTER_7C3B0F56 = "method 必须为 Median、Mode 或 Cluster。";
  public static final String EXCEPTION_ILLEGAL_FILL_METHOD_45FB5556 = "填充方法无效。";
  public static final String EXCEPTION_PARAMETER_SIGMA_SHOULD_BE_FINITE_AND_LARGER_THAN_0_538FEDBA = "参数 $sigma$ 必须为有限且大于 0。";
  public static final String EXCEPTION_PARAMETER_MINSPEED_AND_MAXSPEED_SHOULD_BE_FINITE_AND_MINSPEED_SHOULD_BE_SMALLER_THAN_MAXSPEED_5998EA69 = "参数 $minSpeed$ 和 $maxSpeed$ 必须为有限值，且 $minSpeed$ 必须小于 $maxSpeed$。";
  public static final String EXCEPTION_PARAMETER_CENTER_SHOULD_BE_FINITE_21F289A3 = "参数 $center$ 必须为有限值。";
  public static final String EXCEPTION_METHOD_SHOULD_BE_SCREEN_OR_LSGREEDY_B5975D7A = "method 必须为 screen 或 lsgreedy。";
  public static final String EXCEPTION_COEF_SHOULD_BE_FINITE_DOUBLE_DOUBLE_WHEN_METHOD_IS_BLANK_F72B1092 = "method 为空时，coef 必须为有限的 double,double,...。";
  public static final String EXCEPTION_WAVELET_COEFFICIENTS_SHOULD_NOT_BE_EMPTY_2C78B047 = "Wavelet 系数不能为空。";
  public static final String EXCEPTION_WAVELET_COEFFICIENTS_SHOULD_BE_FINITE_D764C838 = "Wavelet 系数必须为有限值。";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_PATTERN_MUST_BE_FINITE_DOUBLE_DOUBLE_612DE434 = "非法参数，pattern 必须为有限的 double,double,...。";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_THRESHOLD_MUST_BE_A_FINITE_NON_NEGATIVE_NUMBER_1C438942 = "非法参数，threshold 必须为有限且非负数。";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_VALUEPATTERN_VALUES_MUST_BE_FINITE_FFD05AC9 = "非法参数，valuePattern 的值必须为有限数。";
  public static final String EXCEPTION_REGEX_SHOULD_NOT_BE_EMPTY_AND_HAS_TO_BE_A_VALID_REGULAR_EXPRESSION_751072ED = "regex 不能为空且必须是有效的正则表达式。";
  public static final String EXCEPTION_CAPACITY_SHOULD_BE_LARGER_THAN_0_055372C7 = "容量必须大于 0。";
  public static final String EXCEPTION_TIME_VALUE_SHOULD_BE_FINITE_A661DDB0 = "时间值必须为有限值。";
  public static final String EXCEPTION_INVALID_DATETIME_FORMAT_A3DC435A = "日期时间格式无效。";

  private LibraryUdfMessages() {}
}
