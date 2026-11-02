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

  private LibraryUdfMessages() {}

  public static final String EXCEPTION_CAN_T_OBTAIN_MAX_AREA_TRIANGLE_8BE13344 = "无法获取面积最大的三角形";

  public static final String EXCEPTION_THE_VALUE_OF_THE_INPUT_TIME_SERIES_IS_NOT_NUMERIC_02437974 =
      "输入时间序列的值不是数值。\n";

  public static final String EXCEPTION_TARGET_SHOULD_NOT_BE_EMPTY_1B2C02C6 = "target 不应为空";

  public static final String
      EXCEPTION_LIMIT_HAS_TO_BE_1_FOR_REPLACING_ALL_MATCHES_OR_NON_NEGATIVE_INTEGERS_FOR_LIMITED_TIMES_30768594 =
          "limit 必须为 -1（替换所有匹配项）或非负整数（限制替换次数）。";

  public static final String
      EXCEPTION_OFFSET_HAS_TO_BE_NON_NEGATIVE_TO_SKIP_FIRST_SEVERAL_MATCHES_2284C622 =
          "offset 必须为非负数，以跳过前几个匹配项。";

  public static final String EXCEPTION_REGEXP_HAS_TO_BE_A_VALID_REGULAR_EXPRESSION_FA012C63 =
      "regexp 必须为有效的正则表达式。";

  public static final String
      EXCEPTION_INDEX_MUST_A_NON_NEGATIVE_INTEGER_TO_FETCH_SPLIT_RESULTS_OR_1_TO_GET_LENGTH_ABAF4445 =
          "index 必须为非负整数（获取拆分结果）或 -1（获取长度）。";

  public static final String EXCEPTION_REGEX_SHOULD_NOT_BE_EMPTY_AED1BC73 = "regex 不应为空";

  public static final String EXCEPTION_GROUP_INDEX_HAS_TO_BE_A_NON_NEGATIVE_INTEGER_A1B9B0F2 =
      "分组索引必须为非负整数。";

  public static final String EXCEPTION_GAP_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_EDAA139C =
      "gap 应为时间间隔，单位为 ms、s、m、h。";

  public static final String
      EXCEPTION_LENGTH_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_BE421588 =
          "length 应为时间间隔，单位为 ms、s、m、h。";

  public static final String
      EXCEPTION_PARAMETER_COMPUTE_IS_ILLEGAL_PLEASE_USE_BATCH_FOR_DEFAULT_OR_STREAM_C46DD729 =
          "参数 \"compute\" 无效。请使用 \"batch\"（默认）或 \"stream\"。";

  public static final String EXCEPTION_PARAMETER_SD_IS_ILLEGAL_IT_SHOULD_BE_LARGER_THAN_0_61264233 =
      "参数 \"sd\" 无效，应大于 0。";

  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_A_POSITIVE_INTEGER_AF4CCC06 =
      "窗口大小应为正整数。";

  public static final String EXCEPTION_ERROR_BOUND_SHOULD_BE_NO_LESS_THAN_0_1C260253 =
      "误差界限应不小于 0。";

  public static final String EXCEPTION_METHOD_IS_ILLEGAL_4B26E375 = "方法无效。";

  public static final String EXCEPTION_OUTPUT_TYPE_IS_INVALID_75CA99CF = "输出类型无效。";

  public static final String EXCEPTION_K_SHOULD_BE_A_POSITIVE_INTEGER_8B9C3E96 = "k 应为正整数。";

  public static final String EXCEPTION_ILLEGAL_SAMPLING_METHOD_4E8BB494 = "采样方法无效。";

  public static final String
      EXCEPTION_GAP_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_D_0B4CA46A =
          "gap 应为时间间隔，单位为 ms、s、m、h、d。";

  public static final String EXCEPTION_AGGR_SHOULD_BE_MIN_MAX_MEAN_MEDIAN_FIRST_LAST_3588D6DE =
      "aggr 应为 min、max、mean、median、first、last。";

  public static final String
      EXCEPTION_START_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_3FAAA2A2 =
          "start 应符合格式 yyyy-MM-dd HH:mm:ss。";

  public static final String
      EXCEPTION_END_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_F7135FE6 =
          "end 应符合格式 yyyy-MM-dd HH:mm:ss。";

  public static final String
      EXCEPTION_PARAMETER_LAG_SHOULD_BE_AN_POSITIVE_INTEGER_OR_0_FOR_DEFAULT_VALUE_2401EAB3 =
          "参数 $lag$ 应为正整数，或使用 '0' 表示默认值。";

  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_LARGER_THAN_0_F4C2184C = "窗口大小应大于 0。";

  public static final String EXCEPTION_PARAMETER_MIN_SHOULD_BE_SMALLER_THAN_MAX_91D250E1 =
      "参数 $min$ 应小于 $max$。";

  public static final String EXCEPTION_PARAMETER_COUNT_SHOULD_BE_LARGER_THAN_0_8E8FFF68 =
      "参数 $count$ 应大于 0";

  public static final String
      EXCEPTION_PARAMETER_END_SHOULD_BE_LARGER_THAN_OR_EQUAL_TO_START_FBDBED0B =
          "参数 $end$ 应大于或等于 $start$";

  public static final String
      EXCEPTION_ERROR_HAS_TO_BE_GREATER_THAN_OR_EQUAL_TO_0_AND_LESS_THAN_1_2A840865 =
          "error 必须大于或等于 0 且小于 1。";

  public static final String
      EXCEPTION_UNKNOWN_TIME_UNIT_INPUT_SUPPORTED_UNITS_ARE_NS_US_MS_S_M_H_D_69C56AE2 =
          "输入的时间单位未知。支持的单位为 ns、us、ms、s、m、h、d。";

  public static final String EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_SIZE_MUST_LARGER_1_35FEDD60 =
      "参数无效，timePattern 的大小必须大于 1。";

  public static final String
      EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_VALUE_MUST_BE_IN_ASCENDING_ORDER_C07440BB =
          "参数无效，timePattern 的值必须按升序排列。";

  public static final String
      EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_SIZE_MUST_EQUALS_VALUEPATTERN_SIZE_00A547B1 =
          "参数无效，timePattern 与 valuePattern 的大小必须相等。";

  public static final String EXCEPTION_WINDOW_HAS_TO_BE_A_POSITIVE_INTEGER_4A4AC181 =
      "window 必须为正整数。";

  public static final String EXCEPTION_THRESHOLD_HAS_TO_BE_NON_NEGATIVE_4A7BED9A =
      "threshold 必须为非负数。";

  public static final String EXCEPTION_WPASS_SHOULD_BE_WITHIN_0_1_24771828 = "Wpass 应在 (0,1) 范围内。";

  public static final String
      EXCEPTION_INTERVAL_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_D_AF16953B =
          "interval 应为时间间隔，单位为 ms、s、m、h、d。";

  public static final String
      EXCEPTION_METHOD_NOT_SUPPORTED_PLEASE_INPUT_COEFFICIENT_AND_LEAVE_METHOD_BLANK_2D687DFE =
          "不支持此方法，请输入 coefficient 并将 method 留空。";

  public static final String EXCEPTION_LAYER_HAS_TO_BE_A_POSITIVE_INTEGER_43638533 =
      "layer 必须为正整数。";

  public static final String EXCEPTION_TYPE_SHOULD_BE_UNIFORM_OR_NONUNIFORM_DC4A47B1 =
      "Type 应为 'uniform' 或 'nonuniform'。";

  public static final String EXCEPTION_RESULT_SHOULD_BE_REAL_IMAG_ABS_OR_ANGLE_F16F02C8 =
      "Result 应为 'real'、'imag'、'abs' 或 'angle'。";

  public static final String EXCEPTION_COMPRESS_SHOULD_BE_WITHIN_0_1_263845C8 =
      "Compress 应在 (0,1] 范围内。";

  public static final String EXCEPTION_RESULT_SHOULD_BE_QUOTIENT_OR_REMAINDER_ABBEA7C4 =
      "Result 应为 'quotient' 或 'remainder'。";

  public static final String EXCEPTION_PARAMETER_SIGMA_SHOULD_BE_LARGER_THAN_0_48732F14 =
      "参数 $sigma$ 应大于 0。";

  public static final String EXCEPTION_PARAMETER_MINSPEED_SHOULD_BE_SMALLER_THAN_MAXSPEED_31BD6787 =
      "参数 $minSpeed$ 应小于 $maxSpeed$。";

  public static final String
      EXCEPTION_INVALID_TIME_UNIT_INPUT_SUPPORTED_UNITS_ARE_NS_US_MS_S_M_H_D_15CD3A00 =
          "输入的时间单位无效。支持的单位为 ns、us、ms、s、m、h、d。";

  public static final String EXCEPTION_PARAMETER_L_MUST_BE_A_POSITIVE_INTEGER_1DBBDE06 =
      "参数 l 必须为正整数。";

  public static final String EXCEPTION_PARAMETER_K_MUST_BE_AT_LEAST_2_D6C929C3 = "参数 k 必须至少为 2。";

  public static final String
      EXCEPTION_PARAMETER_METHOD_MUST_BE_ONE_OF_KMEANS_KSHAPE_MEDOIDSHAPE_1015186E =
          "参数 method 必须为 kmeans、kshape、medoidshape 之一。";

  public static final String EXCEPTION_PARAMETER_MAXITER_MUST_BE_A_POSITIVE_INTEGER_D64DD725 =
      "参数 maxiter 必须为正整数。";

  public static final String EXCEPTION_PARAMETER_OUTPUT_MUST_BE_LABEL_OR_CENTROID_153DFB81 =
      "参数 output 必须为 label 或 centroid。";

  public static final String EXCEPTION_PARAMETER_SAMPLE_RATE_MUST_BE_IN_0_1_79A35554 =
      "参数 sample_rate 必须在 (0, 1] 范围内。";

  public static final String
      EXCEPTION_MINLEN_SHOULD_BE_AN_INTEGER_GREATER_THAN_OR_EQUAL_TO_10_D160A79D =
          "minlen 应为大于或等于 10 的整数。";

  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_LARGER_THAN_0_9F12BFC9 = "参数 k 应大于 0。";

  public static final String EXCEPTION_PARAMETER_Q1_SHOULD_BE_SMALLER_THAN_Q3_74CEA186 =
      "参数 $q1$ 应小于 $q3$";
}
