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
  public static final String NO_SUCH_DATA_TYPE = "No such kind of data type.";

  // UDTFLOF
  public static final String FAIL_TO_GET_LOF = "Fail to get LOF ";

  // TimeSeriesQuality, ValueRepair
  public static final String AT_LEAST_TWO_NON_NAN_VALUES_NEEDED =
      "At least two non-NaN values are needed";

  // dmatch/util/CrossCorrelation, dprofile/util/CrossCorrelation, dprofile/util/Segment, Util
  public static final String UTILITY_CLASS = "Utility class";

  // LinearRegression
  public static final String EMPTY_INPUT_ARRAYS = "Empty input array(s).";
  public static final String DIFFERENT_INPUT_ARRAY_LENGTH = "Different input array length.";
  public static final String INPUT_SERIES_SHOULD_BE_LONGER_THAN_1 =
      "Input series should be longer than 1.";
  public static final String ALL_INPUT_X_ARE_SAME = "All input x are same.";

  // Util
  public static final String FAIL_TO_GET_DATA_TYPE_IN_ROW = "Fail to get data type in row ";

  // CircularQueue, LongCircularQueue, DoubleCircularQueue, BooleanCircularQueue
  public static final String QUEUE_IS_EMPTY = "Error: Queue is Empty!";

  // UDTFValueFill, UDTFTimestampRepair, UDTFValueRepair
  public static final String ILLEGAL_METHOD = "Illegal method";
  public static final String ILLEGAL_METHOD_WITH_DOT = "Illegal method.";
  public static final String INVALID_TIME_FORMAT_FOR_INTERVAL = "Invalid time format for interval.";

  // ARFill
  public static final String CANNOT_FIT_AR1_MODEL =
      "Cannot fit AR(1) model. Please try another method.";

  // ValueFill
  public static final String ALL_VALUES_ARE_NAN = "All values are NaN";

  // UDFEnvelopeAnalysis
  public static final String EXCEPTION_THE_PARAM_FREQUENCY_MUST_BE_GREATER_THAN_0_45820CF9 =
      "The param 'frequency' must be greater than 0.";
  public static final String EXCEPTION_THE_PARAM_AMPLIFICATION_MUST_BE_GREATER_THAN_OR_EQUAL_TO_1_D64050EB =
      "The param 'amplification' must be greater than or equal to 1.";
  public static final String UNSUPPORTED_TIME_UNIT = "Unsupported time unit.";

  // UDTFAR
  public static final String ILLEGAL_INPUT = "Illegal input.";

  // UDTFDeconv
  public static final String DIVIDED_BY_ZERO = "Divided by zero.";

  // DWTUtil
  public static final String DATA_VECTOR_SIZE_NOT_POWER_OF_2 =
      "The data vector size is not a power of 2.";

  // UDTFCluster
  public static final String UNSUPPORTED_METHOD = "Unsupported method: ";

  // FFTUtil
  public static final String ITS_IMPOSSIBLE = "It's impossible";

  // KShape, MedoidShape, KMeans
  public static final String SAMPLES_MUST_BE_NON_EMPTY = "samples must be non-empty.";
  public static final String K_MUST_SATISFY_RANGE = "k must satisfy 2 <= k <= samples.length.";
  public static final String MAX_ITERATIONS_MUST_BE_AT_LEAST_1 =
      "maxIterations must be at least 1.";
  public static final String SAMPLE_DIMENSION_MUST_BE_POSITIVE =
      "sample dimension must be positive.";
  public static final String ALL_SAMPLES_MUST_HAVE_SAME_LENGTH =
      "All samples must have the same length.";

  // MedoidShape
  public static final String SAMPLE_RATE_MUST_BE_IN_RANGE = "sampleRate must be in (0, 1].";
  public static final String FAST_KSHAPE_EMPTY_CANDIDATE_POOL =
      "fastKShape: empty candidate pool.";
  public static final String FAST_KSHAPE_NO_CANDIDATE_SELECTED =
      "fastKShape: no candidate selected.";
  public static final String K_MUST_BE_AT_LEAST_2 = "k must be at least 2.";
  public static final String K_MUST_NOT_EXCEED_SAMPLES = "k must not exceed the number of samples.";

  // UDAFPatternMatch, UDAFDTWMatch
  public static final String UNSUPPORTED_DATATYPE = "Unsupported datatype %s";

  // ExactOrderStatistics
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E =
      "Unsupported data type: %s";

  // UDAFQuantile
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_A8CA7BE7 = "Unsupported data type";
  public static final String EXCEPTION_SIZE_K_HAS_TO_BE_GREATER_THAN_OR_EQUAL_TO_100_C514D1C3 =
      "Size K has to be greater than or equal to 100.";
  public static final String EXCEPTION_RANK_HAS_TO_BE_GREATER_THAN_0_AND_LESS_THAN_OR_EQUAL_TO_1_0F16AF94 =
      "rank has to be greater than 0 and less than or equal to 1.";

  // LinearScale
  public static final String DOMAIN_START_MUST_BE_LESS_THAN_DOMAIN_END =
      "domainStart must be less than domainEnd";
  public static final String VALUE_OUT_OF_DOMAIN_RANGE = "Value out of domain range";

  // MADSketch
  public static final String NO_VALUES_IN_TIME_SERIES = "No values in the time series";

  // SlidingCollector
  public static final String COMBINING_NOT_POSSIBLE = "Combining not possible";

  // Resampler
  public static final String ILLEGAL_AGGREGATION_ALGORITHM =
      "Error: Illegal Aggregation Algorithm.";
  public static final String ILLEGAL_INTERPOLATION_ALGORITHM =
      "Error: Illegal Interpolation Algorithm.";

  // Util
  public static final String EXCEPTION_THE_PROVIDED_TIME_PRECISION_IS_HIGHER_THAN_THE_SYSTEM_S_TIME_PRECISION_MS_PLEASE_CHECK_YOUR_INPUT_92667537 =
      "The provided time precision is higher than the system's time precision (ms). Please check your input.";
  public static final String EXCEPTION_THE_PROVIDED_TIME_PRECISION_IS_HIGHER_THAN_THE_SYSTEM_S_TIME_PRECISION_US_PLEASE_CHECK_YOUR_INPUT_07596E70 =
      "The provided time precision is higher than the system's time precision (us). Please check your input.";

  // DWTUtil
  public static final String EXCEPTION_THE_DATA_VECTOR_SIZE_IS_LESS_THAN_WAVELET_COEFFICIENT_SIZE_AC6652FF =
      "The data vector size is less than wavelet coefficient size.";

  // UDTFCluster
  public static final String EXCEPTION_TIME_SERIES_LENGTH_MUST_BE_AT_LEAST_L_GOT_322C35E3 =
      "Time series length must be at least l; got ";
  public static final String EXCEPTION_NOT_ENOUGH_NON_OVERLAPPING_WINDOWS_GOT_192388D0 =
      "Not enough non-overlapping windows: got ";
  public static final String EXCEPTION_TIME_SERIES_LENGTH_MUST_BE_AT_LEAST_L_GOT_ARG_POINTS_L_ARG_8C796580 =
      "Time series length must be at least l; got %d points, l=%d.";
  public static final String EXCEPTION_NOT_ENOUGH_NON_OVERLAPPING_WINDOWS_GOT_ARG_WINDOWS_NEED_AT_LEAST_K_ARG_9E42BA1F =
      "Not enough non-overlapping windows: got %d windows, need at least k=%d.";

  // UDAFPatternMatch
  public static final String EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_MUST_BE_LONG_LONG_B2DEE922 =
      "Illegal parameter, timePattern must be long,long...";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_VALUEPATTERN_MUST_BE_DOUBLE_DOUBLE_BAD1419C =
      "Illegal parameter, valuePattern must be double,double...";

  // OnePassBucketizer
  public static final String EXCEPTION_CAN_T_PRODUCE_1EF5D1BB = "Can't produce ";
  public static final String EXCEPTION_CAN_T_PRODUCE_ARG_BUCKETS_FROM_AN_INPUT_SERIES_OF_ARG_ELEMENTS_D17D0135 =
      "Can't produce %d buckets from an input series of %d elements";

  // UDFEnvelopeAnalysis
  public static final String EXCEPTION_FAIL_TO_GET_DATA_TYPE_IN_ROW_8CD82629 =
      "Fail to get data type in row ";

  private LibraryUdfMessages() {}

  public static final String EXCEPTION_CAN_T_OBTAIN_MAX_AREA_TRIANGLE_8BE13344 =
      "Can't obtain max area triangle";

  public static final String EXCEPTION_THE_VALUE_OF_THE_INPUT_TIME_SERIES_IS_NOT_NUMERIC_02437974 =
      "The value of the input time series is not numeric.\n";

  public static final String EXCEPTION_TARGET_SHOULD_NOT_BE_EMPTY_1B2C02C6 =
      "target should not be empty";

  public static final String
      EXCEPTION_LIMIT_HAS_TO_BE_1_FOR_REPLACING_ALL_MATCHES_OR_NON_NEGATIVE_INTEGERS_FOR_LIMITED_TIMES_30768594 =
          "limit has to be -1 for replacing all matches or non-negative integers for limited"
              + " times.";

  public static final String
      EXCEPTION_OFFSET_HAS_TO_BE_NON_NEGATIVE_TO_SKIP_FIRST_SEVERAL_MATCHES_2284C622 =
          "offset has to be non-negative to skip first several matches.";

  public static final String EXCEPTION_REGEXP_HAS_TO_BE_A_VALID_REGULAR_EXPRESSION_FA012C63 =
      "regexp has to be a valid regular expression.";

  public static final String
      EXCEPTION_INDEX_MUST_A_NON_NEGATIVE_INTEGER_TO_FETCH_SPLIT_RESULTS_OR_1_TO_GET_LENGTH_ABAF4445 =
          "index must a non-negative integer to fetch split results or -1 to get length.";

  public static final String EXCEPTION_REGEX_SHOULD_NOT_BE_EMPTY_AED1BC73 =
      "regex should not be empty";

  public static final String EXCEPTION_GROUP_INDEX_HAS_TO_BE_A_NON_NEGATIVE_INTEGER_A1B9B0F2 =
      "group index has to be a non-negative integer.";

  public static final String EXCEPTION_GAP_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_EDAA139C =
      "gap should be a time period whose unit is ms, s, m, h.";

  public static final String
      EXCEPTION_LENGTH_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_BE421588 =
          "length should be a time period whose unit is ms, s, m, h.";

  public static final String
      EXCEPTION_PARAMETER_COMPUTE_IS_ILLEGAL_PLEASE_USE_BATCH_FOR_DEFAULT_OR_STREAM_C46DD729 =
          "Parameter \"compute\" is illegal. Please use \"batch\" (for default) or \"stream\".";

  public static final String EXCEPTION_PARAMETER_SD_IS_ILLEGAL_IT_SHOULD_BE_LARGER_THAN_0_61264233 =
      "Parameter \"sd\" is illegal. It should be larger than 0.";

  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_A_POSITIVE_INTEGER_AF4CCC06 =
      "Window size should be a positive integer.";

  public static final String EXCEPTION_ERROR_BOUND_SHOULD_BE_NO_LESS_THAN_0_1C260253 =
      "Error bound should be no less than 0.";

  public static final String EXCEPTION_METHOD_IS_ILLEGAL_4B26E375 = "Method is illegal.";

  public static final String EXCEPTION_OUTPUT_TYPE_IS_INVALID_75CA99CF = "Output type is invalid.";

  public static final String EXCEPTION_K_SHOULD_BE_A_POSITIVE_INTEGER_8B9C3E96 =
      "k should be a positive integer.";

  public static final String EXCEPTION_ILLEGAL_SAMPLING_METHOD_4E8BB494 =
      "Illegal sampling method.";

  public static final String
      EXCEPTION_GAP_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_D_0B4CA46A =
          "gap should be a time period whose unit is ms, s, m, h, d.";

  public static final String EXCEPTION_AGGR_SHOULD_BE_MIN_MAX_MEAN_MEDIAN_FIRST_LAST_3588D6DE =
      "aggr should be min, max, mean, median, first, last.";

  public static final String
      EXCEPTION_START_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_3FAAA2A2 =
          "start should conform to the format yyyy-MM-dd HH:mm:ss.";

  public static final String
      EXCEPTION_END_SHOULD_CONFORM_TO_THE_FORMAT_YYYY_MM_DD_HH_MM_SS_F7135FE6 =
          "end should conform to the format yyyy-MM-dd HH:mm:ss.";

  public static final String
      EXCEPTION_PARAMETER_LAG_SHOULD_BE_AN_POSITIVE_INTEGER_OR_0_FOR_DEFAULT_VALUE_2401EAB3 =
          "Parameter $lag$ should be an positive integer, or '0' for default value.";

  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_LARGER_THAN_0_F4C2184C =
      "Window size should be larger than 0.";

  public static final String EXCEPTION_PARAMETER_MIN_SHOULD_BE_SMALLER_THAN_MAX_91D250E1 =
      "parameter $min$ should be smaller than $max$.";

  public static final String EXCEPTION_PARAMETER_COUNT_SHOULD_BE_LARGER_THAN_0_8E8FFF68 =
      "parameter $count$ should be larger than 0";

  public static final String
      EXCEPTION_PARAMETER_END_SHOULD_BE_LARGER_THAN_OR_EQUAL_TO_START_FBDBED0B =
          "parameter $end$ should be larger than or equal to $start$";

  public static final String
      EXCEPTION_ERROR_HAS_TO_BE_GREATER_THAN_OR_EQUAL_TO_0_AND_LESS_THAN_1_2A840865 =
          "error has to be greater than or equal to 0 and less than 1.";

  public static final String
      EXCEPTION_UNKNOWN_TIME_UNIT_INPUT_SUPPORTED_UNITS_ARE_NS_US_MS_S_M_H_D_69C56AE2 =
          "Unknown time unit input. Supported units are ns, us, ms, s, m, h, d.";

  public static final String EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_SIZE_MUST_LARGER_1_35FEDD60 =
      "Illegal parameter, timePattern size must larger 1.";

  public static final String
      EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_VALUE_MUST_BE_IN_ASCENDING_ORDER_C07440BB =
          "Illegal parameter, timePattern value must be in ascending order.";

  public static final String
      EXCEPTION_ILLEGAL_PARAMETER_TIMEPATTERN_SIZE_MUST_EQUALS_VALUEPATTERN_SIZE_00A547B1 =
          "Illegal parameter, timePattern size must equals valuePattern size.";

  public static final String EXCEPTION_WINDOW_HAS_TO_BE_A_POSITIVE_INTEGER_4A4AC181 =
      "window has to be a positive integer.";

  public static final String EXCEPTION_THRESHOLD_HAS_TO_BE_NON_NEGATIVE_4A7BED9A =
      "threshold has to be non-negative.";

  public static final String EXCEPTION_WPASS_SHOULD_BE_WITHIN_0_1_24771828 =
      "Wpass should be within (0,1).";

  public static final String
      EXCEPTION_INTERVAL_SHOULD_BE_A_TIME_PERIOD_WHOSE_UNIT_IS_MS_S_M_H_D_AF16953B =
          "interval should be a time period whose unit is ms, s, m, h, d.";

  public static final String
      EXCEPTION_METHOD_NOT_SUPPORTED_PLEASE_INPUT_COEFFICIENT_AND_LEAVE_METHOD_BLANK_2D687DFE =
          "Method not supported, please input coefficient and leave method blank.";

  public static final String EXCEPTION_LAYER_HAS_TO_BE_A_POSITIVE_INTEGER_43638533 =
      "layer has to be a positive integer.";

  public static final String EXCEPTION_TYPE_SHOULD_BE_UNIFORM_OR_NONUNIFORM_DC4A47B1 =
      "Type should be 'uniform' or 'nonuniform'.";

  public static final String EXCEPTION_RESULT_SHOULD_BE_REAL_IMAG_ABS_OR_ANGLE_F16F02C8 =
      "Result should be 'real', 'imag', 'abs' or 'angle'.";

  public static final String EXCEPTION_COMPRESS_SHOULD_BE_WITHIN_0_1_263845C8 =
      "Compress should be within (0,1].";

  public static final String EXCEPTION_RESULT_SHOULD_BE_QUOTIENT_OR_REMAINDER_ABBEA7C4 =
      "Result should be 'quotient' or 'remainder'.";

  public static final String EXCEPTION_PARAMETER_SIGMA_SHOULD_BE_LARGER_THAN_0_48732F14 =
      "Parameter $sigma$ should be larger than 0.";

  public static final String EXCEPTION_PARAMETER_MINSPEED_SHOULD_BE_SMALLER_THAN_MAXSPEED_31BD6787 =
      "parameter $minSpeed$ should be smaller than $maxSpeed$.";

  public static final String
      EXCEPTION_INVALID_TIME_UNIT_INPUT_SUPPORTED_UNITS_ARE_NS_US_MS_S_M_H_D_15CD3A00 =
          "Invalid time unit input. Supported units are ns, us, ms, s, m, h, d.";

  public static final String EXCEPTION_PARAMETER_L_MUST_BE_A_POSITIVE_INTEGER_1DBBDE06 =
      "Parameter l must be a positive integer.";

  public static final String EXCEPTION_PARAMETER_K_MUST_BE_AT_LEAST_2_D6C929C3 =
      "Parameter k must be at least 2.";

  public static final String
      EXCEPTION_PARAMETER_METHOD_MUST_BE_ONE_OF_KMEANS_KSHAPE_MEDOIDSHAPE_1015186E =
          "Parameter method must be one of: kmeans, kshape, medoidshape.";

  public static final String EXCEPTION_PARAMETER_MAXITER_MUST_BE_A_POSITIVE_INTEGER_D64DD725 =
      "Parameter maxiter must be a positive integer.";

  public static final String EXCEPTION_PARAMETER_OUTPUT_MUST_BE_LABEL_OR_CENTROID_153DFB81 =
      "Parameter output must be label or centroid.";

  public static final String EXCEPTION_PARAMETER_SAMPLE_RATE_MUST_BE_IN_0_1_79A35554 =
      "Parameter sample_rate must be in (0, 1].";

  public static final String
      EXCEPTION_MINLEN_SHOULD_BE_AN_INTEGER_GREATER_THAN_OR_EQUAL_TO_10_D160A79D =
          "minlen should be an integer greater than or equal to 10.";

  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_LARGER_THAN_0_9F12BFC9 =
      "Parameter k should be larger than 0.";

  public static final String EXCEPTION_PARAMETER_Q1_SHOULD_BE_SMALLER_THAN_Q3_74CEA186 =
      "parameter $q1$ should be smaller than $q3$";
}
