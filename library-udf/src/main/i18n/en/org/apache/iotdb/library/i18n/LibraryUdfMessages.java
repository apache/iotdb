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

  // Parameter validation and utility exceptions added for UDF edge cases.
  public static final String EXCEPTION_PARAMETER_Q1_AND_Q3_SHOULD_BE_FINITE_AND_Q1_SHOULD_BE_SMALLER_THAN_Q3_EB122E6C = "parameter $q1$ and $q3$ should be finite, and $q1$ should be smaller than $q3$";
  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_FINITE_AND_LARGER_THAN_0_ECE1B163 = "Parameter k should be finite and larger than 0.";
  public static final String EXCEPTION_PARAMETER_K_SHOULD_BE_A_POSITIVE_INTEGER_62AC8458 = "Parameter k should be a positive integer.";
  public static final String EXCEPTION_PARAMETER_WINDOW_SHOULD_BE_A_POSITIVE_INTEGER_54FB7DBB = "Parameter window should be a positive integer.";
  public static final String EXCEPTION_METHOD_SHOULD_BE_DEFAULT_OR_SERIES_181EEECF = "Method should be default or series.";
  public static final String EXCEPTION_PARAMETER_R_SHOULD_BE_FINITE_AND_NON_NEGATIVE_4FFDF663 = "Parameter r should be finite and non-negative.";
  public static final String EXCEPTION_PARAMETER_W_SHOULD_BE_A_POSITIVE_INTEGER_C6C838D2 = "Parameter w should be a positive integer.";
  public static final String EXCEPTION_PARAMETER_S_SHOULD_BE_A_POSITIVE_INTEGER_B941BE85 = "Parameter s should be a positive integer.";
  public static final String EXCEPTION_PARAMETER_LOWER_BOUND_AND_UPPER_BOUND_SHOULD_BE_FINITE_AND_LOWER_BOUND_SHOULD_BE_SMALLER_THAN_UPPER_BOUND_06A9FCA8 = "parameter \"lower_bound\" and \"upper_bound\" should be finite, and \"lower_bound\" should be smaller than \"upper_bound\".";
  public static final String EXCEPTION_PARAMETER_LEN_SHOULD_BE_FINITE_AND_LARGER_THAN_0_F93F1EC9 = "Parameter len should be finite and larger than 0.";
  public static final String EXCEPTION_PARAMETER_THRESHOLD_SHOULD_BE_FINITE_AND_NON_NEGATIVE_93B393D7 = "Parameter threshold should be finite and non-negative.";
  public static final String EXCEPTION_PARAMETER_P_SHOULD_BE_A_POSITIVE_INTEGER_D7914C8A = "Parameter p should be a positive integer.";
  public static final String EXCEPTION_THRESHOLD_HAS_TO_BE_FINITE_AND_NON_NEGATIVE_45C26FD0 = "threshold has to be finite and non-negative.";
  public static final String EXCEPTION_PARAMETER_END_SHOULD_BE_FINITE_AND_LARGER_THAN_START_A5FB7AEC = "parameter $end$ should be finite and larger than $start$";
  public static final String EXCEPTION_PARAMETER_MIN_AND_MAX_SHOULD_BE_FINITE_AND_MIN_SHOULD_BE_SMALLER_THAN_MAX_1EDB33AF = "parameter $min$ and $max$ should be finite, and $min$ should be smaller than $max$.";
  public static final String EXCEPTION_ERROR_BOUND_SHOULD_BE_FINITE_AND_NO_LESS_THAN_0_E2C80B2C = "Error bound should be finite and no less than 0.";
  public static final String EXCEPTION_PARAMETER_POINTS_SHOULD_BE_AT_LEAST_2_A4F55048 = "Parameter points should be at least 2.";
  public static final String EXCEPTION_PARAMETER_AVG_IS_ILLEGAL_IT_SHOULD_BE_FINITE_D4B6B4FF = "Parameter \"avg\" is illegal. It should be finite.";
  public static final String EXCEPTION_PARAMETER_SD_IS_ILLEGAL_IT_SHOULD_BE_FINITE_AND_LARGER_THAN_0_CCE0B275 = "Parameter \"sd\" is illegal. It should be finite and larger than 0.";
  public static final String EXCEPTION_PARAMETER_WINDOW_SHOULD_BE_A_POSITIVE_DURATION_OR_A_POSITIVE_INTEGER_41E4F4DB = "Parameter window should be a positive duration or a positive integer.";
  public static final String EXCEPTION_WINDOW_SIZE_SHOULD_BE_IN_0_INTEGER_MAX_VALUE_52F02FF6 = "Window size should be in (0, Integer.MAX_VALUE].";
  public static final String EXCEPTION_METHOD_SHOULD_BE_MEDIAN_MODE_OR_CLUSTER_7C3B0F56 = "Method should be Median, Mode, or Cluster.";
  public static final String EXCEPTION_ILLEGAL_FILL_METHOD_45FB5556 = "Illegal fill method.";
  public static final String EXCEPTION_PARAMETER_SIGMA_SHOULD_BE_FINITE_AND_LARGER_THAN_0_538FEDBA = "Parameter $sigma$ should be finite and larger than 0.";
  public static final String EXCEPTION_PARAMETER_MINSPEED_AND_MAXSPEED_SHOULD_BE_FINITE_AND_MINSPEED_SHOULD_BE_SMALLER_THAN_MAXSPEED_5998EA69 = "parameter $minSpeed$ and $maxSpeed$ should be finite, and $minSpeed$ should be smaller than $maxSpeed$.";
  public static final String EXCEPTION_PARAMETER_CENTER_SHOULD_BE_FINITE_21F289A3 = "Parameter $center$ should be finite.";
  public static final String EXCEPTION_METHOD_SHOULD_BE_SCREEN_OR_LSGREEDY_B5975D7A = "Method should be screen or lsgreedy.";
  public static final String EXCEPTION_COEF_SHOULD_BE_FINITE_DOUBLE_DOUBLE_WHEN_METHOD_IS_BLANK_F72B1092 = "coef should be finite double,double... when method is blank.";
  public static final String EXCEPTION_WAVELET_COEFFICIENTS_SHOULD_NOT_BE_EMPTY_2C78B047 = "Wavelet coefficients should not be empty.";
  public static final String EXCEPTION_WAVELET_COEFFICIENTS_SHOULD_BE_FINITE_D764C838 = "Wavelet coefficients should be finite.";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_PATTERN_MUST_BE_FINITE_DOUBLE_DOUBLE_612DE434 = "Illegal parameter, pattern must be finite double,double...";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_THRESHOLD_MUST_BE_A_FINITE_NON_NEGATIVE_NUMBER_1C438942 = "Illegal parameter, threshold must be a finite non-negative number.";
  public static final String EXCEPTION_ILLEGAL_PARAMETER_VALUEPATTERN_VALUES_MUST_BE_FINITE_FFD05AC9 = "Illegal parameter, valuePattern values must be finite.";
  public static final String EXCEPTION_REGEX_SHOULD_NOT_BE_EMPTY_AND_HAS_TO_BE_A_VALID_REGULAR_EXPRESSION_751072ED = "regex should not be empty and has to be a valid regular expression.";
  public static final String EXCEPTION_CAPACITY_SHOULD_BE_LARGER_THAN_0_055372C7 = "Capacity should be larger than 0.";
  public static final String EXCEPTION_TIME_VALUE_SHOULD_BE_FINITE_A661DDB0 = "Time value should be finite.";
  public static final String EXCEPTION_INVALID_DATETIME_FORMAT_A3DC435A = "Invalid datetime format.";

  private LibraryUdfMessages() {}
}
