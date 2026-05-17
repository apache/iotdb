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

  private LibraryUdfMessages() {}
}
