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

package org.apache.iotdb.calc.i18n;

public final class CalcMessages {

  private CalcMessages() {}

  public static final String AND_OPERATOR_ONLY_ACCEPTS_BOOLEAN_OPERANDS =
      "AND operator only accepts Boolean operands";
  public static final String ARRAYS_NOT_SAME_LENGTH = "Arrays not same length";
  public static final String CANNOT_ADD_NAN_TO_T_DIGEST = "Cannot add NaN to t-digest";
  public static final String CANNOT_CAST_TO_BOOLEAN = "\"%s\" cannot be cast to [BOOLEAN]";
  public static final String CANNOT_PARSE_STRING_TO_DOUBLE = "Cannot parse String to double: ";
  public static final String CANT_HAPPEN_LOOP_FELL_THROUGH =
      "Can't happen ... loop fell through";
  public static final String COUNT_ALL_ACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS =
      "CountAllAccumulator does not support statistics.";
  public static final String COUNT_IF_ACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS =
      "CountIfAccumulator does not support statistics";
  public static final String CURRENT_COLUMN_IS_NOT_OBJECT_COLUMN =
      "current column is not object column";
  public static final String CURRENT_TS_BLOCK_SIZE_IS = "Current tsBlock size is : {}";
  public static final String DATA_TYPE_CANNOT_BE_ORDERED = "Data type: %s cannot be ordered";
  public static final String DECODE_BASE32_ERROR = "decode base32 error";
  public static final String DECODE_BASE64_ERROR = "decode base64 error";
  public static final String DECODE_BASE64URL_ERROR = "decode base64url error";
  public static final String DECODE_HEX_ERROR = "decode hex error";
  public static final String DENSE_RANK_NOT_YET_IMPLEMENTED = "DENSE_RANK not yet implemented";
  public static final String DISTINCT_AGGREGATION_FUNCTION_CANNOT_BE_PUSH_DOWN =
      "Distinct aggregation function can not be push down";
  public static final String DIVISION_BY_ZERO = "Division by zero";
  public static final String ESCAPE_STRING_MUST_BE_A_SINGLE_CHARACTER =
      "Escape string must be a single character";
  public static final String EXCEPTION_HAPPENED_WHEN_EXECUTING_UDTF =
      "Exception happened when executing UDTF: ";
  public static final String FAIL_TO_CLOSE_FILE_CHANNEL = "Fail to close fileChannel";
  public static final String ILLEGAL_STATE_IN_VISIT_LOGICAL_EXPRESSION =
      "Illegal state in visitLogicalExpression";
  public static final String INDEX_OUT_OF_PARTITION_BOUNDS = "Index out of Partition's bounds!";
  public static final String INITIAL_CAPACITY_IS_NEGATIVE = "Initial capacity (%d) is negative";
  public static final String INITIAL_CAPACITY_EXCEEDS_LIMIT = "Initial capacity (%d) exceeds %d";
  public static final String INPUT_ROW_UTILS_SHOULD_NOT_BE_INSTANTIATED =
      "InputRowUtils should not be instantiated.";
  public static final String INVALID_AGGREGATION_FUNCTION = "Invalid Aggregation function: ";
  public static final String INVALID_TEXT_INPUT_FOR_BOOLEAN =
      "Invalid text input for boolean type: %s";
  public static final String INVALID_VALUE = "Invalid value: %f";
  public static final String LEFT_CHILD_OF_JOIN_NODE_DOESNT_CONTAIN_LEFT_JOIN_KEY =
      "Left child of JoinNode doesn't contain left join key.";
  public static final String MAX_TUPLE_SIZE_OF_TS_BLOCK_IS = "maxTupleSizeOfTsBlock is：{}";
  public static final String MEMORY_IS_NOT_ENOUGH_FOR_CURRENT_QUERY =
      "Memory is not enough for current query.";
  public static final String MERGE_SORT_HEAP_SHOULD_BE_EMPTY = "mergeSortHeap should be empty!";
  public static final String MODULUS_BY_ZERO = "Modulus by zero";
  public static final String MULTIPLE_I_OBJECT_FILE_SERVICE_PROVIDER_FOUND =
      "Multiple IObjectFileServiceProvider found";
  public static final String MULTIPLE_I_TEMPORARY_QUERY_DATA_FILE_SERVICE_PROVIDER_FOUND =
      "Multiple ITemporaryQueryDataFileServiceProvider found";
  public static final String NOT_ENOUGH_MEMORY_FOR_SORTING = "Not enough memory for sorting";
  public static final String NOT_YET_IMPLEMENTED = "not yet implemented";
  public static final String NO_I_OBJECT_FILE_SERVICE_PROVIDER_FOUND =
      "No IObjectFileServiceProvider found";
  public static final String NO_I_TEMPORARY_QUERY_DATA_FILE_SERVICE_PROVIDER_FOUND =
      "No ITemporaryQueryDataFileServiceProvider found";
  public static final String OFFSET_LESS_THAN_ZERO = "offset %d is less than 0.";
  public static final String ONLY_ONE_TUPLE_CAN_BE_SENT_EACH_TIME =
      "Only one tuple can be sent each time caused by limited memory, oneTupleSize: {}B, maxReturnSize: {}B";
  public static final String OR_OPERATOR_ONLY_ACCEPTS_BOOLEAN_OPERANDS =
      "OR operator only accepts Boolean operands";
  public static final String PERCENTAGE_SHOULD_BE_IN_0_1 = "percentage should be in [0,1], got ";
  public static final String READ_OBJECT_IS_NOT_SUPPORTED = "readObject is not supported";
  public static final String RESULT_TS_BLOCK_CANNOT_BE_NULL = "Result tsBlock cannot be null";
  public static final String RIGHT_CHILD_OF_JOIN_NODE_DOESNT_CONTAIN_RIGHT_JOIN_KEY =
      "Right child of JoinNode doesn't contain right join key.";
  public static final String SHOULD_CALL_THE_CONCRETE_VISIT_XX_METHOD =
      "should call the concrete visitXX() method";
  public static final String STATE_FOR_GROUP_NOT_FOUND = "State for group %d is not found";
  public static final String SUM_SHOULD_NEVER_BE_ZERO = "sum should never be zero.";
  public static final String THIS_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVING_INPUTS =
      "This Accumulator does not support removing inputs!";
  public static final String THIS_IS_A_UTILITY_CLASS_AND_CANNOT_BE_INSTANTIATED =
      "This is a utility class and cannot be instantiated";
  public static final String TYPE_OF_LEFT_ASOF_JOIN_KEY_IS_NOT_TIMESTAMP =
      "Type of left ASOF Join key is not TIMESTAMP";
  public static final String TYPE_OF_RIGHT_ASOF_JOIN_KEY_IS_NOT_TIMESTAMP =
      "Type of right ASOF Join key is not TIMESTAMP";
  public static final String UDAF_NOT_SUPPORT_CALCULATE_FROM_STATISTICS =
      "UDAF not support calculate from statistics now";
  public static final String UNBOUND_FOLLOWING_NOT_ALLOWED_IN_FRAME_START =
      "UNBOUND FOLLOWING is not allowed in frame start!";
  public static final String UNBOUND_PRECEDING_NOT_ALLOWED_IN_FRAME_END =
      "UNBOUND PRECEDING is not allowed in frame end!";
  public static final String UNBOUND_PRECEDING_NOT_ALLOWED_IN_FRAME_START =
      "UNBOUND PRECEDING is not allowed in frame start!";
  public static final String UNEXPECTED_ANCHOR_TYPE = "unexpected anchor type: ";
  public static final String UNEXPECTED_EXTRACT_FIELD = "Unexpected extract field: ";
  public static final String UNEXPECTED_SIZE_FOR_LAST_SEQUENCE =
      "Unexpected size for last sequence: ";
  public static final String UNEXPECTED_SKIP_TO_POSITION = "unexpected SKIP TO position: ";
  public static final String UNEXPECTED_VALUE_FOR_REMAINDER = "Unexpected value for remainder: ";
  public static final String UNEXPECTED_VALUE_FOR_SEQUENCES =
      "Unexpected value for sequences: ";
  public static final String UNHANDLED_LITERAL_TYPE = "Unhandled literal type: ";
  public static final String UNKNOWN_DATA_TYPE = "Unknown data type: ";
  public static final String UNKNOWN_DATATYPE = "Unknown datatype: ";
  public static final String UNKNOWN_RANKING_TYPE = "Unknown ranking type: ";
  public static final String UNKNOWN_SIGN = "Unknown sign: ";
  public static final String UNKNOWN_TYPE = "Unknown type: ";
  public static final String UNREACHABLE = "Unreachable!";
  public static final String UNSUPPORTED_ARITHMETIC_OPERATOR =
      "Unsupported arithmetic operator: ";
  public static final String UNSUPPORTED_ASOF_JOIN_TYPE = "Unsupported ASOF join type: ";
  public static final String UNSUPPORTED_CAST_TO_TYPE = "Unsupported cast to type: ";
  public static final String UNSUPPORTED_COLUMN_TRANSFORMER = "Unsupported ColumnTransformer";
  public static final String UNSUPPORTED_COMPARISON_OPERATOR =
      "Unsupported comparison operator: ";
  public static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
  public static final String UNSUPPORTED_DATA_TYPE_LOWER = "unsupported data type: ";
  public static final String UNSUPPORTED_FRAME_BOUND_TYPE = "Unsupported frame bound type: ";
  public static final String UNSUPPORTED_FUNCTION_KIND = "Unsupported function kind: ";
  public static final String UNSUPPORTED_JOIN_TYPE = "Unsupported join type: ";
  public static final String UNSUPPORTED_LOGICAL_OPERATOR = "Unsupported logical operator: ";
  public static final String UNSUPPORTED_TYPE = "Unsupported type: ";
  public static final String UNSUPPORTED_TYPE_BINARY = "Unsupported Type";
  public static final String UNSUPPORTED_TYPE_CLASS = "Unsupported type: ";
  public static final String UNSUPPORTED_TYPE_FOR_ARITHMETIC_OPERATION =
      "Unsupported type for arithmetic operation: ";
  public static final String UNSUPPORTED_TYPE_IN_GENERIC_LITERAL =
      "Unsupported type in GenericLiteral: ";
  public static final String WEIGHT_MUST_BE_GE_1 = "weight must be >= 1, was ";

  public static final String PUSHED_ELEMENT_IS_NULL =
      "pushed element is null";

  public static final String FAILED_TO_DELETE_TEMP_DIR = "Failed to delete temp dir {}.";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_WRONG_OBJECT_FILE_PATH_D317B1AB = "wrong object file path: ";
  public static final String EXCEPTION_OFFSET_ARG_GREATER_THAN_EQUAL_OBJECT_SIZE_ARG_FILE_PATH_31936A19 = "offset %d is greater than or equal to object size %d, file path is %s";
  public static final String EXCEPTION_READ_OBJECT_SIZE_ARG_TOO_LARGE_SIZE_2G_FILE_PATH_7D4A4D10 = "Read object size %s is too large (size > 2G), file path is %s";
  public static final String LOG_FAILED_CLOSE_FILE_METHOD_DEREGISTER_ARG_BECAUSE_ARG_1744AC60 = "Failed to close file in method deregister(%s), because %s";
  public static final String LOG_FAILED_CLEAN_DIR_METHOD_DEREGISTER_ARG_BECAUSE_ARG_F53193E5 = "Failed to clean dir in method deregister(%s), because %s";
  public static final String EXCEPTION_TYPE_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVE_INPUT_905AFAA1 = "This type of accumulator does not support remove input!";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_VARIANCE_ARG_17A45959 = "Unsupported data type in aggregation variance : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_0A19C2A8 = "Unsupported data type in CentralMoment Aggregation: ";
  public static final String EXCEPTION_UNKNOWN_FUNCTION_ARG_NODE_ARG_927DA6A7 = "Unknown function %s on Node: %d.";
  public static final String EXCEPTION_INLIST_LITERAL_TIMESTAMP_CAN_ONLY_LONGLITERAL_DOUBLELITERAL_GENERICLITERAL_CURRENT_A3105E67 =
      "InList Literal for TIMESTAMP can only be LongLiteral, DoubleLiteral and GenericLiteral,"
      + " current is ";
  public static final String EXCEPTION_PARTITION_HANDLER_FINISHED_CANNOT_ADD_MORE_DATA_D8E31A3E = "The partition handler is finished, cannot add more data.";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_PATTERN_VARIABLE_NOT_PRESENT_MATCH_2891B276 = "AFTER MATCH SKIP TO failed: pattern variable is not present in match";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_CANNOT_SKIP_FIRST_ROW_MATCH_71D4093B = "AFTER MATCH SKIP TO failed: cannot skip to first row of match";
  public static final String EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_COPYING_3E03976C = "aggregate function %s does not support copying";
  public static final String EXCEPTION_UNSUPPORTED_BUILT_WINDOW_FUNCTION_NAME_F994ED27 = "Unsupported built-in window function name: ";
  public static final String EXCEPTION_UNSUPPORTED_DEFAULT_VALUE_S_DATA_TYPE_LAG_BEB00511 = "Unsupported default value's data type in Lag: ";
  public static final String EXCEPTION_CANNOT_COMPARE_VALUES_DIFFERENT_TYPES_A95EDA7F = "Cannot compare values of different types: ";
  public static final String EXCEPTION_VS_AEDAB253 = " vs. ";
  public static final String EXCEPTION_TARGET_TYPE_NOT_GENERICDATATYPE_CB87EF37 = "Target type is not a GenericDataType: ";
  public static final String EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_5C50C92E = "Unsupported expression type: ";
  public static final String EXCEPTION_IDENTITYLINEARFILL_S_NEEDPREPAREFORNEXT_METHOD_SHOULD_ALWAYS_RETURN_FALSE_FF7AD73B = "IdentityLinearFill's needPrepareForNext() method should always return false.";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_ARG_37F52124 = "Unsupported data type in LAST: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_2FCF5ADA = "Unsupported data type in LAST Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_B3F2EE42 = "Unsupported data type in LAST aggregation: %s";
  public static final String EXCEPTION_SIZE_CHILDRENCOLUMNS_DATATYPES_SHOULD_SAME_6EA43EA8 = "The size of childrenColumns and dataTypes should be the same.";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_8D31C6CA = "Unsupported data type in FIRST_BY Aggregation: %s";
  public static final String EXCEPTION_APPROX_PERCENTILE_REQUIRES_2_3_ARGUMENTS_BUT_GOT_ARG_D78590AA = "APPROX_PERCENTILE requires 2 or 3 arguments, but got %d";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_PERCENTILE_AGGREGATION_ARG_CFEC0431 = "Unsupported data type in APPROX_PERCENTILE Aggregation: %s";
  public static final String EXCEPTION_APPROXPERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_2BB01365 = "ApproxPercentileAccumulator does not support statistics";
  public static final String EXCEPTION_NO_EXACT_DOUBLE_REPRESENTATION_LONG_ARG_3E5B7550 = "no exact double representation for long: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_COVARIANCE_AGGREGATION_ARG_643BEDD0 = "Unsupported data type in Covariance Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_8C41FBCC = "Unsupported data type in LAST_BY Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_ARG_74B9A3A8 = "Unsupported data type in CentralMoment Aggregation: %s";
  public static final String EXCEPTION_SECOND_THIRD_ARGUMENT_MUST_GREATER_THAN_0_BUT_GOT_K_0AFFFA6A = "The second and third argument must be greater than 0, but got k=";
  public static final String EXCEPTION_CAPACITY_E92593AD = ", capacity=";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_AVG_ARG_4E63A3C3 = "Unsupported data type in aggregation AVG : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CORRELATION_AGGREGATION_ARG_2FF29597 = "Unsupported data type in Correlation Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_ARG_D5B6C6F9 = "Unsupported data type in FIRST: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_B2D27BB9 = "Unsupported data type in FIRST Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_2652C363 = "Unsupported data type in FIRST aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_VARIANCE_AGGREGATION_ARG_C641D425 = "Unsupported data type in VARIANCE Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_7D59F7B2 = "Unsupported data type : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_COUNT_DISTINCT_AGGREGATION_ARG_58F0391E = "Unsupported data type in APPROX_COUNT_DISTINCT Aggregation: %s";
  public static final String EXCEPTION_APPROXCOUNTDISTINCTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_81005A79 = "ApproxCountDistinctAccumulator does not support statistics";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_AGGREGATION_ARG_CCB09C60 = "Unsupported data type in MAX Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_SUM_AGGREGATION_ARG_92F5A18D = "Unsupported data type in SUM Aggregation: %s";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_8CA29DC2 = "distinct values has exceeded the threshold %s when calculate Mode";
  public static final String EXCEPTION_APPROXMOSTFREQUENTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_DB1E218B = "ApproxMostFrequentAccumulator does not support statistics";
  public static final String EXCEPTION_DISTINCT_AGGREGATION_FUNCTION_STATE_CAN_NOT_COPIED_34D0A276 = "Distinct aggregation function state can not be copied";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_REGRESSION_AGGREGATION_ARG_7BB08DA2 = "Unsupported data type in Regression Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MIN_AGGREGATION_ARG_CB73B158 = "Unsupported data type in MIN Aggregation: %s";
  public static final String EXCEPTION_INVALID_FORMAT_SERIALIZED_HISTOGRAM_GOT_ENCODING_A8E50EA5 = "Invalid format for serialized histogram, got encoding: ";
  public static final String EXCEPTION_MAX_STANDARD_ERROR_MUST_ARG_ARG_ARG_EB8443D6 = "Max Standard Error must be in [%s, %s]: %s";
  public static final String EXCEPTION_CANNOT_MERGE_HYPERLOGLOG_INSTANCES_DIFFERENT_PRECISION_9DCF13BC = "Cannot merge HyperLogLog instances with different precision";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_ONE_GROUP_A3F5A1D3 = "distinct values has exceeded the threshold %s when calculate MODE in one group";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_MIN_AGGREGATION_ARG_982BEBD5 = "Unsupported data type in MAX_BY/MIN_BY Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_AF4DA98F = "Unsupported data type EXTREME Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_276BE220 = "Unsupported data type in EXTREME Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AVG_AGGREGATION_ARG_66BF3128 = "Unsupported data type in AVG Aggregation: %s";
  public static final String EXCEPTION_CAN_T_READ_NEW_TSBLOCK_FILESPILLERREADER_072AF71D = "Can't read a new tsBlock in FileSpillerReader: ";
  public static final String EXCEPTION_CAN_T_CLOSE_FILECHANNEL_FILESPILLERREADER_E46979F0 = "Can't close fileChannel in FileSpillerReader: ";
  public static final String EXCEPTION_CREATE_FILE_ERROR_B8B379CF = "Create file error: ";
  public static final String EXCEPTION_CAN_T_WRITE_INTERMEDIATE_SORTED_DATA_FILE_0027961E = "Can't write intermediate sorted data to file: ";
  public static final String EXCEPTION_CAN_T_GET_FILE_FILESPILLERREADER_CHECK_IF_FILE_EXISTS_DEED83D9 = "Can't get file for FileSpillerReader, check if the file exists: ";
  public static final String EXCEPTION_LONG_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B3F9016B = "long value %d is out of range of integer value.";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B0E6DDED = "Float value %f is out of range of integer value.";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_LONG_VALUE_62F8153E = "Float value %f is out of range of long value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_BAB52E11 = "Double value %f is out of range of integer value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_LONG_VALUE_5793A91E = "Double value %f is out of range of long value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_DB914FA0 = "Double value %f is out of range of float value.";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_D171B313 = "Text value %s is out of range of float value.";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_DOUBLE_VALUE_C0589D83 = "Text value %s is out of range of double value.";
  public static final String EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_ARG_678B759C = "Unsupported source dataType: %s";
  public static final String EXCEPTION_CANNOT_CAST_ARG_ARG_TYPE_8266A2C6 = "Cannot cast %s to %s type";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_NUM_MUST_REPRESENTABLE_BITS_SPECIFIED_ARG_241AD2FE =
      "Argument exception, the scalar function num must be representable with the bits specified. %s"
      + " cannot be represented with %s bits.";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_BIT_COUNT_BITS_MUST_BETWEEN_2_6A365911 = "Argument exception, the scalar function bit_count bits must be between 2 and 64.";
  public static final String LOG_ERROR_OCCURRED_DURING_EXECUTING_UDTF_PLEASE_CHECK_WHETHER_IMPLEMENTATION_UDF_7A719D38 =
      "Error occurred during executing UDTF, please check whether the implementation of UDF is"
      + " correct according to the udf-api description.";
  public static final String EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDTF_ARG_ARG_PLEASE_CHECK_WHETHER_4E706370 =
      "Error occurred during executing UDTF#%s: %s, please check whether the implementation of UDF"
      + " is correct according to the udf-api description.";
  public static final String EXCEPTION_UNSUPPORTED_TYPE_FF7F518D = "Unsupported Type: ";
  public static final String EXCEPTION_LOGICALORMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_E3A2B2FA = "LogicalOrMultiColumnTransformer do not support doTransform with selection";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E = "Unsupported data type: %s";
  public static final String EXCEPTION_LOGICALANDMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_2507527D = "LogicalAndMultiColumnTransformer do not support doTransform with selection";
  public static final String EXCEPTION_ERROR_OCCURS_EVALUATING_USER_DEFINED_SCALAR_FUNCTION_05903C18 = "Error occurs when evaluating user-defined scalar function ";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_MD5_INVALID_INPUT_FORMAT_EMPTY_KEY_DED1C525 =
      "Failed to execute function hmac_md5 due to an invalid input format, the empty key is not"
      + " allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA1_INVALID_INPUT_FORMAT_EMPTY_KEY_518063E5 =
      "Failed to execute function hmac_sha1 due to an invalid input format, the empty key is not"
      + " allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA256_INVALID_INPUT_FORMAT_EMPTY_KEY_2F6AD64E =
      "Failed to execute function hmac_sha256 due to an invalid input format, the empty key is not"
      + " allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA512_INVALID_INPUT_FORMAT_EMPTY_KEY_3671AF09 =
      "Failed to execute function hmac_sha512 due to an invalid input format, the empty key is not"
      + " allowed in HMAC operation.";
  public static final String EXCEPTION_INVALID_FORMAT_STRING_ARG_ARG_05853138 = "Invalid format string: %s (%s)";
  public static final String EXCEPTION_YEAR_MUST_BETWEEN_1000_9999_8FBB94AA = "Year must be between 1000 and 9999.";
  public static final String EXCEPTION_UNSUPPORTED_PRECISION_CDB58979 = "Unsupported precision: ";
  public static final String EXCEPTION_UNKNOWN_PRECISION_0119BEB0 = "Unknown precision: ";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_LENGTH_MUST_NOT_LESS_THAN_072B7355 = "Argument exception,the scalar function substring length must not be less than 0";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_MUST_NOT_GREATER_THAN_F0BA2A56 =
      "Argument exception,the scalar function substring beginPosition must not be greater than the"
      + " string length";
  public static final String EXCEPTION_TYPE_DB38663D = "type ";
  public static final String EXCEPTION_NOT_SUPPORTED_GENERATEPROBLEMATICVALUESTRING_624BB179 = " is not supported in generateProblematicValueString()";
  public static final String EXCEPTION_BASE64_LENGTH_MUST_MULTIPLE_4_INCLUDING_PADDING_8717829D = "Base64 length must be a multiple of 4 (including padding '=')";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_32_MUST_4_C1214BB5 = "The length of the input BLOB of function from_big_endian_32 must be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_64_MUST_8_797F1A26 = "The length of the input BLOB of function from_big_endian_64 must be 8.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_32_MUST_4_0FBBD083 = "The length of the input BLOB of function from_little_endian_32 must be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_64_MUST_8_771F7A96 = "The length of the input BLOB of function from_little_endian_64 must be 8.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_32_BIG_ENDIAN_MUST_4_51AF4EF0 = "The length of the input BLOB of function from_ieee754_32_big_endian must be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_64_BIG_ENDIAN_MUST_8_56F33455 = "The length of the input BLOB of function from_ieee754_64_big_endian must be 8.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_INVALID_TARGET_SIZE_5054AE0B =
      "Failed to execute function '%s' due to the value %s corresponding to a invalid target size,"
      + " the allowed range is [0, %d].";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_EMPTY_PADDING_STRING_F1CE573C = "Failed to execute function '%s' due the value %s corresponding to a empty padding string.";
  public static final String EXCEPTION_FOUND_NO_COLUMN_ARG_ARG_8CF632F7 = "Found no column %s in %s";
  public static final String EXCEPTION_SORT_ITEM_ARG_NOT_INCLUDED_CHILDREN_S_OUTPUT_COLUMNS_CC911999 = "Sort Item %s is not included in children's output columns";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFTOUTPUTSYMBOL_0C8AA216 = "Left child of JoinNode doesn't contain LeftOutputSymbol ";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHTOUTPUTSYMBOL_10A86F63 = "Right child of JoinNode doesn't contain RightOutputSymbol ";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFT_ASOF_MAIN_JOIN_2850234A = "Left child of JoinNode doesn't contain left ASOF main join key.";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHT_ASOF_MAIN_JOIN_1A9631C9 = "Right child of JoinNode doesn't contain right ASOF main join key.";
  public static final String EXCEPTION_JOIN_KEY_TYPE_MISMATCH_LEFT_JOIN_KEY_TYPE_072E692E = "Join key type mismatch. Left join key type: ";
  public static final String EXCEPTION_RIGHT_JOIN_KEY_TYPE_56895767 = ", right join key type: ";
  public static final String EXCEPTION_ROW_COUNT_MUST_0_1_8D44189F = "Row count must be 0 or 1";
  public static final String EXCEPTION_COLUMN_ARG_CANNOT_RESOLVED_508579EC = "Column '%s' cannot be resolved.";
  public static final String EXCEPTION_REQUESTED_VALUE_UNKNOWN_CAPTURE_WAS_IT_REGISTERED_PATTERN_C77DF7E9 = "Requested value for unknown Capture. Was it registered in the Pattern?";
  public static final String EXCEPTION_TARGET_NODE_INDEX_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_75BFB7C3 = "Target node index must be greater than or equal to one";
  public static final String EXCEPTION_ALREADY_AT_TARGET_32DFD57F = "Already at target";
  public static final String EXCEPTION_PARTIALRESULT_OF_COVARIANCE_SHOULD_BE_1_E6A950B1 = "partialResult of Covariance should be 1";
  public static final String EXCEPTION_INPUT_OF_COVARIANCE_SHOULD_BE_1_75D3F4FD = "Input of Covariance should be 1";
  public static final String EXCEPTION_COVARIANCE_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_8C088DC6 = "Covariance state count is smaller than removed state count";
  public static final String EXCEPTION_PARTIALRESULT_OF_VARIANCE_SHOULD_BE_1_9C281868 = "partialResult of variance should be 1";
  public static final String EXCEPTION_INPUT_OF_VARIANCE_SHOULD_BE_1_1BC7A702 = "Input of variance should be 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_CENTRALMOMENT_SHOULD_BE_1_B7C0B4B9 = "partialResult of CentralMoment should be 1";
  public static final String EXCEPTION_PARTIALRESULT_SHOULD_BE_1_399D5DC3 = "partialResult should be 1";
  public static final String EXCEPTION_INPUT_OF_CENTRALMOMENT_SHOULD_BE_1_FD23B170 = "Input of CentralMoment should be 1";
  public static final String EXCEPTION_CENTRALMOMENT_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_B87864BA = "CentralMoment state count is smaller than removed state count";
  public static final String EXCEPTION_PARTIALRESULT_OF_CORRELATION_SHOULD_BE_1_D5A8CED6 = "partialResult of Correlation should be 1";
  public static final String EXCEPTION_INPUT_OF_CORRELATION_SHOULD_BE_1_5D666DB5 = "Input of Correlation should be 1";
  public static final String EXCEPTION_CORRELATION_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_66C90757 = "Correlation state count is smaller than removed state count";
  public static final String EXCEPTION_PARTIALRESULT_OF_REGRESSION_SHOULD_BE_1_B7ED33FA = "partialResult of Regression should be 1";
  public static final String EXCEPTION_INPUT_OF_REGRESSION_SHOULD_BE_1_9AF4E8EC = "Input of Regression should be 1";
  public static final String EXCEPTION_REGRESSION_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_4415A3AE = "Regression state count is smaller than removed state count";
  public static final String EXCEPTION_INVALID_STARTINGPOSITION_COLON_ARG_7B4D49C0 = "invalid startingPosition: %s";
  public static final String EXCEPTION_ROWID_DOES_NOT_MATCH_THIS_TSBLOCK_18F9EB4D = "rowId does not match this TsBlock";
  public static final String EXCEPTION_CURSOR_STILL_ACTIVE_E9462D3B = "Cursor still active";
  public static final String EXCEPTION_NO_ACTIVE_CURSOR_C89C9944 = "No active cursor";
  public static final String EXCEPTION_NOT_YET_ADVANCED_1600C5A7 = "Not yet advanced";
  public static final String EXCEPTION_SHOULD_NOT_ATTEMPT_COMPACTION_WHEN_TSBLOCK_IS_LOCKED_DF775E6B = "Should not attempt compaction when TsBlock is locked";
  public static final String EXCEPTION_STRATEGY_IS_NULL_DDA2C67F = "strategy is null";
  public static final String EXCEPTION_ROWIDEVICTIONLISTENER_IS_NULL_457EFBEE = "rowIdEvictionListener is null";
  public static final String EXCEPTION_TOPN_MUST_BE_GREATER_THAN_ZERO_43866D3E = "topN must be greater than zero";
  public static final String EXCEPTION_NO_ROOT_TO_PEEK_43C3D8D5 = "No root to peek";
  public static final String EXCEPTION_GROUP_ID_HAS_AN_EMPTY_HEAP_C9EDB841 = "Group ID has an empty heap";
  public static final String EXCEPTION_POPANDINSERT_LEFT_PAREN_RIGHT_PAREN_REQUIRES_AT_LEAST_A_ROOT_NODE_0CA0DD58 = "popAndInsert() requires at least a root node";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext is null";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator is null";
  public static final String EXCEPTION_LIMIT_MUST_BE_AT_LEAST_ZERO_E960530F = "limit must be at least zero";
  public static final String EXCEPTION_OFFSET_MUST_BE_AT_LEAST_ZERO_21BB6BB6 = "offset must be at least zero";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks is null";
  public static final String EXCEPTION_FILLARRAY_SHOULD_NOT_BE_NULL_OR_EMPTY_118FB134 = "fillArray should not be null or empty";
  public static final String EXCEPTION_UNIQUE_ROW_ID_EXCEEDS_A_LIMIT_COLON_ARG_71EE4B7D = "Unique row id exceeds a limit: %s";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_FOR_PREVIOUSFILLWITHGROUPOPERATOR_SHOULD_NEVER_BE_NEGATIVE_ABA9C2B1 = "helperColumnIndex for PreviousFillWithGroupOperator should never be negative";
  public static final String EXCEPTION_OUTPUTCOLUMNCOUNT_IS_NOT_EQUAL_TO_VALUE_COLUMN_COUNT_OF_CHILD_OPERATOR_QUOTE_S_T_8E30BAD8 = "outputColumnCount is not equal to value column count of child operator's TsBlock";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_SHOULD_BE_RESOLVED_WHEN_TIMEBOUND_EXISTS_01BE233B = "helperColumnIndex should be resolved when timeBound exists";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type is null";
  public static final String EXCEPTION_LOGICALINDEXNAVIGATION_IS_NULL_3B31CD23 = "logicalIndexNavigation is null";
  public static final String EXCEPTION_OVERRIDING_AGGREGATIONS_FOR_CHILD_THREAD_9C84E47D = "overriding aggregations for child thread";
  public static final String EXCEPTION_SKIP_TO_NAVIGATION_IS_MISSING_FOR_SKIP_TO_ARG_8A303D73 = "skip to navigation is missing for SKIP TO %s";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels is null";
  public static final String EXCEPTION_LOGICAL_OFFSET_MUST_BE_GREATER_THAN_EQUALS_0_COMMA_ACTUAL_COLON_ARG_539807BF = "logical offset must be >= 0, actual: %s";
  public static final String EXCEPTION_CURRENT_ROW_IS_OUT_OF_BOUNDS_OF_THE_MATCH_D8D8B611 = "current row is out of bounds of the match";
  public static final String EXCEPTION_EVALUATIONS_IS_NULL_F3F72380 = "evaluations is null";
  public static final String EXCEPTION_PARTITION_IS_NULL_27FD9756 = "partition is null";
  public static final String EXCEPTION_LABELNAMES_IS_NULL_EEFDB807 = "labelNames is null";
  public static final String EXCEPTION_BOUNDSIGNATURE_IS_NULL_D1077180 = "boundSignature is null";
  public static final String EXCEPTION_ACCUMULATO_IS_NULL_D51F532F = "accumulato is null";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IS_NULL_82EC5B1C = "patternAggregationTracker is null";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IN_INCONSISTENT_STATE_C6FF5294 = "PatternAggregationTracker in inconsistent state";
  public static final String EXCEPTION_SETEVALUATOR_IN_INCONSISTENT_STATE_D5FAA900 = "SetEvaluator in inconsistent state";
  public static final String EXCEPTION_ACCUMULATOR_IS_NULL_EF0C1DFF = "accumulator is null";
  public static final String EXCEPTION_INTERMEDIATETYPE_IS_NULL_D0D9B957 = "intermediateType is null";
  public static final String EXCEPTION_INPUTCHANNELS_IS_NULL_647DA393 = "inputChannels is null";
  public static final String EXCEPTION_ARRAY_IS_NULL_BCF2EEB1 = "array is null";
  public static final String EXCEPTION_USED_SLOTS_COUNT_IS_NEGATIVE_49F017C5 = "used slots count is negative";
  public static final String EXCEPTION_USED_SLOTS_COUNT_EXCEEDS_ARRAY_SIZE_6DCD8C7E = "used slots count exceeds array size";
  public static final String EXCEPTION_ARRAY_INDEX_OUT_OF_BOUNDS_35C8A83F = "array index out of bounds";
  public static final String EXCEPTION_EXCLUSIONS_IS_NULL_336ED5E7 = "exclusions is null";
  public static final String EXCEPTION_INSTRUCTIONS_IS_NULL_A9CDA591 = "instructions is null";
  public static final String EXCEPTION_LABELMAPPING_IS_NULL_535461C7 = "labelMapping is null";
  public static final String EXCEPTION_INVALID_PATTERN_COLON_PERMUTATION_WITH_SINGLE_ELEMENT_DOT_RUN_IRROWPATTERNFLATTE_3FF56E02 = "invalid pattern: permutation with single element. run IrRowPatternFlattener first";
  public static final String EXCEPTION_INVALID_MIN_VALUE_COLON_ARG_8D899A2E = "invalid min value: %s";
  public static final String EXCEPTION_INVALID_RANGE_COLON_LEFT_PAREN_ARG_COMMA_ARG_RIGHT_PAREN_EF9544AA = "invalid range: (%s, %s)";
  public static final String EXCEPTION_NEXTCOLUMN_QUOTE_S_TIME_SHOULD_BE_GREATER_THAN_CURRENT_TIME_334CB115 = "nextColumn's time should be greater than current time";
  public static final String EXCEPTION_HASHCHANNEL_IS_NULL_A5E70672 = "hashChannel is null";
  public static final String EXCEPTION_MARKDISTINCTCHANNELS_IS_NULL_7130C56A = "markDistinctChannels is null";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_SHOULD_BE_BINARYCOLUMN_24E654CD = "intermediate input and output of LAST should be BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_FIRST_BY_SHOULD_BE_3_BE623E5E = "Length of input Column[] for FIRST_BY should be 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_BY_SHOULD_BE_BINARYCOLUMN_F0D867AB = "intermediate input and output of FIRST_BY should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_COVARIANCE_SHOULD_BE_2_786B97E6 = "Input of Covariance should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_SHOULD_BE_BINARYCOLUMN_3B5148FA = "intermediate input and output should be BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LAST_BY_SHOULD_BE_3_D759D45F = "Length of input Column[] for LAST_BY should be 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_BY_SHOULD_BE_BINARYCOLUMN_87700792 = "intermediate input and output of LAST_BY should be BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_UDAF_SHOULD_BE_BINARYCOLUMN_6F28900F = "intermediate input and output of UDAF should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82 = "argument of AVG should be one column";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB = "intermediate input and output of AVG should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_CORRELATION_SHOULD_BE_2_64BB37B5 = "Input of Correlation should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_SHOULD_BE_BINARYCOLUMN_57CE212C = "intermediate input and output of FIRST should be BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_APPROX_COUNT_DISTINCT_SHOULD_BE_BINARYCOLUMN_F444D3BE = "intermediate input and output of APPROX_COUNT_DISTINCT should be BinaryColumn";
  public static final String EXCEPTION_SELECTEDPOSITIONS_IS_NULL_2300C002 = "selectedPositions is null";
  public static final String EXCEPTION_RETAINEDPOSITIONS_IS_NULL_E62F9B0D = "retainedPositions is null";
  public static final String EXCEPTION_POSITIONCOUNT_IS_NEGATIVE_2FACCDCA = "positionCount is negative";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_IS_NEGATIVE_A8A7FDA1 = "selectedPositionCount is negative";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_CANNOT_BE_GREATER_THAN_POSITIONCOUNT_871FF8DA = "selectedPositionCount cannot be greater than positionCount";
  public static final String EXCEPTION_SELECTEDPOSITION_IS_SMALLER_THAN_SELECTEDPOSITIONCOUNT_465B9220 = "selectedPosition is smaller than selectedPositionCount";
  public static final String EXCEPTION_BLOCK_POSITION_COUNT_DOES_NOT_MATCH_CURRENT_POSITION_COUNT_FE4BBE3D = "Block position count does not match current position count";
  public static final String EXCEPTION_GETSELECTEDPOSITIONS_NOT_AVAILABLE_WHEN_IN_SELECTALL_MODE_CBE671D3 = "getSelectedPositions not available when in selectAll mode";
  public static final String EXCEPTION_ARGUMENT_OF_MAX_SHOULD_BE_ONE_COLUMN_FC251F55 = "argument of MAX should be one column";
  public static final String EXCEPTION_ARGUMENT_OF_SUM_SHOULD_BE_ONE_COLUMN_D6E636D1 = "argument of SUM should be one column";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_2BB3E0AF = "intermediate input and output of Mode should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_LEFT_PAREN_RIGHT_PAREN_SHOULD_BE_ONE_COLUMN_9158A9FE = "argument of COUNT(*) should be one column";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "Wrong inputDataTypes size.";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_SHOULD_BE_ONE_COLUMN_906D6B19 = "argument of COUNT should be one column";
  public static final String EXCEPTION_INTERMEDIATE_OUTPUT_OF_REGRESSION_SHOULD_BE_BINARYCOLUMN_E74D77ED = "intermediate output of Regression should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_REGRESSION_SHOULD_BE_2_3893EEE3 = "Input of Regression should be 2";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_MAX_BY_SLASH_MIN_BY_SHOULD_3767E905 = "Length of input Column[] for MAX_BY/MIN_BY should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MAX_BY_SLASH_MIN_BY_SHOULD_BE_BINARYCOLUMN_82B1BE6B = "intermediate input and output of MAX_BY/MIN_BY should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_MIN_SHOULD_BE_ONE_COLUMN_F8AA1E88 = "argument of MIN should be one column";
  public static final String EXCEPTION_STEP_IS_NULL_F83262DA = "step is null";
  public static final String EXCEPTION_MASKCHANNEL_IS_NULL_571AD53D = "maskChannel is null";
  public static final String EXCEPTION_EXPECTED_1_INPUT_CHANNEL_FOR_INTERMEDIATE_AGGREGATION_3190C507 = "expected 1 input channel for intermediate aggregation";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value is null";
  public static final String EXCEPTION_VALUE_MUST_BE_POSITIVE_24FE7959 = "value must be positive";
  public static final String EXCEPTION_NUMBEROFBUCKETS_MUST_BE_A_POWER_OF_2_COMMA_ACTUAL_COLON_ARG_476E74ED = "numberOfBuckets must be a power of 2, actual: %s";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_0D03B323 = "intermediate input and output of MODE should be BinaryColumn";
  public static final String EXCEPTION_AGGREGATION_BUFFER_IS_FULL_233DAF3E = "Aggregation buffer is full";
  public static final String EXCEPTION_PAGE_IS_NULL_4AA19E1C = "page is null";
  public static final String EXCEPTION_SPILL_IS_NOT_SUPPORTED_E6E35549 = "spill is not supported";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LASTBY_SLASH_FIRSTBY_SHOUL_9E836ABA = "Length of input Column[] for LastBy/FirstBy should be 3";
  public static final String EXCEPTION_THE_SIZE_OF_READYQUEUE_CANNOT_BE_NEGATIVE_DOT_01D8D0CB = "The size of readyQueue cannot be negative.";
  public static final String EXCEPTION_THE_SIZE_BETWEEN_WHENTRANSFORMERS_AND_THENTRANSFORMERS_NEEDS_TO_BE_SAME_AC796883 = "the size between whenTransformers and thenTransformers needs to be same";
  public static final String EXCEPTION_EXCEED_MAX_CALL_TIMES_OF_GETCOLUMN_69C77C7E = "Exceed max call times of getColumn";
  public static final String EXCEPTION_TOPK_RUNTIME_FILTER_REQUIRES_SINGLE_TIME_ORDER_BY_7EED6208 =
      "TopK runtime filter requires ORDER BY time on a single INT64 or TIMESTAMP column";
  public static final String EXCEPTION_FILTER_IS_NOT_SUPPORTED_IN_ARG_DOT_FILTER_IS_ARG_DOT_417C4F3C = "Filter is not supported in %s. Filter is %s.";
  public static final String EXCEPTION_NULL_9B41EF67 = "null";
  public static final String EXCEPTION_ARG_MUST_HAVE_JOIN_KEYS_DOT_C24DAB2D = "%s must have join keys.";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEOUTPUTSYMBOL_DOT_527996EC = "Source of SemiJoinNode doesn't contain sourceOutputSymbol.";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEJOINSYMBOL_DOT_32209273 = "Source of SemiJoinNode doesn't contain sourceJoinSymbol.";
  public static final String EXCEPTION_FILTERINGSOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_FILTERINGSOURCEJOINSYMBOL__1B75DDE2 = "FilteringSource of SemiJoinNode doesn't contain filteringSourceJoinSymbol.";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name is null";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function is null";
  public static final String EXCEPTION_PREVIOUS_IS_NULL_056F5D52 = "previous is null";
  public static final String EXCEPTION_PROPERTY_IS_NULL_1C6980FF = "property is null";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern is null";
  public static final String EXCEPTION_CAPTURES_IS_NULL_75EACA5A = "captures is null";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "predicate is null";
  public static final String EXCEPTION_EXPECTEDCLASS_IS_NULL_B619CCC7 = "expectedClass is null";
  public static final String EXCEPTION_EXPECTEDVALUE_CAN_QUOTE_T_BE_NULL_DOT_USE_ISNULL_LEFT_PAREN_RIGHT_PAREN_PATTERN__FC25E374 = "expectedValue can't be null. Use isNull() pattern instead.";
  public static final String EXCEPTION_CAPTURE_IS_NULL_C54AA710 = "capture is null";
  public static final String EXCEPTION_PROPERTYPATTERN_IS_NULL_37185AE7 = "propertyPattern is null";
  public static final String EXCEPTION_TARGET_NODE_MUST_EXIST_2C8D8F3A = "Target node must exist";
  public static final String EXCEPTION_HEAP_MUST_HAVE_AT_LEAST_ONE_NODE_BEFORE_STARTING_TRAVERSAL_A1E65C70 = "heap must have at least one node before starting traversal";
  public static final String EXCEPTION_NEW_CHILD_SHOULDN_QUOTE_T_EXIST_YET_1EB3B129 = "New child shouldn't exist yet";
  public static final String EXCEPTION_ROWID_AND_UNIQUEVALUE_MASK_OVERLAPS_9A092E09 = "RowId and uniqueValue mask overlaps";

  // --- Execution ---

  public static final String ERROR_SETTING_FUTURE_STATE_FOR =
      "Error setting future state for {}";
  public static final String ERROR_NOTIFYING_STATE_CHANGE_LISTENER_FOR =
      "Error notifying state change listener for {}";
  public static final String SERVER_IS_SHUTTING_DOWN =
      "Server is shutting down";

  public static final String EXCEPTION_PERCENTAGE_SHOULD_BE_IN_0_1_GOT_7A2C2F83 =
      "percentage should be in [0,1], got ";
  public static final String EXCEPTION_PERCENTILE_REQUIRES_2_ARGUMENTS_BUT_GOT_ARG_F3F1882F =
      "PERCENTILE requires 2 arguments, but got %d";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_PERCENTILE_AGGREGATION_ARG_E0C2513D =
      "Unsupported data type in Percentile Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_PERCENTILE_AGGREGATION_ARG_8ACED24D =
      "Unsupported data type in PERCENTILE Aggregation: %s";
  public static final String EXCEPTION_PERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_66308C79 =
      "PercentileAccumulator does not support statistics";

}
