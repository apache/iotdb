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

  // --- Execution ---

  public static final String ERROR_SETTING_FUTURE_STATE_FOR =
      "Error setting future state for {}";
  public static final String ERROR_NOTIFYING_STATE_CHANGE_LISTENER_FOR =
      "Error notifying state change listener for {}";
  public static final String SERVER_IS_SHUTTING_DOWN =
      "Server is shutting down";

}
