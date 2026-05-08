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

package org.apache.iotdb.commons.i18n;

public final class QueryMessages {

  // ======================== PlanNode ========================

  public static final String PLAN_NODE_NOT_SUPPORT_GET_TYPE =
      "This planNode does not support getType().";
  public static final String PLAN_NODE_CANNOT_CREATE_SUB_NODE =
      "Can't create subNode for %s";
  public static final String PLAN_NODE_CHILD_COUNT_INCORRECT =
      "Child count is not correct for PlanNode. Expected: %d, Value: %d";
  public static final String PLAN_NODE_SERIALIZE_ERROR =
      "Unexpected error occurs when serializing writePlanNode.";
  public static final String PLAN_NODE_NOT_SUPPORT_GET_OUTPUT_SYMBOLS =
      "This planNode does not support getOutputSymbols().";
  public static final String TWO_CHILD_NODE_EXCEEDS_LIMIT =
      "This node doesn't support more than two children";

  // ======================== CommonPlanNodeDeserializer ========================

  public static final String NOT_SUPPORTED_FOR_COMMON_DESERIALIZER =
      "Not supported for CommonPlanNodeDeserializer";
  public static final String INVALID_NODE_TYPE = "Invalid node type: %s";

  // ======================== PlanNodeType ========================

  public static final String MULTIPLE_PLAN_NODE_DESERIALIZER_PROVIDER =
      "Multiple IPlanNodeDeserializerProvider found";

  // ======================== Row Pattern ========================

  public static final String UNKNOWN_IR_ROW_PATTERN_TYPE = "Unknown IrRowPattern type";
  public static final String UNKNOWN_VALUE_POINTER_TYPE = "Unknown ValuePointer type";
  public static final String UNSUPPORTED_IR_NODE_TYPE =
      "unsupported node type: %s";

  // ======================== SQL AST ========================

  public static final String NOT_YET_IMPLEMENTED = "not yet implemented: %s";
  public static final String NOT_YET_IMPLEMENTED_VISIT =
      "not yet implemented: %s.visit%s";
  public static final String GET_EXPRESSION_TYPE_NOT_IMPLEMENTED =
      "getExpressionType is not implemented yet: %s";
  public static final String INVALID_EXPRESSION_TYPE = "Invalid expression type: %s";
  public static final String ONLY_TABLE_SUBQUERY_SUPPORTED =
      "Only TableSubquery is supported now";
  public static final String FLOAT_LITERAL_CANNOT_CREATE_FROM_LOCATION =
      "Currently the FloatLiteral cannot be created from NodeLocation";
  public static final String COLUMNS_SHOULD_BE_EXPANDED =
      "Columns should be expanded in Analyze stage";

  // ======================== Parsing ========================

  public static final String BINARY_LITERAL_HEX_ONLY =
      "Binary literal can only contain hexadecimal digits";
  public static final String BINARY_LITERAL_EVEN_DIGITS =
      "Binary literal must contain an even number of digits";
  public static final String GENERIC_LITERAL_NO_SPACE =
      "Spaces are not allowed between 'X' and the starting quote of a binary literal";
  public static final String INVALID_NUMERIC_LITERAL = "Invalid numeric literal: %s";

  // ======================== Comparison / Logical ========================

  public static final String UNSUPPORTED_COMPARISON = "Unsupported comparison: %s";
  public static final String UNSUPPORTED_LOGICAL_EXPRESSION_TYPE =
      "Unsupported logical expression type: %s";

  // ======================== SQL Formatter ========================

  public static final String UNKNOWN_FILL_METHOD = "Unknown fill method: %s";
  public static final String UNKNOWN_JOIN_CRITERIA = "unknown join criteria: %s";
  public static final String UNSUPPORTED_RELATION = "unsupported relation: %s";
  public static final String UNEXPECTED_ROWS_PER_MATCH =
      "unexpected rowsPerMatch: %s";
  public static final String UNEXPECTED_SKIP_TO = "unexpected skipTo: %s";
  public static final String UNKNOWN_SIGN = "Unknown sign: %s";
  public static final String UNSUPPORTED_FRAME_TYPE = "Unsupported frame type: %s";
  // ======================== Join ========================

  public static final String INVALID_OPERATOR_TYPE = "Invalid operator type: %s";
  public static final String UNSUPPORTED_JOIN_TYPE = "Unsupported join type: %s";

  // ======================== Type System ========================

  public static final String UNSUPPORTED_DATA_TYPE = "Unsupported DataType: %s";
  public static final String INVALID_TYPE_PARAMETER = "Invalid type parameter: %s";
  public static final String UNSUPPORTED_TYPE_PARAMETER_KIND =
      "Unsupported type parameter kind: %s";
  public static final String EXPECTED_ALL_TYPE_SIGNATURES =
      "Expected all parameters to be TypeSignatures but [%s] was found";
  public static final String PARAMETER_KIND_MISMATCH =
      "ParameterKind is [%s] but expected [%s]";
  public static final String UNEXPECTED_PARAMETER_KIND = "Unexpected parameter kind: %s";
  public static final String UNSUPPORTED_COLUMN_TYPE = "Unsupported column type: %s";

  // ======================== Function ========================

  public static final String UNKNOWN_FUNCTION = "Unknown function: %s";
  public static final String UNSUPPORTED_TABLE_FUNCTION = "Unsupported table function: %s";
  public static final String FUNCTION_ID_MUST_NOT_BE_EMPTY = "id must not be empty";
  public static final String FUNCTION_ID_MUST_BE_LOWERCASE = "id must be lowercase";
  public static final String FUNCTION_ID_MUST_NOT_CONTAIN_AT = "id must not contain '@'";
  public static final String VARIADIC_BOUND_MUST_BE_ROW =
      "variadicBound must be row but is %s";
  public static final String BUILTIN_SCALAR_FUNCTION_CANNOT_BE_INVOKED =
      "BuiltinScalarFunctio %s cannot be invoked.";
  public static final String OPERATOR_TYPE_CANNOT_BE_INVOKED =
      "OperatorType %s cannot be invoked.";
  public static final String INVALID_AGGREGATION_FUNCTION =
      "Invalid Aggregation function: %s";

  // ======================== Table Function (TVF) ========================

  public static final String INVALID_OPTIONS = "Invalid options: %s";
  public static final String COLUMN_TYPE_NOT_ALLOWED =
      "The type of the column [%s] is [%s], only INT32, INT64, FLOAT, DOUBLE is allowed";
  public static final String PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY =
      "%s should never be null or empty";
  public static final String PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY_DOT =
      "%s should never be null or empty.";
  public static final String PARAM_SHOULD_BE_GREATER_THAN_ZERO =
      "%s should be greater than 0";
  public static final String TARGETS_TOO_MANY_COLUMNS =
      "%s should not contain more than one target column, found [%s] target columns.";
  public static final String INPUT_END_TIME_LESS_THAN_START_TIME =
      "input end time should never less than start time, start time is %s, end time is %s";
  public static final String OUTPUT_START_TIME_SHOULD_BE_GREATER =
      "The %s should be greater than the maximum timestamp of target time series. Expected greater than [%s] but found [%s].";
  public static final String TIME_COLUMN_SHOULD_NOT_BE_NULL =
      "Time column should never be null";
  public static final String NON_PARTITIONING_PASS_THROUGH =
      "non-partitioning pass-through column for non-pass-through source of a table function";

  // ======================== Pattern Match TVF ========================

  public static final String SMOOTH_MUST_BE_NON_NEGATIVE =
      "smooth must be a non-negative number, but got: %s";
  public static final String THRESHOLD_MUST_BE_NON_NEGATIVE =
      "threshold must be a non-negative number, but got: %s";
  public static final String WIDTH_MUST_BE_NON_NEGATIVE =
      "width must be a non-negative number, but got: %s";
  public static final String HEIGHT_MUST_BE_NON_NEGATIVE =
      "height must be a non-negative number, but got: %s";
  public static final String INVALID_PATTERN_MISSING_REPEAT_SIGN =
      "Invalid pattern: %s, missing repeat sign after '}'";
  public static final String INVALID_PATTERN_MUST_CONTAIN_DATA_POINTS =
      "Invalid pattern: '%s'. Pattern must contain at least two numeric data points separated by commas, e.g., '1,2,3'";

  // ======================== Forecast / Classify TVF ========================

  public static final String SERIALIZE_FORECAST_HANDLE_ERROR =
      "Error occurred while serializing ForecastTableFunctionHandle: %s";
  public static final String FORECAST_EXECUTION_ERROR =
      "Error occurred while executing forecast:[%s]";
  public static final String CLASSIFY_EXECUTION_ERROR =
      "Error occurred while executing classify:[%s]";
  public static final String MODEL_OUTPUT_COLUMN_MISMATCH =
      "Model %s output %s columns, doesn't equal to specified %s";
  public static final String MODEL_OUTPUT_LENGTH_MISMATCH =
      "Model %s output length is %s, doesn't equal to specified %s";

  // ======================== UDF Management ========================

  public static final String UDF_INFORMATION_NOT_AVAILABLE = "UDFInformation is not available";
  public static final String UDF_REGISTER_CONFLICT_BUILTIN =
      "Failed to register UDF %s(%s), because the given function name conflicts with the built-in function name";
  public static final String UDF_REGISTER_JAR_MD5_CONFLICT =
      "Failed to register function %s(%s), because existed md5 of jar file for function %s is different from the new jar file. ";
  public static final String UDF_REGISTER_INSTANCE_FAILED =
      "Failed to register UDF %s(%s), because its instance can not be constructed successfully. Exception: %s";
  public static final String UDF_CLASS_TYPE_UNSUPPORTED =
      "Unsupported UDF class type. Only UDF and SQLFunction are supported.";
  public static final String UDF_REFLECT_NOT_REGISTERED =
      "Failed to reflect UDF instance, because UDF %s has not been registered.";
  public static final String UDF_REFLECT_INSTANCE_FAILED =
      "Failed to reflect UDF %s(%s) instance, because %s";
  public static final String AI_NODE_SERVICE_NOT_AVAILABLE =
      "Table function AINode service is not available in current node";
  // ======================== UDTF Forecast ========================

  public static final String INPUT_DATA_TYPE_NOT_SUPPORTED =
      "Input data type %s is not supported, only %s are allowed.";
  public static final String MODEL_ID_MUST_BE_PROVIDED =
      "MODEL_ID parameter must be provided and cannot be empty.";
  public static final String UNSUPPORTED_DATA_TYPE_FOR_UDF = "Unsupported data type %s";
  public static final String FORECAST_FAILED =
      "Forecast failed due to %d %s";
  public static final String FORECAST_RESULT_LENGTH_MISMATCH =
      "The forecast result length %d does not match the expected output length %d";
  public static final String FORECAST_RESULT_SINGLE_COLUMN =
      "The forecast result should have only one value column, but got %d";

  // ======================== TableSchema ========================

  public static final String TABLE_COLUMN_NAME_DUPLICATED =
      "Columns in table shall not share the same name %s.";
  public static final String TSFILE_TABLE_SCHEMA_CONVERT_FAILED =
      "Cannot convert tsfile table schema to iotdb table schema, table name: {}, tsfile table schema: {}";

  // ======================== DateTime ========================

  public static final String TIMESTAMP_OVERFLOW =
      "Timestamp overflow, Millisecond: %s , Timestamp precision: %s";
  public static final String EXTRACT_TIMESTAMP_MS_PART_NULL =
      "ExtractTimestampMsPart is null";
  public static final String EXTRACT_TIMESTAMP_US_PART_NULL =
      "ExtractTimestampUsPart is null";
  public static final String EXTRACT_TIMESTAMP_NS_PART_NULL =
      "ExtractTimestampNsPart is null";
  public static final String DATETIME_CONVERT_FAILED =
      "Failed to convert %s to millisecond, zone offset is %s, please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00";
  public static final String DATETIME_TIME_REGION_NOT_SUPPORTED =
      "%s with [time-region] at end is not supported now, please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00";
  public static final String UNSUPPORTED_TIME_PRECISION =
      "not supported time_precision: %s";
  public static final String TIMESTAMP_UNEXPECTEDLY_LARGE =
      "The timestamp is unexpectedly large, you may forget to set the timestamp precision."
          + "Current system timestamp precision is %s, "
          + "please check whether the timestamp %s is correct."
          + "If you insist to insert this timestamp, please set timestamp_precision_check_enabled=false and restart the server.";

  // ======================== SqlDialect ========================

  public static final String UNKNOWN_SQL_DIALECT = "Unknown sql dialect: %s";

  // ======================== Hash / GroupBy ========================

  public static final String MEMORY_FOR_FLAT_HASH_NOT_ENOUGH =
      "Memory for flatHash is not enough";
  public static final String LONG_OVERFLOW = "long overflow";
  public static final String INTEGER_OVERFLOW = "integer overflow";
  public static final String INVALID_INPUT_DESC_LENGTH_LT_8 =
      "Invalid input: desc.length - offset < 8";

  private QueryMessages() {}

  public static final String TIME_PRECISION_MUST_BE_ONE_OF = "time precision must be one of: h,m,s,ms,u,n";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_4 = "Invalid input: bytes.length - offset < 4";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_8 = "Invalid input: bytes.length - offset < 8";
  public static final String INVALID_INPUT_DESC_OFFSET_LT_4 = "Invalid input: desc.length - offset < 4";
  public static final String SIZE_OF_HASH_TABLE_CANNOT_EXCEED = "Size of hash table cannot exceed 1 billion entries";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_APPEND = "NoChannelGroupByHash does not support appendValuesTo";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_RAW_HASH = "NoChannelGroupByHash does not support getRawHash";
  public static final String MULTIPLE_TABLE_FUNCTION_AI_NODE_PROVIDER = "Multiple ITableFunctionAINodeServiceProvider found";
}
