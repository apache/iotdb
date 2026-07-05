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
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNKNOWN_TYPE_BC7EAB78 = "Unknown type: ";
  public static final String EXCEPTION_UNEXPECTED_ROWSPERMATCH_F9B931C9 = "unexpected rowsPerMatch: ";
  public static final String EXCEPTION_UNSUPPORTED_FILL_METHOD_0809912A = "Unsupported fill method: ";
  public static final String EXCEPTION_EXCEPTNODE_SHOULD_NEVER_SERIALIZED_CURRENT_VERSION_6F5B4EE0 = "ExceptNode should never be serialized in current version";
  public static final String EXCEPTION_EXCEPTNODE_SHOULD_NEVER_DESERIALIZED_CURRENT_VERSION_690C18F0 = "ExceptNode should never be deserialized in current version";
  public static final String EXCEPTION_INTERSECTNODE_SHOULD_NEVER_SERIALIZED_CURRENT_VERSION_68D2C0D3 = "IntersectNode should never be serialized in current version";
  public static final String EXCEPTION_INTERSECTNODE_SHOULD_NEVER_DESERIALIZED_CURRENT_VERSION_A3614371 = "IntersectNode should never be deserialized in current version";
  public static final String EXCEPTION_COMMA_50AD1C01 = ", ";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_OUTERQUERYSCOPE_IS_NULL_D5305685 = "outerQueryScope is null";
  public static final String EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68 = "warningCollector is null";
  public static final String EXCEPTION_UPDATEKIND_IS_NULL_6CA83291 = "updateKind is null";
  public static final String EXCEPTION_EXPECTED_ARG_FIELDS_COMMA_GOT_ARG_498063F2 = "Expected %s fields, got %s";
  public static final String EXCEPTION_MISSING_TABLE_ARGUMENT_COLON_ARG_4F1E9F4E = "Missing table argument: %s";
  public static final String EXCEPTION_TABLE_ARGUMENT_SPECIFICATION_CANNOT_HAVE_A_DEFAULT_VALUE_DOT_009A02CA = "Table argument specification cannot have a default value.";
  public static final String EXCEPTION_MISSING_DEFAULT_VALUE_FOR_SCALAR_ARGUMENT_COLON_ARG_8AF70F38 = "Missing default value for scalar argument: %s";
  public static final String EXCEPTION_RESULT_SCOPE_SHOULD_HAVE_OUTER_QUERY_SCOPE_EQUAL_WITH_PARAMETER_OUTER_QUERY_SCOP_BE9CDB22 = "result scope should have outer query scope equal with parameter outer query scope";
  public static final String EXCEPTION_RETURN_SCOPE_SHOULD_HAVE_CONTEXT_SCOPE_AS_ONE_OF_ITS_ANCESTORS_BBBFFE3E = "return scope should have context scope as one of its ancestors";
  public static final String EXCEPTION_MISSING_RELATION_ALIAS_437EDF9C = "missing relation alias";
  public static final String EXCEPTION_MISMATCHED_ALIASES_A6D3F8CE = "mismatched aliases";
  public static final String EXCEPTION_UNEXPECTED_OFFSET_ROWCOUNT_COLON_977E305C = "unexpected OFFSET rowCount: ";
  public static final String EXCEPTION_INVALID_LIMIT_NODE_TYPE_DOT_EXPECTED_COLON_LIMIT_DOT_ACTUAL_COLON_ARG_5BCD1C76 = "Invalid limit node type. Expected: Limit. Actual: %s";
  public static final String EXCEPTION_UNEXPECTED_LIMIT_ROWCOUNT_COLON_CFF154F0 = "unexpected LIMIT rowCount: ";
  public static final String EXCEPTION_NON_MINUS_EMPTY_ORDERBYEXPRESSIONS_LIST_WITHOUT_ORDERBYSCOPE_PROVIDED_240DDD64 = "non-empty orderByExpressions list without orderByScope provided";
  public static final String EXCEPTION_OUTPUT_FIELDS_IS_NULL_FOR_SELECT_ITEM_ARG_56AFEC51 = "output fields is null for select item %s";
  public static final String EXCEPTION_NODE_IS_NULL_C1479F4A = "node is null";
  public static final String EXCEPTION_COLUMNS_SHOULDN_QUOTE_T_BE_EMPTY_HERE_34E44AAA = "columns shouldn't be empty here";
  public static final String EXCEPTION_GREATER_THAN_0B37D87A = " > ";
  public static final String EXCEPTION_GROUPID_OUT_OF_RANGE_8B10E54B = "groupId out of range";
  public static final String EXCEPTION_MINIMUMREQUIREDCAPACITY_MUST_BE_POSITIVE_81638118 = "minimumRequiredCapacity must be positive";
  public static final String EXCEPTION_EXPECTEDSIZE_MUST_BE_GREATER_THAN_ZERO_DA2E0345 = "expectedSize must be greater than zero";
  public static final String EXCEPTION_POSITION_COUNT_OUT_OF_BOUND_5513847E = "position count out of bound";
  public static final String EXCEPTION_ID_IS_NULL_9D5D27B1 = "id is null";
  public static final String EXCEPTION_NEWCHILDREN_IS_NOT_EMPTY_170FCE18 = "newChildren is not empty";
  public static final String EXCEPTION_ORDERBY_IS_NULL_AA2494DE = "orderBy is null";
  public static final String EXCEPTION_ORDERINGS_IS_NULL_59B1C097 = "orderings is null";
  public static final String EXCEPTION_ORDERBY_IS_EMPTY_963405E0 = "orderBy is empty";
  public static final String EXCEPTION_ORDERBY_KEYS_AND_ORDERINGS_DON_QUOTE_T_MATCH_E0334493 = "orderBy keys and orderings don't match";
  public static final String EXCEPTION_NO_ORDERING_FOR_SYMBOL_COLON_ARG_F54E70FC = "No ordering for symbol: %s";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name is null";
  public static final String EXCEPTION_UNEXPECTED_EXPRESSION_COLON_ARG_8E4AC833 = "Unexpected expression: %s";
  public static final String EXCEPTION_ORDERINGSCHEME_IS_NULL_4D4D2F6F = "orderingScheme is null";
  public static final String EXCEPTION_ASSIGNMENTS_IS_NULL_1FD6142D = "assignments is null";
  public static final String EXCEPTION_OUTPUT_IS_NULL_3CDA316E = "output is null";
  public static final String EXCEPTION_EXPRESSION_IS_NULL_16C079B5 = "expression is null";
  public static final String EXCEPTION_SYMBOL_ARG_ALREADY_HAS_ASSIGNMENT_ARG_COMMA_WHILE_ADDING_ARG_EE8CADA3 = "Symbol %s already has assignment %s, while adding %s";
  public static final String EXCEPTION_TYPEVARIABLECONSTRAINTS_IS_NULL_0A86DA34 = "typeVariableConstraints is null";
  public static final String EXCEPTION_LONGVARIABLECONSTRAINTS_IS_NULL_51F80E3C = "longVariableConstraints is null";
  public static final String EXCEPTION_RETURNTYPE_IS_NULL_07C7C6A5 = "returnType is null";
  public static final String EXCEPTION_ARGUMENTTYPES_IS_NULL_1E377BFD = "argumentTypes is null";
  public static final String EXCEPTION_TYPEVARIABLECONSTRAINT_IS_NULL_18B97042 = "typeVariableConstraint is null";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type is null";
  public static final String EXCEPTION_VARIADICBOUND_IS_NULL_33A6BCC2 = "variadicBound is null";
  public static final String EXCEPTION_CASTABLETO_IS_NULL_0F2A5B36 = "castableTo is null";
  public static final String EXCEPTION_CASTABLEFROM_IS_NULL_DE5158C7 = "castableFrom is null";
  public static final String EXCEPTION_FUNCTIONNAME_IS_NULL_0818CBC7 = "functionName is null";
  public static final String EXCEPTION_RESOLVEDFUNCTION_IS_NULL_81B5B93A = "resolvedFunction is null";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value is null";
  public static final String EXCEPTION_VALUES_IS_NULL_F1D7D3D8 = "values is null";
  public static final String EXCEPTION_FIELDNAME_IS_NULL_DEB1CA0C = "fieldName is null";
  public static final String EXCEPTION_TYPESIGNATURE_IS_NULL_E8B47305 = "typeSignature is null";
  public static final String EXCEPTION_BASE_TYPE_NAME_CANNOT_BE_A_TYPE_VARIABLE_71E810A9 = "Base type name cannot be a type variable";
  public static final String EXCEPTION_BASE_IS_NULL_AC445AD0 = "base is null";
  public static final String EXCEPTION_BASE_IS_EMPTY_E86FBC3A = "base is empty";
  public static final String EXCEPTION_BAD_CHARACTERS_IN_BASE_TYPE_COLON_ARG_FA811786 = "Bad characters in base type: %s";
  public static final String EXCEPTION_PARAMETERS_IS_NULL_418C7892 = "parameters is null";
  public static final String EXCEPTION_PARAMETERS_FOR_ROW_TYPE_MUST_BE_NAMED_TYPE_PARAMETERS_6AADD078 = "Parameters for ROW type must be NAMED_TYPE parameters";
  public static final String EXCEPTION_KIND_IS_NULL_8C13BAB2 = "kind is null";
  public static final String EXCEPTION_COLUMNCATEGORY_IS_NULL_0075924B = "columnCategory is null";
  public static final String EXCEPTION_PROPERTIES_IS_NULL_57B88B49 = "properties is null";
  public static final String EXCEPTION_COMMENT_IS_NULL_0AD46118 = "comment is null";
  public static final String EXCEPTION_EXTRAINFO_IS_NULL_43AE989F = "extraInfo is null";
  public static final String EXCEPTION_SIGNATURE_IS_NULL_CA3D8772 = "signature is null";
  public static final String EXCEPTION_FUNCTIONID_IS_NULL_F91F8E89 = "functionId is null";
  public static final String EXCEPTION_FUNCTIONKIND_IS_NULL_EDF86E36 = "functionKind is null";
  public static final String EXCEPTION_FUNCTIONNULLABILITY_IS_NULL_6EF52FB4 = "functionNullability is null";
  public static final String EXCEPTION_INVALID_NAME_ARG_AD3FA0BF = "Invalid name %s";
  public static final String EXCEPTION_INVALID_NAME_COLON_LEFT_BRACKET_ARG_RIGHT_BRACKET_C46F15A4 = "Invalid name: [%s]";
  public static final String EXCEPTION_ARGUMENTNULLABLE_IS_NULL_0CC221D3 = "argumentNullable is null";
  public static final String EXCEPTION_LITERALFORMATTER_IS_NULL_3F4F4A2B = "literalFormatter is null";
  public static final String EXCEPTION_SYMBOLREFERENCEFORMATTER_IS_NULL_B9B540EC = "symbolReferenceFormatter is null";
  public static final String EXCEPTION_BUILDER_IS_NULL_ADE64E9B = "builder is null";
  public static final String EXCEPTION_VISITEXPRESSION_SHOULD_ONLY_BE_CALLED_AT_ROOT_93E4AAD4 = "visitExpression should only be called at root";
  public static final String EXCEPTION_VISITROWPATTERN_SHOULD_ONLY_BE_CALLED_AT_ROOT_9DE9F574 = "visitRowPattern should only be called at root";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_AFTER_MATCH_SKIP_TO_LAST_82A12A21 = "missing identifier in AFTER MATCH SKIP TO LAST";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_AFTER_MATCH_SKIP_TO_FIRST_F988B839 = "missing identifier in AFTER MATCH SKIP TO FIRST";
  public static final String EXCEPTION_LINE_MUST_BE_GREATER_THAN_0_8D2C1802 = "line must be > 0";
  public static final String EXCEPTION_COLUMN_MUST_BE_GREATER_THAN_0_2481C561 = "column must be > 0";
  public static final String EXCEPTION_LEFT_IS_NULL_2C1080C5 = "left is null";
  public static final String EXCEPTION_RIGHT_IS_NULL_97BD6491 = "right is null";
  public static final String EXCEPTION_LOCATION_IS_NULL_F134D388 = "location is null";
  public static final String EXCEPTION_CRITERIA_IS_NULL_2996D1A3 = "criteria is null";
  public static final String EXCEPTION_NO_JOIN_CRITERIA_SPECIFIED_44DED0B9 = "No join criteria specified";
  public static final String EXCEPTION_ARG_JOIN_CANNOT_HAVE_JOIN_CRITERIA_3B23E0D1 = "%s join cannot have join criteria";
  public static final String EXCEPTION_SELECTITEMS_16EC72CF = "selectItems";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function is null";
  public static final String EXCEPTION_PRECISION_IS_NULL_73E48139 = "precision is null";
  public static final String EXCEPTION_OPERAND_IS_NULL_D0182140 = "operand is null";
  public static final String EXCEPTION_WHENCLAUSES_IS_NULL_140535CF = "whenClauses is null";
  public static final String EXCEPTION_DEFAULTVALUE_IS_NULL_B1C8490D = "defaultValue is null";
  public static final String EXCEPTION_THE_LENGTH_OF_SIMPLECASEEXPRESSION_QUOTE_S_WHENCLAUSES_MUST_GREATER_THAN_0_5D2963B9 = "the length of SimpleCaseExpression's whenClauses must greater than 0";
  public static final String EXCEPTION_SETS_IS_NULL_C9A2FA67 = "sets is null";
  public static final String EXCEPTION_GROUPING_SETS_CANNOT_BE_EMPTY_8E0BE7DB = "grouping sets cannot be empty";
  public static final String EXCEPTION_IDENTIFIERS_IS_NULL_72CDC3BE = "identifiers is null";
  public static final String EXCEPTION_IDENTIFIERS_IS_EMPTY_410ED65C = "identifiers is empty";
  public static final String EXCEPTION_QUERIES_IS_NULL_94399651 = "queries is null";
  public static final String EXCEPTION_QUERIES_IS_EMPTY_FE480BBC = "queries is empty";
  public static final String EXCEPTION_VALUE_IS_EMPTY_A42300DD = "value is empty";
  public static final String EXCEPTION_VALUE_CONTAINS_ILLEGAL_CHARACTERS_COLON_ARG_C6DBFEBA = "value contains illegal characters: %s";
  public static final String EXCEPTION_RELATIONS_IS_NULL_0275CA3C = "relations is null";
  public static final String EXCEPTION_FILLMETHOD_IS_NULL_2E5A83E6 = "fillMethod is null";
  public static final String EXCEPTION_RELATION_IS_NULL_890596ED = "relation is null";
  public static final String EXCEPTION_ALIAS_IS_NULL_B2ED729A = "alias is null";
  public static final String EXCEPTION_RESULT_IS_NULL_031E2F89 = "result is null";
  public static final String EXCEPTION_VALUELIST_IS_NULL_9271DC51 = "valueList is null";
  public static final String EXCEPTION_ROWS_IS_NULL_B8BF74DE = "rows is null";
  public static final String EXCEPTION_SORTITEMS_IS_NULL_DD277CDD = "sortItems is null";
  public static final String EXCEPTION_SORTITEMS_SHOULD_NOT_BE_EMPTY_78C26791 = "sortItems should not be empty";
  public static final String EXCEPTION_FIELD_IS_NULL_80E8CE23 = "field is null";
  public static final String EXCEPTION_EXISTINGWINDOWNAME_IS_NULL_5D67FD72 = "existingWindowName is null";
  public static final String EXCEPTION_PARTITIONBY_IS_NULL_84791B6B = "partitionBy is null";
  public static final String EXCEPTION_FRAME_IS_NULL_5A92D609 = "frame is null";
  public static final String EXCEPTION_CONDITION_IS_NULL_327EBA55 = "condition is null";
  public static final String EXCEPTION_TRUEVALUE_IS_NULL_99C09BA5 = "trueValue is null";
  public static final String EXCEPTION_PATTERNS_IS_NULL_7E0DDDF0 = "patterns is null";
  public static final String EXCEPTION_PATTERNS_LIST_IS_EMPTY_D4D1802F = "patterns list is empty";
  public static final String EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2 = "arguments is null";
  public static final String EXCEPTION_TARGET_IS_NULL_240F0372 = "target is null";
  public static final String EXCEPTION_ALIASES_IS_NULL_C481EE59 = "aliases is null";
  public static final String EXCEPTION_SUBQUERY_IS_NULL_F0E1842F = "subquery is null";
  public static final String EXCEPTION_INPUT_IS_NULL_EE7EADB0 = "input is null";
  public static final String EXCEPTION_MEASURES_IS_NULL_EC9D2431 = "measures is null";
  public static final String EXCEPTION_ROWSPERMATCH_IS_NULL_661EA4A9 = "rowsPerMatch is null";
  public static final String EXCEPTION_AFTERMATCHSKIPTO_IS_NULL_7623C3C0 = "afterMatchSkipTo is null";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern is null";
  public static final String EXCEPTION_SUBSETS_IS_NULL_AF77CD01 = "subsets is null";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_NULL_5F7B8ED4 = "variableDefinitions is null";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_EMPTY_9E324869 = "variableDefinitions is empty";
  public static final String EXCEPTION_UNEXPECTED_ROWCOUNT_CLASS_COLON_ARG_170FC7CD = "unexpected rowCount class: %s";
  public static final String EXCEPTION_ITEMS_IS_NULL_32A25379 = "items is null";
  public static final String EXCEPTION_OPERANDS_IS_NULL_135465B7 = "operands is null";
  public static final String EXCEPTION_MUST_HAVE_AT_LEAST_TWO_OPERANDS_CE7B0DCB = "must have at least two operands";
  public static final String EXCEPTION_LINE_MUST_BE_AT_LEAST_ONE_COMMA_GOT_COLON_ARG_6BA7A99C = "line must be at least one, got: %s";
  public static final String EXCEPTION_COLUMN_MUST_BE_AT_LEAST_ONE_COMMA_GOT_COLON_ARG_4A529240 = "column must be at least one, got: %s";
  public static final String EXCEPTION_QUERY_IS_NULL_689B7978 = "query is null";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator is null";
  public static final String EXCEPTION_EXPECTED_AT_LEAST_2_TERMS_87CD9010 = "Expected at least 2 terms";
  public static final String EXCEPTION_SPECIFICATION_IS_NULL_BB4AA029 = "specification is null";
  public static final String EXCEPTION_TRIMSOURCE_IS_NULL_C7E6B71D = "trimSource is null";
  public static final String EXCEPTION_TRIMCHARACTER_IS_NULL_4036C3D8 = "trimCharacter is null";
  public static final String EXCEPTION_POSITION_IS_NULL_5B14C61B = "position is null";
  public static final String EXCEPTION_IDENTIFIER_IS_NULL_8F8171C8 = "identifier is null";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_SKIP_TO_ARG_33E1A7C3 = "missing identifier in SKIP TO %s";
  public static final String EXCEPTION_UNEXPECTED_IDENTIFIER_IN_SKIP_TO_ARG_58438AFC = "unexpected identifier in SKIP TO %s";
  public static final String EXCEPTION_FIRST_IS_NULL_DC679129 = "first is null";
  public static final String EXCEPTION_ORIGINALPARTS_IS_NULL_EA9B01F3 = "originalParts is null";
  public static final String EXCEPTION_ORIGINALPARTS_IS_EMPTY_0D1EFAC4 = "originalParts is empty";
  public static final String EXCEPTION_MIN_IS_NULL_B80144C1 = "min is null";
  public static final String EXCEPTION_MAX_IS_NULL_CCABB3BF = "max is null";
  public static final String EXCEPTION_TABLE_IS_NULL_8DDD9098 = "table is null";
  public static final String EXCEPTION_WITH_IS_NULL_20C2A1AE = "with is null";
  public static final String EXCEPTION_QUERYBODY_IS_NULL_E3EB26CA = "queryBody is null";
  public static final String EXCEPTION_FILL_IS_NULL_3548C13D = "fill is null";
  public static final String EXCEPTION_OFFSET_IS_NULL_82BA6093 = "offset is null";
  public static final String EXCEPTION_LIMIT_IS_NULL_2EE9FA0F = "limit is null";
  public static final String EXCEPTION_LIMIT_MUST_BE_OPTIONAL_OF_EITHER_FETCHFIRST_OR_LIMIT_TYPE_0636CAA9 = "limit must be optional of either FetchFirst or Limit type";
  public static final String EXCEPTION_ATLEAST_IS_NULL_2FE8D701 = "atLeast is null";
  public static final String EXCEPTION_ATMOST_IS_NULL_778B3B3A = "atMost is null";
  public static final String EXCEPTION_VALUES_CANNOT_BE_EMPTY_F18F863D = "values cannot be empty";
  public static final String EXCEPTION_SELECT_IS_NULL_B45C440F = "select is null";
  public static final String EXCEPTION_FROM_IS_NULL_2651BF57 = "from is null";
  public static final String EXCEPTION_WHERE_IS_NULL_A1A3FCBC = "where is null";
  public static final String EXCEPTION_GROUPBY_IS_NULL_4CF478F2 = "groupBy is null";
  public static final String EXCEPTION_HAVING_IS_NULL_700DC5B0 = "having is null";
  public static final String EXCEPTION_WINDOWS_IS_NULL_549D8892 = "windows is null";
  public static final String EXCEPTION_FIELDINDEX_MUST_BE_GREATER_THAN_EQUALS_0_4138C39B = "fieldIndex must be >= 0";
  public static final String EXCEPTION_WINDOW_IS_NULL_7532B76A = "window is null";
  public static final String EXCEPTION_ESCAPE_IS_NULL_C9E4F69B = "escape is null";
  public static final String EXCEPTION_PROCESSINGMODE_IS_NULL_F4D8AE91 = "processingMode is null";
  public static final String EXCEPTION_SIMPLEGROUPBYEXPRESSIONS_IS_NULL_3F6A1ECC = "simpleGroupByExpressions is null";
  public static final String EXCEPTION_THE_LENGTH_OF_SEARCHEDCASEEXPRESSION_QUOTE_S_WHENCLAUSES_MUST_GREATER_THAN_0_D710DBF6 = "the length of SearchedCaseExpression's whenClauses must greater than 0";
  public static final String EXCEPTION_SIGN_IS_NULL_4AF91D16 = "sign is null";
  public static final String EXCEPTION_START_IS_NULL_5A075F04 = "start is null";
  public static final String EXCEPTION_END_IS_NULL_C6C46BAA = "end is null";
  public static final String EXCEPTION_SECOND_IS_NULL_989FAA15 = "second is null";
  public static final String EXCEPTION_MODE_IS_NULL_54A948DB = "mode is null";
  public static final String EXCEPTION_PATTERNQUANTIFIER_IS_NULL_0AC88BAB = "patternQuantifier is null";
  public static final String EXCEPTION_COLUMNS_IS_NULL_6C8F32B3 = "columns is null";
  public static final String EXCEPTION_COLUMNS_IS_EMPTY_C7A671C9 = "columns is empty";
  public static final String EXCEPTION_QUANTIFIER_IS_NULL_7B81C096 = "quantifier is null";
  public static final String EXCEPTION_COLUMNNAMES_IS_NULL_C3BF708F = "columnNames is null";
  public static final String EXCEPTION_SETDESCRIPTOR_IS_NULL_4ED0D19A = "setDescriptor is null";
  public static final String EXCEPTION_CLASSIFIERSYMBOL_IS_NULL_B92EE093 = "classifierSymbol is null";
  public static final String EXCEPTION_MATCHNUMBERSYMBOL_IS_NULL_D88DC4EE = "matchNumberSymbol is null";
  public static final String EXCEPTION_LOGICALINDEXPOINTER_IS_NULL_BF8B516B = "logicalIndexPointer is null";
  public static final String EXCEPTION_INPUTSYMBOL_IS_NULL_F4B354EB = "inputSymbol is null";
  public static final String EXCEPTION_PATTERN_CONCATENATION_MUST_HAVE_AT_LEAST_2_ELEMENTS_LEFT_PAREN_ACTUAL_COLON_ARG__8B283563 = "pattern concatenation must have at least 2 elements (actual: %s)";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels is null";
  public static final String EXCEPTION_LOGICAL_OFFSET_MUST_BE_GREATER_THAN_EQUALS_0_COMMA_ACTUAL_COLON_ARG_539807BF = "logical offset must be >= 0, actual: %s";
  public static final String EXCEPTION_PATTERN_ALTERNATION_MUST_HAVE_AT_LEAST_2_ELEMENTS_LEFT_PAREN_ACTUAL_COLON_ARG_RI_A03B40AC = "pattern alternation must have at least 2 elements (actual: %s)";
  public static final String EXCEPTION_RUN_IRROWPATTERNFLATTENER_FIRST_TO_REMOVE_REDUNDANT_EMPTY_PATTERN_D2FB553C = "run IrRowPatternFlattener first to remove redundant empty pattern";
  public static final String EXCEPTION_OUTPUTSYMBOLS_IS_NULL_D7024804 = "outputSymbols is null";
  public static final String EXCEPTION_ROW_IS_NULL_36A3CCAA = "row is null";
  public static final String EXCEPTION_DECLARED_AND_ACTUAL_ROW_COUNTS_DON_QUOTE_T_MATCH_COLON_ARG_VS_ARG_EC8361A5 = "declared and actual row counts don't match: %s vs %s";
  public static final String EXCEPTION_MISSING_ROWS_SPECIFICATION_FOR_VALUES_WITH_NON_MINUS_EMPTY_OUTPUT_SYMBOLS_9BA9C169 = "missing rows specification for Values with non-empty output symbols";
  public static final String EXCEPTION_MISMATCHED_ROWS_DOT_ALL_ROWS_MUST_BE_THE_SAME_SIZE_E98CF3BE = "mismatched rows. All rows must be the same size";
  public static final String EXCEPTION_ROW_SIZE_DOESN_QUOTE_T_MATCH_THE_NUMBER_OF_OUTPUT_SYMBOLS_COLON_ARG_VS_ARG_5FBDF729 = "row size doesn't match the number of output symbols: %s vs %s";
  public static final String EXCEPTION_ARGUMENTNAME_IS_NULL_7F8F665F = "argumentName is null";
  public static final String EXCEPTION_PASSTHROUGHSPECIFICATION_IS_NULL_2B48FE41 = "passThroughSpecification is null";
  public static final String EXCEPTION_SYMBOL_IS_NULL_AE539B31 = "symbol is null";
  public static final String EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800 = "wrong number of new children";
  public static final String EXCEPTION_STARTTYPE_IS_NULL_2EB77A1F = "startType is null";
  public static final String EXCEPTION_STARTVALUE_IS_NULL_4FD58C2B = "startValue is null";
  public static final String EXCEPTION_SORTKEYCOERCEDFORFRAMESTARTCOMPARISON_IS_NULL_4E922E4D = "sortKeyCoercedForFrameStartComparison is null";
  public static final String EXCEPTION_ENDTYPE_IS_NULL_6B9E47D2 = "endType is null";
  public static final String EXCEPTION_ENDVALUE_IS_NULL_69BD66CB = "endValue is null";
  public static final String EXCEPTION_SORTKEYCOERCEDFORFRAMEENDCOMPARISON_IS_NULL_CB0BAC41 = "sortKeyCoercedForFrameEndComparison is null";
  public static final String EXCEPTION_ORIGINALSTARTVALUE_IS_NULL_6BCE78A9 = "originalStartValue is null";
  public static final String EXCEPTION_ORIGINALENDVALUE_IS_NULL_EBA46FFA = "originalEndValue is null";
  public static final String EXCEPTION_PREPARTITIONEDINPUTS_MUST_BE_CONTAINED_IN_PARTITIONBY_CE9DCE6F = "prePartitionedInputs must be contained in partitionBy";
  public static final String EXCEPTION_CANNOT_HAVE_SORTED_MORE_SYMBOLS_THAN_THOSE_REQUESTED_52EEB7C9 = "Cannot have sorted more symbols than those requested";
  public static final String EXCEPTION_PRESORTEDORDERPREFIX_CAN_ONLY_BE_GREATER_THAN_ZERO_IF_ALL_PARTITION_SYMBOLS_ARE__76BBC8DD = "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned";
  public static final String EXCEPTION_ORIGINALSTARTVALUE_MUST_BE_PRESENT_IF_STARTVALUE_IS_PRESENT_30B6FDFF = "originalStartValue must be present if startValue is present";
  public static final String EXCEPTION_FOR_FRAME_OF_TYPE_RANGE_COMMA_SORTKEYCOERCEDFORFRAMESTARTCOMPARISON_MUST_BE_PRES_7533A433 = "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present";
  public static final String EXCEPTION_ORIGINALENDVALUE_MUST_BE_PRESENT_IF_ENDVALUE_IS_PRESENT_E79EF3D2 = "originalEndValue must be present if endValue is present";
  public static final String EXCEPTION_FOR_FRAME_OF_TYPE_RANGE_COMMA_SORTKEYCOERCEDFORFRAMEENDCOMPARISON_MUST_BE_PRESEN_36A665AC = "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present";
  public static final String EXCEPTION_SUBQUERYASSIGNMENTS_IS_NULL_946CDC43 = "subqueryAssignments is null";
  public static final String EXCEPTION_CORRELATION_IS_NULL_F8327EAD = "correlation is null";
  public static final String EXCEPTION_ORIGINSUBQUERY_IS_NULL_8EFEB8D5 = "originSubquery is null";
  public static final String EXCEPTION_INPUT_DOES_NOT_CONTAIN_SYMBOLS_FROM_CORRELATION_1B3DB7BF = "Input does not contain symbols from correlation";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_2_NODES_25FE7927 = "expected newChildren to contain 2 nodes";
  public static final String EXCEPTION_CHILDREN_IS_NULL_0CB43CE2 = "children is null";
  public static final String EXCEPTION_OUTPUTTOINPUTS_IS_NULL_4125312B = "outputToInputs is null";
  public static final String EXCEPTION_OUTPUTS_IS_NULL_EFD078A2 = "outputs is null";
  public static final String EXCEPTION_MUST_HAVE_AT_LEAST_ONE_SOURCE_4114F454 = "Must have at least one source";
  public static final String EXCEPTION_EVERY_CHILD_NEEDS_TO_MAP_ITS_SYMBOLS_TO_AN_OUTPUT_ARG_OPERATION_SYMBOL_7FA107A2 = "Every child needs to map its symbols to an output %s operation symbol";
  public static final String EXCEPTION_CHILD_DOES_NOT_PROVIDE_REQUIRED_SYMBOLS_B7CE956E = "Child does not provide required symbols";
  public static final String EXCEPTION_LEFTOUTPUTSYMBOLS_IS_NULL_083AE900 = "leftOutputSymbols is null";
  public static final String EXCEPTION_RIGHTOUTPUTSYMBOLS_IS_NULL_F44B848F = "rightOutputSymbols is null";
  public static final String EXCEPTION_FILTER_IS_NULL_8F83BD19 = "filter is null";
  public static final String EXCEPTION_SPILLABLE_IS_NULL_8226EA70 = "spillable is null";
  public static final String EXCEPTION_FILTER_MUST_BE_AN_EXPRESSION_OF_BOOLEAN_TYPE_COLON_ARG_F358F1A8 = "Filter must be an expression of boolean type: %s";
  public static final String EXCEPTION_LEFT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_LEFT_OUTPUT_SYMBOLS_71459E3B = "Left source inputs do not contain all left output symbols";
  public static final String EXCEPTION_RIGHT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_RIGHT_OUTPUT_SYMBOLS_23EBC024 = "Right source inputs do not contain all right output symbols";
  public static final String EXCEPTION_EQUALITY_JOIN_CRITERIA_SHOULD_BE_NORMALIZED_ACCORDING_TO_JOIN_SIDES_COLON_ARG_76EB7C14 = "Equality join criteria should be normalized according to join sides: %s";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_2_NODES_FOR_JOINNODE_BEEC3D82 = "expected newChildren to contain 2 nodes for JoinNode";
  public static final String EXCEPTION_MARKERSYMBOL_IS_NULL_0313F046 = "markerSymbol is null";
  public static final String EXCEPTION_HASHSYMBOL_IS_NULL_1BD487F2 = "hashSymbol is null";
  public static final String EXCEPTION_DISTINCTSYMBOLS_IS_NULL_28B8DB52 = "distinctSymbols is null";
  public static final String EXCEPTION_DISTINCTSYMBOLS_CANNOT_BE_EMPTY_A44A693E = "distinctSymbols cannot be empty";
  public static final String EXCEPTION_EXPRESSIONANDVALUEPOINTERS_IS_NULL_8A02F345 = "expressionAndValuePointers is null";
  public static final String EXCEPTION_SKIPTOLABELS_IS_NULL_0A543C09 = "skipToLabels is null";
  public static final String EXCEPTION_SKIPTOPOSITION_IS_NULL_EFBA10CA = "skipToPosition is null";
  public static final String EXCEPTION_COLUMNNAMES_AND_OUTPUTSYMBOLS_SIZES_DON_QUOTE_T_MATCH_1BDF6B28 = "columnNames and outputSymbols sizes don't match";
  public static final String EXCEPTION_AGGREGATIONS_IS_NULL_CFE9CD2E = "aggregations is null";
  public static final String EXCEPTION_GROUPINGSETS_IS_NULL_8EE6D9BF = "groupingSets is null";
  public static final String EXCEPTION_PREGROUPEDSYMBOLS_IS_NULL_DC24FF7B = "preGroupedSymbols is null";
  public static final String EXCEPTION_GLOBALGROUPINGSETS_IS_NULL_5B175B47 = "globalGroupingSets is null";
  public static final String EXCEPTION_GROUPINGKEYS_IS_NULL_90D11F0C = "groupingKeys is null";
  public static final String EXCEPTION_MASK_IS_NULL_5A7CEA49 = "mask is null";
  public static final String EXCEPTION_SOURCE_IS_NULL_45946547 = "source is null";
  public static final String EXCEPTION_STEP_IS_NULL_F83262DA = "step is null";
  public static final String EXCEPTION_GROUPIDSYMBOL_IS_NULL_BFD3763D = "groupIdSymbol is null";
  public static final String EXCEPTION_GROUPING_COLUMNS_DOES_NOT_CONTAIN_GROUPID_COLUMN_83976C83 = "Grouping columns does not contain groupId column";
  public static final String EXCEPTION_ORDER_BY_DOES_NOT_SUPPORT_DISTRIBUTED_AGGREGATION_05109B26 = "ORDER BY does not support distributed aggregation";
  public static final String EXCEPTION_PRE_MINUS_GROUPED_SYMBOLS_MUST_BE_A_SUBSET_OF_THE_GROUPING_KEYS_AFC6C33D = "Pre-grouped symbols must be a subset of the grouping keys";
  public static final String EXCEPTION_GROUPING_SET_COUNT_MUST_BE_LARGER_THAN_0_06FE609E = "grouping set count must be larger than 0";
  public static final String EXCEPTION_LIST_OF_EMPTY_GLOBAL_GROUPING_SETS_MUST_BE_NO_LARGER_THAN_GROUPING_SET_COUNT_12DAD147 = "list of empty global grouping sets must be no larger than grouping set count";
  public static final String EXCEPTION_NO_GROUPING_KEYS_IMPLIES_AT_LEAST_ONE_GLOBAL_GROUPING_SET_COMMA_BUT_NONE_PROVIDE_F099B6D2 = "no grouping keys implies at least one global grouping set, but none provided";
  public static final String EXCEPTION_ARGUMENT_MUST_BE_SYMBOL_COLON_ARG_9C176D4D = "argument must be symbol: %s";
  public static final String EXCEPTION_ARG_AGGREGATION_FUNCTION_ARG_HAS_ARG_ARGUMENTS_COMMA_BUT_ARG_ARGUMENTS_WERE_PROV_53CC06CF = "%s aggregation function %s has %s arguments, but %s arguments were provided to function call";
  public static final String EXCEPTION_IDCOLUMN_IS_NULL_FA206D71 = "idColumn is null";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_1_NODE_7A97D180 = "expected newChildren to contain 1 node";
  public static final String EXCEPTION_SOURCEJOINSYMBOL_IS_NULL_A88AEC85 = "sourceJoinSymbol is null";
  public static final String EXCEPTION_FILTERINGSOURCEJOINSYMBOL_IS_NULL_502C5BD5 = "filteringSourceJoinSymbol is null";
  public static final String EXCEPTION_SEMIJOINOUTPUT_IS_NULL_D961A39B = "semiJoinOutput is null";
  public static final String EXCEPTION_SOURCE_DOES_NOT_CONTAIN_JOIN_SYMBOL_FA572FD2 = "Source does not contain join symbol";
  public static final String EXCEPTION_FILTERING_SOURCE_DOES_NOT_CONTAIN_FILTERING_JOIN_SYMBOL_0C7B1BE3 = "Filtering source does not contain filtering join symbol";
  public static final String EXCEPTION_REHASHMEMORYRESERVATION_IS_NEGATIVE_7EFEFCC2 = "rehashMemoryReservation is negative";
  public static final String EXCEPTION_IDENTIFIER_CANNOT_BE_EMPTY_OR_NULL_9C70B87D = "Identifier cannot be empty or null";
  public static final String EXCEPTION_COLUMN_ALIAS_ARG_IS_AMBIGUOUS_AT_POSITIONS_ARG_F6FF2E93 = "Column alias '%s' is ambiguous at positions %s";
  public static final String EXCEPTION_INVALID_ARGUMENT_ARG_EXPECTED_SCALAR_ARGUMENT_4AD4E32F = "Invalid argument %s. Expected scalar argument";
  public static final String EXCEPTION_THE_SLIDE_ARGUMENT_MUST_HAVE_THE_SAME_WINDOW_MODE_AS_THE_SIZE_ARGUMENT_419C9844 = "The SLIDE argument must have the same window mode as the SIZE argument.";
  public static final String EXCEPTION_THE_ORIGIN_ARGUMENT_IS_ONLY_SUPPORTED_IN_TIME_WINDOW_MODE_B3F358B9 = "The ORIGIN argument is only supported in time window mode.";
  public static final String EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_SORT_THE_TIME_COLUMN_IN_ASCENDING_ORDER_C3FD0F6A = "The ORDER BY clause of the DATA argument must sort the time column in ascending order.";
  public static final String EXCEPTION_ARGUMENTS_SHOULD_NEVER_BE_EMPTY_WHEN_RESOLVING_M4_MODE_F982EA1C = "Arguments should never be empty when resolving M4 mode";
  public static final String EXCEPTION_MISSING_REQUIRED_ARGUMENT_ARG_1308D0DC = "Missing required argument: %s";
  public static final String EXCEPTION_UNKNOWN_ARGUMENT_ARG_F40E9394 = "Unknown argument: %s";

}
