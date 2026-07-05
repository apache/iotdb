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
      "该 PlanNode 不支持 getType() 操作。";
  public static final String PLAN_NODE_CANNOT_CREATE_SUB_NODE =
      "无法为 %s 创建子节点";
  public static final String PLAN_NODE_CHILD_COUNT_INCORRECT =
      "PlanNode 的子节点数量不正确。期望：%d，实际：%d";
  public static final String PLAN_NODE_SERIALIZE_ERROR =
      "序列化 writePlanNode 时发生意外错误。";
  public static final String PLAN_NODE_NOT_SUPPORT_GET_OUTPUT_SYMBOLS =
      "该 PlanNode 不支持 getOutputSymbols() 操作。";
  public static final String TWO_CHILD_NODE_EXCEEDS_LIMIT =
      "该节点不支持两个以上的子节点";

  // ======================== CommonPlanNodeDeserializer ========================

  public static final String NOT_SUPPORTED_FOR_COMMON_DESERIALIZER =
      "CommonPlanNodeDeserializer 不支持此操作";
  public static final String INVALID_NODE_TYPE = "无效的节点类型：%s";

  // ======================== PlanNodeType ========================

  public static final String MULTIPLE_PLAN_NODE_DESERIALIZER_PROVIDER =
      "发现多个 IPlanNodeDeserializerProvider";

  // ======================== Row Pattern ========================

  public static final String UNKNOWN_IR_ROW_PATTERN_TYPE = "未知的 IrRowPattern 类型";
  public static final String UNKNOWN_VALUE_POINTER_TYPE = "未知的 ValuePointer 类型";
  public static final String UNSUPPORTED_IR_NODE_TYPE =
      "不支持的节点类型：%s";

  // ======================== SQL AST ========================

  public static final String NOT_YET_IMPLEMENTED = "尚未实现：%s";
  public static final String NOT_YET_IMPLEMENTED_VISIT =
      "尚未实现：%s.visit%s";
  public static final String GET_EXPRESSION_TYPE_NOT_IMPLEMENTED =
      "getExpressionType 尚未实现：%s";
  public static final String INVALID_EXPRESSION_TYPE = "无效的表达式类型：%s";
  public static final String ONLY_TABLE_SUBQUERY_SUPPORTED =
      "目前仅支持 TableSubquery";
  public static final String FLOAT_LITERAL_CANNOT_CREATE_FROM_LOCATION =
      "当前不支持通过 NodeLocation 创建 FloatLiteral";
  public static final String COLUMNS_SHOULD_BE_EXPANDED =
      "Columns 应在 Analyze 阶段展开";

  // ======================== Parsing ========================

  public static final String BINARY_LITERAL_HEX_ONLY =
      "二进制字面量只能包含十六进制数字";
  public static final String BINARY_LITERAL_EVEN_DIGITS =
      "二进制字面量必须包含偶数个数字";
  public static final String GENERIC_LITERAL_NO_SPACE =
      "'X' 与二进制字面量的起始引号之间不允许有空格";
  public static final String INVALID_NUMERIC_LITERAL = "无效的数值字面量：%s";

  // ======================== Comparison / Logical ========================

  public static final String UNSUPPORTED_COMPARISON = "不支持的比较运算：%s";
  public static final String UNSUPPORTED_LOGICAL_EXPRESSION_TYPE =
      "不支持的逻辑表达式类型：%s";

  // ======================== SQL Formatter ========================

  public static final String UNKNOWN_FILL_METHOD = "未知的填充方法：%s";
  public static final String UNKNOWN_JOIN_CRITERIA = "未知的 join 条件：%s";
  public static final String UNSUPPORTED_RELATION = "不支持的关系：%s";
  public static final String UNEXPECTED_ROWS_PER_MATCH =
      "意外的 rowsPerMatch：%s";
  public static final String UNEXPECTED_SKIP_TO = "意外的 skipTo：%s";
  public static final String UNKNOWN_SIGN = "未知的符号：%s";
  public static final String UNSUPPORTED_FRAME_TYPE = "不支持的帧类型：%s";
  // ======================== Join ========================

  public static final String INVALID_OPERATOR_TYPE = "无效的运算符类型：%s";
  public static final String UNSUPPORTED_JOIN_TYPE = "不支持的 join 类型：%s";

  // ======================== Type System ========================

  public static final String UNSUPPORTED_DATA_TYPE = "不支持的 DataType：%s";
  public static final String INVALID_TYPE_PARAMETER = "无效的类型参数：%s";
  public static final String UNSUPPORTED_TYPE_PARAMETER_KIND =
      "不支持的类型参数种类：%s";
  public static final String EXPECTED_ALL_TYPE_SIGNATURES =
      "期望所有参数均为 TypeSignature，但发现了 [%s]";
  public static final String PARAMETER_KIND_MISMATCH =
      "ParameterKind 为 [%s]，但期望为 [%s]";
  public static final String UNEXPECTED_PARAMETER_KIND = "意外的参数种类：%s";
  public static final String UNSUPPORTED_COLUMN_TYPE = "不支持的列类型：%s";

  // ======================== Function ========================

  public static final String UNKNOWN_FUNCTION = "未知函数：%s";
  public static final String UNSUPPORTED_TABLE_FUNCTION = "不支持的表函数：%s";
  public static final String FUNCTION_ID_MUST_NOT_BE_EMPTY = "id 不能为空";
  public static final String FUNCTION_ID_MUST_BE_LOWERCASE = "id 必须为小写";
  public static final String FUNCTION_ID_MUST_NOT_CONTAIN_AT = "id 不能包含 '@'";
  public static final String VARIADIC_BOUND_MUST_BE_ROW =
      "variadicBound 必须为 row，但实际为 %s";
  public static final String BUILTIN_SCALAR_FUNCTION_CANNOT_BE_INVOKED =
      "内置标量函数 %s 不可调用。";
  public static final String OPERATOR_TYPE_CANNOT_BE_INVOKED =
      "OperatorType %s 不可调用。";
  public static final String INVALID_AGGREGATION_FUNCTION =
      "无效的聚合函数：%s";

  // ======================== Table Function (TVF) ========================

  public static final String INVALID_OPTIONS = "无效的选项：%s";
  public static final String COLUMN_TYPE_NOT_ALLOWED =
      "列 [%s] 的类型为 [%s]，仅允许 INT32、INT64、FLOAT、DOUBLE";
  public static final String PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY =
      "%s 不能为 null 或空";
  public static final String PARAM_SHOULD_NOT_BE_NULL_OR_EMPTY_DOT =
      "%s 不能为 null 或空。";
  public static final String PARAM_SHOULD_BE_GREATER_THAN_ZERO =
      "%s 应大于 0";
  public static final String TARGETS_TOO_MANY_COLUMNS =
      "%s 不应包含多于一个目标列，发现了 [%s] 个目标列。";
  public static final String INPUT_END_TIME_LESS_THAN_START_TIME =
      "输入结束时间不应小于起始时间，起始时间为 %s，结束时间为 %s";
  public static final String OUTPUT_START_TIME_SHOULD_BE_GREATER =
      "%s 应大于目标时间序列的最大时间戳。期望大于 [%s]，但实际为 [%s]。";
  public static final String TIME_COLUMN_SHOULD_NOT_BE_NULL =
      "时间列不应为 null";
  public static final String NON_PARTITIONING_PASS_THROUGH =
      "非 pass-through 来源的表函数不允许使用非分区 pass-through 列";

  // ======================== Pattern Match TVF ========================

  public static final String SMOOTH_MUST_BE_NON_NEGATIVE =
      "smooth 必须为非负数，但传入了：%s";
  public static final String THRESHOLD_MUST_BE_NON_NEGATIVE =
      "threshold 必须为非负数，但传入了：%s";
  public static final String WIDTH_MUST_BE_NON_NEGATIVE =
      "width 必须为非负数，但传入了：%s";
  public static final String HEIGHT_MUST_BE_NON_NEGATIVE =
      "height 必须为非负数，但传入了：%s";
  public static final String INVALID_PATTERN_MISSING_REPEAT_SIGN =
      "无效的模式：%s，'}' 后缺少重复符号";
  public static final String INVALID_PATTERN_MUST_CONTAIN_DATA_POINTS =
      "无效的模式：'%s'。模式必须包含至少两个以逗号分隔的数值数据点，例如 '1,2,3'";

  // ======================== Forecast / Classify TVF ========================

  public static final String SERIALIZE_FORECAST_HANDLE_ERROR =
      "序列化 ForecastTableFunctionHandle 时发生错误：%s";
  public static final String FORECAST_EXECUTION_ERROR =
      "执行 forecast 时发生错误：[%s]";
  public static final String CLASSIFY_EXECUTION_ERROR =
      "执行 classify 时发生错误：[%s]";
  public static final String MODEL_OUTPUT_COLUMN_MISMATCH =
      "模型 %s 输出了 %s 列，与指定的 %s 列不一致";
  public static final String MODEL_OUTPUT_LENGTH_MISMATCH =
      "模型 %s 输出长度为 %s，与指定的 %s 不一致";

  // ======================== UDF Management ========================

  public static final String UDF_INFORMATION_NOT_AVAILABLE = "UDFInformation 不可用";
  public static final String UDF_REGISTER_CONFLICT_BUILTIN =
      "注册 UDF %s(%s) 失败，因为函数名与内置函数名冲突";
  public static final String UDF_REGISTER_JAR_MD5_CONFLICT =
      "注册函数 %s(%s) 失败，因为函数 %s 已有的 jar 文件 md5 与新 jar 文件不同。";
  public static final String UDF_REGISTER_INSTANCE_FAILED =
      "注册 UDF %s(%s) 失败，因为无法成功构造其实例。异常：%s";
  public static final String UDF_CLASS_TYPE_UNSUPPORTED =
      "不支持的 UDF 类类型。仅支持 UDF 和 SQLFunction。";
  public static final String UDF_REFLECT_NOT_REGISTERED =
      "反射 UDF 实例失败，因为 UDF %s 尚未注册。";
  public static final String UDF_REFLECT_INSTANCE_FAILED =
      "反射 UDF %s(%s) 实例失败，原因：%s";
  public static final String AI_NODE_SERVICE_NOT_AVAILABLE =
      "当前节点不支持表函数 AINode 服务";
  // ======================== UDTF Forecast ========================

  public static final String INPUT_DATA_TYPE_NOT_SUPPORTED =
      "输入数据类型 %s 不被支持，仅允许 %s。";
  public static final String MODEL_ID_MUST_BE_PROVIDED =
      "MODEL_ID 参数必须提供且不能为空。";
  public static final String UNSUPPORTED_DATA_TYPE_FOR_UDF = "不支持的数据类型 %s";
  public static final String FORECAST_FAILED =
      "预测失败，错误码 %d，原因：%s";
  public static final String FORECAST_RESULT_LENGTH_MISMATCH =
      "预测结果长度 %d 与期望的输出长度 %d 不一致";
  public static final String FORECAST_RESULT_SINGLE_COLUMN =
      "预测结果应仅包含一个值列，但实际有 %d 个";

  // ======================== TableSchema ========================

  public static final String TABLE_COLUMN_NAME_DUPLICATED =
      "表中的列不应同名：%s。";
  public static final String TSFILE_TABLE_SCHEMA_CONVERT_FAILED =
      "无法将 TsFile 表结构转换为 IoTDB 表结构，表名：{}，TsFile 表结构：{}";

  // ======================== DateTime ========================

  public static final String TIMESTAMP_OVERFLOW =
      "时间戳溢出，毫秒值：%s，时间戳精度：%s";
  public static final String EXTRACT_TIMESTAMP_MS_PART_NULL =
      "ExtractTimestampMsPart 为 null";
  public static final String EXTRACT_TIMESTAMP_US_PART_NULL =
      "ExtractTimestampUsPart 为 null";
  public static final String EXTRACT_TIMESTAMP_NS_PART_NULL =
      "ExtractTimestampNsPart 为 null";
  public static final String DATETIME_CONVERT_FAILED =
      "将 %s 转换为毫秒失败，时区偏移为 %s，请输入类似 2011-12-03T10:15:30 或 2011-12-03T10:15:30+01:00 的格式";
  public static final String DATETIME_TIME_REGION_NOT_SUPPORTED =
      "暂不支持 %s 末尾的 [time-region] 格式，请输入类似 2011-12-03T10:15:30 或 2011-12-03T10:15:30+01:00 的格式";
  public static final String UNSUPPORTED_TIME_PRECISION =
      "不支持的 time_precision：%s";
  public static final String TIMESTAMP_UNEXPECTEDLY_LARGE =
      "时间戳异常大，您可能忘记设置时间戳精度。"
          + "当前系统时间戳精度为 %s，"
          + "请检查时间戳 %s 是否正确。"
          + "如果确定要插入此时间戳，请设置 timestamp_precision_check_enabled=false 并重启服务。";

  // ======================== SqlDialect ========================

  public static final String UNKNOWN_SQL_DIALECT = "未知的 SQL 方言：%s";

  // ======================== Hash / GroupBy ========================

  public static final String MEMORY_FOR_FLAT_HASH_NOT_ENOUGH =
      "flatHash 的内存不足";
  public static final String LONG_OVERFLOW = "long 溢出";
  public static final String INTEGER_OVERFLOW = "integer 溢出";
  public static final String INVALID_INPUT_DESC_LENGTH_LT_8 =
      "无效输入：desc.length - offset < 8";

  private QueryMessages() {}

  public static final String TIME_PRECISION_MUST_BE_ONE_OF = "时间精度必须是以下之一：h,m,s,ms,u,n";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_4 = "无效输入：bytes.length - offset < 4";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_8 = "无效输入：bytes.length - offset < 8";
  public static final String INVALID_INPUT_DESC_OFFSET_LT_4 = "无效输入：desc.length - offset < 4";
  public static final String SIZE_OF_HASH_TABLE_CANNOT_EXCEED = "哈希表大小不能超过 10 亿条目";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_APPEND = "NoChannelGroupByHash 不支持 appendValuesTo";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_RAW_HASH = "NoChannelGroupByHash 不支持 getRawHash";
  public static final String MULTIPLE_TABLE_FUNCTION_AI_NODE_PROVIDER = "找到多个 ITableFunctionAINodeServiceProvider";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNKNOWN_TYPE_BC7EAB78 = "未知的类型: ";
  public static final String EXCEPTION_UNEXPECTED_ROWSPERMATCH_F9B931C9 = "意外的 rowsPerMatch: ";
  public static final String EXCEPTION_UNSUPPORTED_FILL_METHOD_0809912A = "不支持的填充方法: ";
  public static final String EXCEPTION_EXCEPTNODE_SHOULD_NEVER_SERIALIZED_CURRENT_VERSION_6F5B4EE0 = "ExceptNode 在当前版本中绝不应被序列化";
  public static final String EXCEPTION_EXCEPTNODE_SHOULD_NEVER_DESERIALIZED_CURRENT_VERSION_690C18F0 = "ExceptNode 在当前版本中绝不应被反序列化";
  public static final String EXCEPTION_INTERSECTNODE_SHOULD_NEVER_SERIALIZED_CURRENT_VERSION_68D2C0D3 = "IntersectNode 在当前版本中绝不应被序列化";
  public static final String EXCEPTION_INTERSECTNODE_SHOULD_NEVER_DESERIALIZED_CURRENT_VERSION_A3614371 = "IntersectNode 在当前版本中绝不应被反序列化";
  public static final String EXCEPTION_COMMA_50AD1C01 = ", ";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_OUTERQUERYSCOPE_IS_NULL_D5305685 = "outerQueryScope 不能为空";
  public static final String EXCEPTION_WARNINGCOLLECTOR_IS_NULL_7A524A68 = "warningCollector 不能为空";
  public static final String EXCEPTION_UPDATEKIND_IS_NULL_6CA83291 = "updateKind 不能为空";
  public static final String EXCEPTION_EXPECTED_ARG_FIELDS_COMMA_GOT_ARG_498063F2 = "期望 %s 个字段，实际为 %s";
  public static final String EXCEPTION_MISSING_TABLE_ARGUMENT_COLON_ARG_4F1E9F4E = "缺少表参数: %s";
  public static final String EXCEPTION_TABLE_ARGUMENT_SPECIFICATION_CANNOT_HAVE_A_DEFAULT_VALUE_DOT_009A02CA = "表参数规格不能有默认值。";
  public static final String EXCEPTION_MISSING_DEFAULT_VALUE_FOR_SCALAR_ARGUMENT_COLON_ARG_8AF70F38 = "缺少标量参数的默认值: %s";
  public static final String EXCEPTION_RESULT_SCOPE_SHOULD_HAVE_OUTER_QUERY_SCOPE_EQUAL_WITH_PARAMETER_OUTER_QUERY_SCOP_BE9CDB22 = "result scope 应具有等于参数外层查询 scope 的外层查询 scope";
  public static final String EXCEPTION_RETURN_SCOPE_SHOULD_HAVE_CONTEXT_SCOPE_AS_ONE_OF_ITS_ANCESTORS_BBBFFE3E = "return scope 应将 context scope 作为其祖先之一";
  public static final String EXCEPTION_MISSING_RELATION_ALIAS_437EDF9C = "缺少 relation alias";
  public static final String EXCEPTION_MISMATCHED_ALIASES_A6D3F8CE = "aliases 不匹配";
  public static final String EXCEPTION_UNEXPECTED_OFFSET_ROWCOUNT_COLON_977E305C = "意外的 OFFSET rowCount: ";
  public static final String EXCEPTION_INVALID_LIMIT_NODE_TYPE_DOT_EXPECTED_COLON_LIMIT_DOT_ACTUAL_COLON_ARG_5BCD1C76 = "无效的 limit node type。期望: Limit。实际: %s";
  public static final String EXCEPTION_UNEXPECTED_LIMIT_ROWCOUNT_COLON_CFF154F0 = "意外的 LIMIT rowCount: ";
  public static final String EXCEPTION_NON_MINUS_EMPTY_ORDERBYEXPRESSIONS_LIST_WITHOUT_ORDERBYSCOPE_PROVIDED_240DDD64 = "非空 orderByExpressions 列表未提供 orderByScope";
  public static final String EXCEPTION_OUTPUT_FIELDS_IS_NULL_FOR_SELECT_ITEM_ARG_56AFEC51 = "select item %s 的输出字段不能为空";
  public static final String EXCEPTION_NODE_IS_NULL_C1479F4A = "node 不能为空";
  public static final String EXCEPTION_COLUMNS_SHOULDN_QUOTE_T_BE_EMPTY_HERE_34E44AAA = "columns 此处不应为空";
  public static final String EXCEPTION_GREATER_THAN_0B37D87A = " > ";
  public static final String EXCEPTION_GROUPID_OUT_OF_RANGE_8B10E54B = "groupId 超出范围";
  public static final String EXCEPTION_MINIMUMREQUIREDCAPACITY_MUST_BE_POSITIVE_81638118 = "minimumRequiredCapacity 必须为正数";
  public static final String EXCEPTION_EXPECTEDSIZE_MUST_BE_GREATER_THAN_ZERO_DA2E0345 = "expectedSize 必须大于 0";
  public static final String EXCEPTION_POSITION_COUNT_OUT_OF_BOUND_5513847E = "position count 越界";
  public static final String EXCEPTION_ID_IS_NULL_9D5D27B1 = "id 不能为空";
  public static final String EXCEPTION_NEWCHILDREN_IS_NOT_EMPTY_170FCE18 = "newChildren 不为空";
  public static final String EXCEPTION_ORDERBY_IS_NULL_AA2494DE = "orderBy 不能为空";
  public static final String EXCEPTION_ORDERINGS_IS_NULL_59B1C097 = "orderings 不能为空";
  public static final String EXCEPTION_ORDERBY_IS_EMPTY_963405E0 = "orderBy 为空";
  public static final String EXCEPTION_ORDERBY_KEYS_AND_ORDERINGS_DON_QUOTE_T_MATCH_E0334493 = "orderBy keys 和 orderings 不匹配";
  public static final String EXCEPTION_NO_ORDERING_FOR_SYMBOL_COLON_ARG_F54E70FC = "symbol 缺少 ordering: %s";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name 不能为空";
  public static final String EXCEPTION_UNEXPECTED_EXPRESSION_COLON_ARG_8E4AC833 = "意外的 expression: %s";
  public static final String EXCEPTION_ORDERINGSCHEME_IS_NULL_4D4D2F6F = "orderingScheme 不能为空";
  public static final String EXCEPTION_ASSIGNMENTS_IS_NULL_1FD6142D = "assignments 不能为空";
  public static final String EXCEPTION_OUTPUT_IS_NULL_3CDA316E = "output 不能为空";
  public static final String EXCEPTION_EXPRESSION_IS_NULL_16C079B5 = "expression 不能为空";
  public static final String EXCEPTION_SYMBOL_ARG_ALREADY_HAS_ASSIGNMENT_ARG_COMMA_WHILE_ADDING_ARG_EE8CADA3 = "Symbol %s 已有 assignment %s，无法再添加 %s";
  public static final String EXCEPTION_TYPEVARIABLECONSTRAINTS_IS_NULL_0A86DA34 = "typeVariableConstraints 不能为空";
  public static final String EXCEPTION_LONGVARIABLECONSTRAINTS_IS_NULL_51F80E3C = "longVariableConstraints 不能为空";
  public static final String EXCEPTION_RETURNTYPE_IS_NULL_07C7C6A5 = "returnType 不能为空";
  public static final String EXCEPTION_ARGUMENTTYPES_IS_NULL_1E377BFD = "argumentTypes 不能为空";
  public static final String EXCEPTION_TYPEVARIABLECONSTRAINT_IS_NULL_18B97042 = "typeVariableConstraint 不能为空";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type 不能为空";
  public static final String EXCEPTION_VARIADICBOUND_IS_NULL_33A6BCC2 = "variadicBound 不能为空";
  public static final String EXCEPTION_CASTABLETO_IS_NULL_0F2A5B36 = "castableTo 不能为空";
  public static final String EXCEPTION_CASTABLEFROM_IS_NULL_DE5158C7 = "castableFrom 不能为空";
  public static final String EXCEPTION_FUNCTIONNAME_IS_NULL_0818CBC7 = "functionName 不能为空";
  public static final String EXCEPTION_RESOLVEDFUNCTION_IS_NULL_81B5B93A = "resolvedFunction 不能为空";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value 不能为空";
  public static final String EXCEPTION_VALUES_IS_NULL_F1D7D3D8 = "values 不能为空";
  public static final String EXCEPTION_FIELDNAME_IS_NULL_DEB1CA0C = "fieldName 不能为空";
  public static final String EXCEPTION_TYPESIGNATURE_IS_NULL_E8B47305 = "typeSignature 不能为空";
  public static final String EXCEPTION_BASE_TYPE_NAME_CANNOT_BE_A_TYPE_VARIABLE_71E810A9 = "base type name 不能是 type variable";
  public static final String EXCEPTION_BASE_IS_NULL_AC445AD0 = "base 不能为空";
  public static final String EXCEPTION_BASE_IS_EMPTY_E86FBC3A = "base 为空";
  public static final String EXCEPTION_BAD_CHARACTERS_IN_BASE_TYPE_COLON_ARG_FA811786 = "base type 中存在非法字符: %s";
  public static final String EXCEPTION_PARAMETERS_IS_NULL_418C7892 = "parameters 不能为空";
  public static final String EXCEPTION_PARAMETERS_FOR_ROW_TYPE_MUST_BE_NAMED_TYPE_PARAMETERS_6AADD078 = "ROW 类型的参数必须为 NAMED_TYPE 参数";
  public static final String EXCEPTION_KIND_IS_NULL_8C13BAB2 = "kind 不能为空";
  public static final String EXCEPTION_COLUMNCATEGORY_IS_NULL_0075924B = "columnCategory 不能为空";
  public static final String EXCEPTION_PROPERTIES_IS_NULL_57B88B49 = "properties 不能为空";
  public static final String EXCEPTION_COMMENT_IS_NULL_0AD46118 = "comment 不能为空";
  public static final String EXCEPTION_EXTRAINFO_IS_NULL_43AE989F = "extraInfo 不能为空";
  public static final String EXCEPTION_SIGNATURE_IS_NULL_CA3D8772 = "signature 不能为空";
  public static final String EXCEPTION_FUNCTIONID_IS_NULL_F91F8E89 = "functionId 不能为空";
  public static final String EXCEPTION_FUNCTIONKIND_IS_NULL_EDF86E36 = "functionKind 不能为空";
  public static final String EXCEPTION_FUNCTIONNULLABILITY_IS_NULL_6EF52FB4 = "functionNullability 不能为空";
  public static final String EXCEPTION_INVALID_NAME_ARG_AD3FA0BF = "无效名称 %s";
  public static final String EXCEPTION_INVALID_NAME_COLON_LEFT_BRACKET_ARG_RIGHT_BRACKET_C46F15A4 = "无效名称: [%s]";
  public static final String EXCEPTION_ARGUMENTNULLABLE_IS_NULL_0CC221D3 = "argumentNullable 不能为空";
  public static final String EXCEPTION_LITERALFORMATTER_IS_NULL_3F4F4A2B = "literalFormatter 不能为空";
  public static final String EXCEPTION_SYMBOLREFERENCEFORMATTER_IS_NULL_B9B540EC = "symbolReferenceFormatter 不能为空";
  public static final String EXCEPTION_BUILDER_IS_NULL_ADE64E9B = "builder 不能为空";
  public static final String EXCEPTION_VISITEXPRESSION_SHOULD_ONLY_BE_CALLED_AT_ROOT_93E4AAD4 = "visitExpression 只能在 root 调用";
  public static final String EXCEPTION_VISITROWPATTERN_SHOULD_ONLY_BE_CALLED_AT_ROOT_9DE9F574 = "visitRowPattern 只能在 root 调用";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_AFTER_MATCH_SKIP_TO_LAST_82A12A21 = "AFTER MATCH SKIP TO LAST 中缺少标识符";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_AFTER_MATCH_SKIP_TO_FIRST_F988B839 = "AFTER MATCH SKIP TO FIRST 中缺少标识符";
  public static final String EXCEPTION_LINE_MUST_BE_GREATER_THAN_0_8D2C1802 = "line 必须为 > 0";
  public static final String EXCEPTION_COLUMN_MUST_BE_GREATER_THAN_0_2481C561 = "column 必须为 > 0";
  public static final String EXCEPTION_LEFT_IS_NULL_2C1080C5 = "left 不能为空";
  public static final String EXCEPTION_RIGHT_IS_NULL_97BD6491 = "right 不能为空";
  public static final String EXCEPTION_LOCATION_IS_NULL_F134D388 = "location 不能为空";
  public static final String EXCEPTION_CRITERIA_IS_NULL_2996D1A3 = "criteria 不能为空";
  public static final String EXCEPTION_NO_JOIN_CRITERIA_SPECIFIED_44DED0B9 = "未指定 join criteria";
  public static final String EXCEPTION_ARG_JOIN_CANNOT_HAVE_JOIN_CRITERIA_3B23E0D1 = "%s join 不能包含 join criteria";
  public static final String EXCEPTION_SELECTITEMS_16EC72CF = "selectItems";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function 不能为空";
  public static final String EXCEPTION_PRECISION_IS_NULL_73E48139 = "precision 不能为空";
  public static final String EXCEPTION_OPERAND_IS_NULL_D0182140 = "operand 不能为空";
  public static final String EXCEPTION_WHENCLAUSES_IS_NULL_140535CF = "whenClauses 不能为空";
  public static final String EXCEPTION_DEFAULTVALUE_IS_NULL_B1C8490D = "defaultValue 不能为空";
  public static final String EXCEPTION_THE_LENGTH_OF_SIMPLECASEEXPRESSION_QUOTE_S_WHENCLAUSES_MUST_GREATER_THAN_0_5D2963B9 = "SimpleCaseExpression 的 whenClauses 长度必须大于 0";
  public static final String EXCEPTION_SETS_IS_NULL_C9A2FA67 = "sets 不能为空";
  public static final String EXCEPTION_GROUPING_SETS_CANNOT_BE_EMPTY_8E0BE7DB = "grouping sets 不能为空";
  public static final String EXCEPTION_IDENTIFIERS_IS_NULL_72CDC3BE = "identifiers 不能为空";
  public static final String EXCEPTION_IDENTIFIERS_IS_EMPTY_410ED65C = "identifiers 为空";
  public static final String EXCEPTION_QUERIES_IS_NULL_94399651 = "queries 不能为空";
  public static final String EXCEPTION_QUERIES_IS_EMPTY_FE480BBC = "queries 为空";
  public static final String EXCEPTION_VALUE_IS_EMPTY_A42300DD = "value 为空";
  public static final String EXCEPTION_VALUE_CONTAINS_ILLEGAL_CHARACTERS_COLON_ARG_C6DBFEBA = "value 包含非法字符: %s";
  public static final String EXCEPTION_RELATIONS_IS_NULL_0275CA3C = "relations 不能为空";
  public static final String EXCEPTION_FILLMETHOD_IS_NULL_2E5A83E6 = "fillMethod 不能为空";
  public static final String EXCEPTION_RELATION_IS_NULL_890596ED = "relation 不能为空";
  public static final String EXCEPTION_ALIAS_IS_NULL_B2ED729A = "alias 不能为空";
  public static final String EXCEPTION_RESULT_IS_NULL_031E2F89 = "result 不能为空";
  public static final String EXCEPTION_VALUELIST_IS_NULL_9271DC51 = "valueList 不能为空";
  public static final String EXCEPTION_ROWS_IS_NULL_B8BF74DE = "rows 不能为空";
  public static final String EXCEPTION_SORTITEMS_IS_NULL_DD277CDD = "sortItems 不能为空";
  public static final String EXCEPTION_SORTITEMS_SHOULD_NOT_BE_EMPTY_78C26791 = "sortItems 不应为空";
  public static final String EXCEPTION_FIELD_IS_NULL_80E8CE23 = "field 不能为空";
  public static final String EXCEPTION_EXISTINGWINDOWNAME_IS_NULL_5D67FD72 = "existingWindowName 不能为空";
  public static final String EXCEPTION_PARTITIONBY_IS_NULL_84791B6B = "partitionBy 不能为空";
  public static final String EXCEPTION_FRAME_IS_NULL_5A92D609 = "frame 不能为空";
  public static final String EXCEPTION_CONDITION_IS_NULL_327EBA55 = "condition 不能为空";
  public static final String EXCEPTION_TRUEVALUE_IS_NULL_99C09BA5 = "trueValue 不能为空";
  public static final String EXCEPTION_PATTERNS_IS_NULL_7E0DDDF0 = "patterns 不能为空";
  public static final String EXCEPTION_PATTERNS_LIST_IS_EMPTY_D4D1802F = "patterns list 为空";
  public static final String EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2 = "arguments 不能为空";
  public static final String EXCEPTION_TARGET_IS_NULL_240F0372 = "target 不能为空";
  public static final String EXCEPTION_ALIASES_IS_NULL_C481EE59 = "aliases 不能为空";
  public static final String EXCEPTION_SUBQUERY_IS_NULL_F0E1842F = "subquery 不能为空";
  public static final String EXCEPTION_INPUT_IS_NULL_EE7EADB0 = "input 不能为空";
  public static final String EXCEPTION_MEASURES_IS_NULL_EC9D2431 = "measures 不能为空";
  public static final String EXCEPTION_ROWSPERMATCH_IS_NULL_661EA4A9 = "rowsPerMatch 不能为空";
  public static final String EXCEPTION_AFTERMATCHSKIPTO_IS_NULL_7623C3C0 = "afterMatchSkipTo 不能为空";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern 不能为空";
  public static final String EXCEPTION_SUBSETS_IS_NULL_AF77CD01 = "subsets 不能为空";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_NULL_5F7B8ED4 = "variableDefinitions 不能为空";
  public static final String EXCEPTION_VARIABLEDEFINITIONS_IS_EMPTY_9E324869 = "variableDefinitions 为空";
  public static final String EXCEPTION_UNEXPECTED_ROWCOUNT_CLASS_COLON_ARG_170FC7CD = "意外的 rowCount class: %s";
  public static final String EXCEPTION_ITEMS_IS_NULL_32A25379 = "items 不能为空";
  public static final String EXCEPTION_OPERANDS_IS_NULL_135465B7 = "operands 不能为空";
  public static final String EXCEPTION_MUST_HAVE_AT_LEAST_TWO_OPERANDS_CE7B0DCB = "必须至少有两个 operands";
  public static final String EXCEPTION_LINE_MUST_BE_AT_LEAST_ONE_COMMA_GOT_COLON_ARG_6BA7A99C = "line 必须至少为 1，实际为: %s";
  public static final String EXCEPTION_COLUMN_MUST_BE_AT_LEAST_ONE_COMMA_GOT_COLON_ARG_4A529240 = "column 必须至少为 1，实际为: %s";
  public static final String EXCEPTION_QUERY_IS_NULL_689B7978 = "query 不能为空";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator 不能为空";
  public static final String EXCEPTION_EXPECTED_AT_LEAST_2_TERMS_87CD9010 = "期望至少 2 个 terms";
  public static final String EXCEPTION_SPECIFICATION_IS_NULL_BB4AA029 = "specification 不能为空";
  public static final String EXCEPTION_TRIMSOURCE_IS_NULL_C7E6B71D = "trimSource 不能为空";
  public static final String EXCEPTION_TRIMCHARACTER_IS_NULL_4036C3D8 = "trimCharacter 不能为空";
  public static final String EXCEPTION_POSITION_IS_NULL_5B14C61B = "position 不能为空";
  public static final String EXCEPTION_IDENTIFIER_IS_NULL_8F8171C8 = "identifier 不能为空";
  public static final String EXCEPTION_MISSING_IDENTIFIER_IN_SKIP_TO_ARG_33E1A7C3 = "SKIP TO %s 中缺少标识符";
  public static final String EXCEPTION_UNEXPECTED_IDENTIFIER_IN_SKIP_TO_ARG_58438AFC = "SKIP TO %s 中出现意外的标识符";
  public static final String EXCEPTION_FIRST_IS_NULL_DC679129 = "first 不能为空";
  public static final String EXCEPTION_ORIGINALPARTS_IS_NULL_EA9B01F3 = "originalParts 不能为空";
  public static final String EXCEPTION_ORIGINALPARTS_IS_EMPTY_0D1EFAC4 = "originalParts 为空";
  public static final String EXCEPTION_MIN_IS_NULL_B80144C1 = "min 不能为空";
  public static final String EXCEPTION_MAX_IS_NULL_CCABB3BF = "max 不能为空";
  public static final String EXCEPTION_TABLE_IS_NULL_8DDD9098 = "table 不能为空";
  public static final String EXCEPTION_WITH_IS_NULL_20C2A1AE = "with 不能为空";
  public static final String EXCEPTION_QUERYBODY_IS_NULL_E3EB26CA = "queryBody 不能为空";
  public static final String EXCEPTION_FILL_IS_NULL_3548C13D = "fill 不能为空";
  public static final String EXCEPTION_OFFSET_IS_NULL_82BA6093 = "offset 不能为空";
  public static final String EXCEPTION_LIMIT_IS_NULL_2EE9FA0F = "limit 不能为空";
  public static final String EXCEPTION_LIMIT_MUST_BE_OPTIONAL_OF_EITHER_FETCHFIRST_OR_LIMIT_TYPE_0636CAA9 = "limit 必须是 FetchFirst 或 Limit 类型之一的可选项";
  public static final String EXCEPTION_ATLEAST_IS_NULL_2FE8D701 = "atLeast 不能为空";
  public static final String EXCEPTION_ATMOST_IS_NULL_778B3B3A = "atMost 不能为空";
  public static final String EXCEPTION_VALUES_CANNOT_BE_EMPTY_F18F863D = "values 不能为空";
  public static final String EXCEPTION_SELECT_IS_NULL_B45C440F = "select 不能为空";
  public static final String EXCEPTION_FROM_IS_NULL_2651BF57 = "FROM 不能为空";
  public static final String EXCEPTION_WHERE_IS_NULL_A1A3FCBC = "where 不能为空";
  public static final String EXCEPTION_GROUPBY_IS_NULL_4CF478F2 = "groupBy 不能为空";
  public static final String EXCEPTION_HAVING_IS_NULL_700DC5B0 = "having 不能为空";
  public static final String EXCEPTION_WINDOWS_IS_NULL_549D8892 = "windows 不能为空";
  public static final String EXCEPTION_FIELDINDEX_MUST_BE_GREATER_THAN_EQUALS_0_4138C39B = "fieldIndex 必须为 >= 0";
  public static final String EXCEPTION_WINDOW_IS_NULL_7532B76A = "window 不能为空";
  public static final String EXCEPTION_ESCAPE_IS_NULL_C9E4F69B = "escape 不能为空";
  public static final String EXCEPTION_PROCESSINGMODE_IS_NULL_F4D8AE91 = "processingMode 不能为空";
  public static final String EXCEPTION_SIMPLEGROUPBYEXPRESSIONS_IS_NULL_3F6A1ECC = "simpleGroupByExpressions 不能为空";
  public static final String EXCEPTION_THE_LENGTH_OF_SEARCHEDCASEEXPRESSION_QUOTE_S_WHENCLAUSES_MUST_GREATER_THAN_0_D710DBF6 = "SearchedCaseExpression 的 whenClauses 长度必须大于 0";
  public static final String EXCEPTION_SIGN_IS_NULL_4AF91D16 = "sign 不能为空";
  public static final String EXCEPTION_START_IS_NULL_5A075F04 = "start 不能为空";
  public static final String EXCEPTION_END_IS_NULL_C6C46BAA = "end 不能为空";
  public static final String EXCEPTION_SECOND_IS_NULL_989FAA15 = "second 不能为空";
  public static final String EXCEPTION_MODE_IS_NULL_54A948DB = "mode 不能为空";
  public static final String EXCEPTION_PATTERNQUANTIFIER_IS_NULL_0AC88BAB = "patternQuantifier 不能为空";
  public static final String EXCEPTION_COLUMNS_IS_NULL_6C8F32B3 = "columns 不能为空";
  public static final String EXCEPTION_COLUMNS_IS_EMPTY_C7A671C9 = "columns 为空";
  public static final String EXCEPTION_QUANTIFIER_IS_NULL_7B81C096 = "quantifier 不能为空";
  public static final String EXCEPTION_COLUMNNAMES_IS_NULL_C3BF708F = "columnNames 不能为空";
  public static final String EXCEPTION_SETDESCRIPTOR_IS_NULL_4ED0D19A = "setDescriptor 不能为空";
  public static final String EXCEPTION_CLASSIFIERSYMBOL_IS_NULL_B92EE093 = "classifierSymbol 不能为空";
  public static final String EXCEPTION_MATCHNUMBERSYMBOL_IS_NULL_D88DC4EE = "matchNumberSymbol 不能为空";
  public static final String EXCEPTION_LOGICALINDEXPOINTER_IS_NULL_BF8B516B = "logicalIndexPointer 不能为空";
  public static final String EXCEPTION_INPUTSYMBOL_IS_NULL_F4B354EB = "inputSymbol 不能为空";
  public static final String EXCEPTION_PATTERN_CONCATENATION_MUST_HAVE_AT_LEAST_2_ELEMENTS_LEFT_PAREN_ACTUAL_COLON_ARG__8B283563 = "pattern concatenation 必须至少有 2 个元素（实际: %s）";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels 不能为空";
  public static final String EXCEPTION_LOGICAL_OFFSET_MUST_BE_GREATER_THAN_EQUALS_0_COMMA_ACTUAL_COLON_ARG_539807BF = "logical offset 必须 >= 0，实际: %s";
  public static final String EXCEPTION_PATTERN_ALTERNATION_MUST_HAVE_AT_LEAST_2_ELEMENTS_LEFT_PAREN_ACTUAL_COLON_ARG_RI_A03B40AC = "pattern alternation 必须至少有 2 个元素（实际: %s）";
  public static final String EXCEPTION_RUN_IRROWPATTERNFLATTENER_FIRST_TO_REMOVE_REDUNDANT_EMPTY_PATTERN_D2FB553C = "请先运行 IrRowPatternFlattener，以移除多余的空 pattern";
  public static final String EXCEPTION_OUTPUTSYMBOLS_IS_NULL_D7024804 = "outputSymbols 不能为空";
  public static final String EXCEPTION_ROW_IS_NULL_36A3CCAA = "row 不能为空";
  public static final String EXCEPTION_DECLARED_AND_ACTUAL_ROW_COUNTS_DON_QUOTE_T_MATCH_COLON_ARG_VS_ARG_EC8361A5 = "声明的 row count 与实际 row count 不匹配: %s vs %s";
  public static final String EXCEPTION_MISSING_ROWS_SPECIFICATION_FOR_VALUES_WITH_NON_MINUS_EMPTY_OUTPUT_SYMBOLS_9BA9C169 = "具有非空 output symbols 的 Values 缺少 rows specification";
  public static final String EXCEPTION_MISMATCHED_ROWS_DOT_ALL_ROWS_MUST_BE_THE_SAME_SIZE_E98CF3BE = "rows 不匹配。所有 rows 大小必须一致";
  public static final String EXCEPTION_ROW_SIZE_DOESN_QUOTE_T_MATCH_THE_NUMBER_OF_OUTPUT_SYMBOLS_COLON_ARG_VS_ARG_5FBDF729 = "row size 与 output symbols 数量不匹配: %s vs %s";
  public static final String EXCEPTION_ARGUMENTNAME_IS_NULL_7F8F665F = "argumentName 不能为空";
  public static final String EXCEPTION_PASSTHROUGHSPECIFICATION_IS_NULL_2B48FE41 = "passThroughSpecification 不能为空";
  public static final String EXCEPTION_SYMBOL_IS_NULL_AE539B31 = "symbol 不能为空";
  public static final String EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800 = "new children 数量错误";
  public static final String EXCEPTION_STARTTYPE_IS_NULL_2EB77A1F = "startType 不能为空";
  public static final String EXCEPTION_STARTVALUE_IS_NULL_4FD58C2B = "startValue 不能为空";
  public static final String EXCEPTION_SORTKEYCOERCEDFORFRAMESTARTCOMPARISON_IS_NULL_4E922E4D = "sortKeyCoercedForFrameStartComparison 不能为空";
  public static final String EXCEPTION_ENDTYPE_IS_NULL_6B9E47D2 = "endType 不能为空";
  public static final String EXCEPTION_ENDVALUE_IS_NULL_69BD66CB = "endValue 不能为空";
  public static final String EXCEPTION_SORTKEYCOERCEDFORFRAMEENDCOMPARISON_IS_NULL_CB0BAC41 = "sortKeyCoercedForFrameEndComparison 不能为空";
  public static final String EXCEPTION_ORIGINALSTARTVALUE_IS_NULL_6BCE78A9 = "originalStartValue 不能为空";
  public static final String EXCEPTION_ORIGINALENDVALUE_IS_NULL_EBA46FFA = "originalEndValue 不能为空";
  public static final String EXCEPTION_PREPARTITIONEDINPUTS_MUST_BE_CONTAINED_IN_PARTITIONBY_CE9DCE6F = "prePartitionedInputs 必须包含在 partitionBy 中";
  public static final String EXCEPTION_CANNOT_HAVE_SORTED_MORE_SYMBOLS_THAN_THOSE_REQUESTED_52EEB7C9 = "不能包含多于请求数量的 sorted symbols";
  public static final String EXCEPTION_PRESORTEDORDERPREFIX_CAN_ONLY_BE_GREATER_THAN_ZERO_IF_ALL_PARTITION_SYMBOLS_ARE__76BBC8DD = "只有在所有 partition symbols 均已 pre-partitioned 时，preSortedOrderPrefix 才能大于 0";
  public static final String EXCEPTION_ORIGINALSTARTVALUE_MUST_BE_PRESENT_IF_STARTVALUE_IS_PRESENT_30B6FDFF = "startValue 存在时 originalStartValue 必须存在";
  public static final String EXCEPTION_FOR_FRAME_OF_TYPE_RANGE_COMMA_SORTKEYCOERCEDFORFRAMESTARTCOMPARISON_MUST_BE_PRES_7533A433 = "RANGE 类型 frame 中，startValue 存在时 sortKeyCoercedForFrameStartComparison 必须存在";
  public static final String EXCEPTION_ORIGINALENDVALUE_MUST_BE_PRESENT_IF_ENDVALUE_IS_PRESENT_E79EF3D2 = "endValue 存在时 originalEndValue 必须存在";
  public static final String EXCEPTION_FOR_FRAME_OF_TYPE_RANGE_COMMA_SORTKEYCOERCEDFORFRAMEENDCOMPARISON_MUST_BE_PRESEN_36A665AC = "RANGE 类型 frame 中，endValue 存在时 sortKeyCoercedForFrameEndComparison 必须存在";
  public static final String EXCEPTION_SUBQUERYASSIGNMENTS_IS_NULL_946CDC43 = "subqueryAssignments 不能为空";
  public static final String EXCEPTION_CORRELATION_IS_NULL_F8327EAD = "correlation 不能为空";
  public static final String EXCEPTION_ORIGINSUBQUERY_IS_NULL_8EFEB8D5 = "originSubquery 不能为空";
  public static final String EXCEPTION_INPUT_DOES_NOT_CONTAIN_SYMBOLS_FROM_CORRELATION_1B3DB7BF = "Input 不包含来自 correlation 的 symbols";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_2_NODES_25FE7927 = "期望 newChildren 包含 2 个 nodes";
  public static final String EXCEPTION_CHILDREN_IS_NULL_0CB43CE2 = "children 不能为空";
  public static final String EXCEPTION_OUTPUTTOINPUTS_IS_NULL_4125312B = "outputToInputs 不能为空";
  public static final String EXCEPTION_OUTPUTS_IS_NULL_EFD078A2 = "outputs 不能为空";
  public static final String EXCEPTION_MUST_HAVE_AT_LEAST_ONE_SOURCE_4114F454 = "必须至少有一个 source";
  public static final String EXCEPTION_EVERY_CHILD_NEEDS_TO_MAP_ITS_SYMBOLS_TO_AN_OUTPUT_ARG_OPERATION_SYMBOL_7FA107A2 = "每个 child 都需要将其 symbols 映射到输出 %s operation symbol";
  public static final String EXCEPTION_CHILD_DOES_NOT_PROVIDE_REQUIRED_SYMBOLS_B7CE956E = "Child 未提供必需 symbols";
  public static final String EXCEPTION_LEFTOUTPUTSYMBOLS_IS_NULL_083AE900 = "leftOutputSymbols 不能为空";
  public static final String EXCEPTION_RIGHTOUTPUTSYMBOLS_IS_NULL_F44B848F = "rightOutputSymbols 不能为空";
  public static final String EXCEPTION_FILTER_IS_NULL_8F83BD19 = "filter 不能为空";
  public static final String EXCEPTION_SPILLABLE_IS_NULL_8226EA70 = "spillable 不能为空";
  public static final String EXCEPTION_FILTER_MUST_BE_AN_EXPRESSION_OF_BOOLEAN_TYPE_COLON_ARG_F358F1A8 = "Filter 必须是 boolean 类型表达式：%s";
  public static final String EXCEPTION_LEFT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_LEFT_OUTPUT_SYMBOLS_71459E3B = "Left source inputs 不包含所有 left output symbols";
  public static final String EXCEPTION_RIGHT_SOURCE_INPUTS_DO_NOT_CONTAIN_ALL_RIGHT_OUTPUT_SYMBOLS_23EBC024 = "Right source inputs 不包含所有 right output symbols";
  public static final String EXCEPTION_EQUALITY_JOIN_CRITERIA_SHOULD_BE_NORMALIZED_ACCORDING_TO_JOIN_SIDES_COLON_ARG_76EB7C14 = "等值 join criteria 应按 join sides 规范化：%s";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_2_NODES_FOR_JOINNODE_BEEC3D82 = "期望 newChildren 包含 2 个 JoinNode 节点";
  public static final String EXCEPTION_MARKERSYMBOL_IS_NULL_0313F046 = "markerSymbol 不能为空";
  public static final String EXCEPTION_HASHSYMBOL_IS_NULL_1BD487F2 = "hashSymbol 不能为空";
  public static final String EXCEPTION_DISTINCTSYMBOLS_IS_NULL_28B8DB52 = "distinctSymbols 不能为空";
  public static final String EXCEPTION_DISTINCTSYMBOLS_CANNOT_BE_EMPTY_A44A693E = "distinctSymbols 不能为空";
  public static final String EXCEPTION_EXPRESSIONANDVALUEPOINTERS_IS_NULL_8A02F345 = "expressionAndValuePointers 不能为空";
  public static final String EXCEPTION_SKIPTOLABELS_IS_NULL_0A543C09 = "skipToLabels 不能为空";
  public static final String EXCEPTION_SKIPTOPOSITION_IS_NULL_EFBA10CA = "skipToPosition 不能为空";
  public static final String EXCEPTION_COLUMNNAMES_AND_OUTPUTSYMBOLS_SIZES_DON_QUOTE_T_MATCH_1BDF6B28 = "columnNames 和 outputSymbols 的大小不匹配";
  public static final String EXCEPTION_AGGREGATIONS_IS_NULL_CFE9CD2E = "aggregations 不能为空";
  public static final String EXCEPTION_GROUPINGSETS_IS_NULL_8EE6D9BF = "groupingSets 不能为空";
  public static final String EXCEPTION_PREGROUPEDSYMBOLS_IS_NULL_DC24FF7B = "preGroupedSymbols 不能为空";
  public static final String EXCEPTION_GLOBALGROUPINGSETS_IS_NULL_5B175B47 = "globalGroupingSets 不能为空";
  public static final String EXCEPTION_GROUPINGKEYS_IS_NULL_90D11F0C = "groupingKeys 不能为空";
  public static final String EXCEPTION_MASK_IS_NULL_5A7CEA49 = "mask 不能为空";
  public static final String EXCEPTION_SOURCE_IS_NULL_45946547 = "source 不能为空";
  public static final String EXCEPTION_STEP_IS_NULL_F83262DA = "step 不能为空";
  public static final String EXCEPTION_GROUPIDSYMBOL_IS_NULL_BFD3763D = "groupIdSymbol 不能为空";
  public static final String EXCEPTION_GROUPING_COLUMNS_DOES_NOT_CONTAIN_GROUPID_COLUMN_83976C83 = "Grouping columns 不包含 groupId column";
  public static final String EXCEPTION_ORDER_BY_DOES_NOT_SUPPORT_DISTRIBUTED_AGGREGATION_05109B26 = "ORDER BY 不支持分布式聚合";
  public static final String EXCEPTION_PRE_MINUS_GROUPED_SYMBOLS_MUST_BE_A_SUBSET_OF_THE_GROUPING_KEYS_AFC6C33D = "pre-grouped symbols 必须是 grouping keys 的子集";
  public static final String EXCEPTION_GROUPING_SET_COUNT_MUST_BE_LARGER_THAN_0_06FE609E = "grouping set 数量必须大于 0";
  public static final String EXCEPTION_LIST_OF_EMPTY_GLOBAL_GROUPING_SETS_MUST_BE_NO_LARGER_THAN_GROUPING_SET_COUNT_12DAD147 = "空 global grouping sets 列表大小不能大于 grouping set 数量";
  public static final String EXCEPTION_NO_GROUPING_KEYS_IMPLIES_AT_LEAST_ONE_GLOBAL_GROUPING_SET_COMMA_BUT_NONE_PROVIDE_F099B6D2 = "没有 grouping keys 意味着至少需要一个 global grouping set，但未提供";
  public static final String EXCEPTION_ARGUMENT_MUST_BE_SYMBOL_COLON_ARG_9C176D4D = "argument 必须为 symbol: %s";
  public static final String EXCEPTION_ARG_AGGREGATION_FUNCTION_ARG_HAS_ARG_ARGUMENTS_COMMA_BUT_ARG_ARGUMENTS_WERE_PROV_53CC06CF = "%s aggregation function %s 有 %s 个参数，但函数调用提供了 %s 个参数";
  public static final String EXCEPTION_IDCOLUMN_IS_NULL_FA206D71 = "idColumn 不能为空";
  public static final String EXCEPTION_EXPECTED_NEWCHILDREN_TO_CONTAIN_1_NODE_7A97D180 = "期望 newChildren 包含 1 个 node";
  public static final String EXCEPTION_SOURCEJOINSYMBOL_IS_NULL_A88AEC85 = "sourceJoinSymbol 不能为空";
  public static final String EXCEPTION_FILTERINGSOURCEJOINSYMBOL_IS_NULL_502C5BD5 = "filteringSourceJoinSymbol 不能为空";
  public static final String EXCEPTION_SEMIJOINOUTPUT_IS_NULL_D961A39B = "semiJoinOutput 不能为空";
  public static final String EXCEPTION_SOURCE_DOES_NOT_CONTAIN_JOIN_SYMBOL_FA572FD2 = "Source 不包含 join symbol";
  public static final String EXCEPTION_FILTERING_SOURCE_DOES_NOT_CONTAIN_FILTERING_JOIN_SYMBOL_0C7B1BE3 = "Filtering source 不包含 filtering join symbol";
  public static final String EXCEPTION_REHASHMEMORYRESERVATION_IS_NEGATIVE_7EFEFCC2 = "rehashMemoryReservation 不能为负数";
  public static final String EXCEPTION_IDENTIFIER_CANNOT_BE_EMPTY_OR_NULL_9C70B87D = "Identifier 不能为空或 null";
  public static final String EXCEPTION_COLUMN_ALIAS_ARG_IS_AMBIGUOUS_AT_POSITIONS_ARG_F6FF2E93 = "列别名 '%s' 在位置 %s 处存在歧义";
  public static final String EXCEPTION_INVALID_ARGUMENT_ARG_EXPECTED_SCALAR_ARGUMENT_4AD4E32F = "无效的参数 %s。期望标量参数";
  public static final String EXCEPTION_THE_SLIDE_ARGUMENT_MUST_HAVE_THE_SAME_WINDOW_MODE_AS_THE_SIZE_ARGUMENT_419C9844 = "SLIDE 参数必须与 SIZE 参数具有相同的窗口模式。";
  public static final String EXCEPTION_THE_ORIGIN_ARGUMENT_IS_ONLY_SUPPORTED_IN_TIME_WINDOW_MODE_B3F358B9 = "ORIGIN 参数仅在时间窗口模式下受支持。";
  public static final String EXCEPTION_THE_ORDER_BY_CLAUSE_OF_THE_DATA_ARGUMENT_MUST_SORT_THE_TIME_COLUMN_IN_ASCENDING_ORDER_C3FD0F6A = "DATA 参数的 ORDER BY 子句必须按时间列升序排序。";
  public static final String EXCEPTION_ARGUMENTS_SHOULD_NEVER_BE_EMPTY_WHEN_RESOLVING_M4_MODE_F982EA1C = "解析 M4 模式时参数不应为空";
  public static final String EXCEPTION_MISSING_REQUIRED_ARGUMENT_ARG_1308D0DC = "缺少必要参数：%s";
  public static final String EXCEPTION_UNKNOWN_ARGUMENT_ARG_F40E9394 = "未知参数：%s";

}
