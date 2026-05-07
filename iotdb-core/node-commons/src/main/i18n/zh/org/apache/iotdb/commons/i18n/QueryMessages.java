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
  public static final String INVALID_ROW_PATTERN_INPUT = "无效的输入：%s";

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
  public static final String MULTIPLE_AI_NODE_SERVICE_PROVIDER =
      "发现多个 ITableFunctionAINodeServiceProvider";

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
  public static final String TIME_PRECISION_INVALID =
      "时间精度必须为以下之一：h,m,s,ms,u,n";
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
  public static final String HASH_TABLE_SIZE_EXCEEDED =
      "哈希表大小不能超过 10 亿条记录";
  public static final String INVALID_INPUT_BYTES_LENGTH_LT_8 =
      "无效输入：bytes.length - offset < 8";
  public static final String INVALID_INPUT_BYTES_LENGTH_LT_4 =
      "无效输入：bytes.length - offset < 4";
  public static final String INVALID_INPUT_DESC_LENGTH_LT_4 =
      "无效输入：desc.length - offset < 4";
  public static final String NO_CHANNEL_GROUP_BY_HASH_APPEND_VALUES_TO =
      "NoChannelGroupByHash 不支持 appendValuesTo 操作";
  public static final String NO_CHANNEL_GROUP_BY_HASH_GET_RAW_HASH =
      "NoChannelGroupByHash 不支持 getRawHash 操作";
  public static final String MIN_GREATER_THAN_MAX = "%s > %s";
  public static final String LONG_OVERFLOW = "long 溢出";
  public static final String INTEGER_OVERFLOW = "integer 溢出";
  public static final String INVALID_INPUT_DESC_LENGTH_LT_8 =
      "无效输入：desc.length - offset < 8";

  private QueryMessages() {}

  public static final String TIME_PRECISION_MUST_BE_ONE_OF = "时间精度必须是以下之一：h,m,s,ms,u,n";
  public static final String QUERY_INVALID_INPUT = "无效输入：";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_4 = "无效输入：bytes.length - offset < 4";
  public static final String INVALID_INPUT_BYTES_OFFSET_LT_8 = "无效输入：bytes.length - offset < 8";
  public static final String INVALID_INPUT_DESC_OFFSET_LT_4 = "无效输入：desc.length - offset < 4";
  public static final String SIZE_OF_HASH_TABLE_CANNOT_EXCEED = "哈希表大小不能超过 10 亿条目";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_APPEND = "NoChannelGroupByHash 不支持 appendValuesTo";
  public static final String NO_CHANNEL_GROUP_BY_HASH_NOT_SUPPORT_RAW_HASH = "NoChannelGroupByHash 不支持 getRawHash";
  public static final String MULTIPLE_TABLE_FUNCTION_AI_NODE_PROVIDER = "找到多个 ITableFunctionAINodeServiceProvider";
}
