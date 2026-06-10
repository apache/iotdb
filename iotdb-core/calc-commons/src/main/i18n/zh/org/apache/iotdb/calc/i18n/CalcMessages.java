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
      "AND 运算符只接受布尔操作数";
  public static final String ARRAYS_NOT_SAME_LENGTH = "数组长度不一致";
  public static final String CANNOT_ADD_NAN_TO_T_DIGEST = "不能将 NaN 添加到 t-digest";
  public static final String CANNOT_CAST_TO_BOOLEAN = "\"%s\" 无法转换为 [BOOLEAN]";
  public static final String CANNOT_PARSE_STRING_TO_DOUBLE = "无法将字符串解析为 double：";
  public static final String CANT_HAPPEN_LOOP_FELL_THROUGH = "不应发生……循环穿透";
  public static final String COUNT_ALL_ACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS =
      "CountAllAccumulator 不支持统计信息。";
  public static final String COUNT_IF_ACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS =
      "CountIfAccumulator 不支持统计信息";
  public static final String CURRENT_COLUMN_IS_NOT_OBJECT_COLUMN = "当前列不是对象列";
  public static final String CURRENT_TS_BLOCK_SIZE_IS = "当前 tsBlock 大小为：{}";
  public static final String DATA_TYPE_CANNOT_BE_ORDERED = "数据类型：%s 不能排序";
  public static final String DECODE_BASE32_ERROR = "base32 解码错误";
  public static final String DECODE_BASE64_ERROR = "base64 解码错误";
  public static final String DECODE_BASE64URL_ERROR = "base64url 解码错误";
  public static final String DECODE_HEX_ERROR = "hex 解码错误";
  public static final String DENSE_RANK_NOT_YET_IMPLEMENTED = "DENSE_RANK 尚未实现";
  public static final String DISTINCT_AGGREGATION_FUNCTION_CANNOT_BE_PUSH_DOWN =
      "DISTINCT 聚合函数不能下推";
  public static final String DIVISION_BY_ZERO = "除以零";
  public static final String ESCAPE_STRING_MUST_BE_A_SINGLE_CHARACTER =
      "转义字符串必须是单个字符";
  public static final String EXCEPTION_HAPPENED_WHEN_EXECUTING_UDTF =
      "执行 UDTF 时发生异常：";
  public static final String FAIL_TO_CLOSE_FILE_CHANNEL = "关闭文件通道失败";
  public static final String ILLEGAL_STATE_IN_VISIT_LOGICAL_EXPRESSION =
      "visitLogicalExpression 中状态非法";
  public static final String INDEX_OUT_OF_PARTITION_BOUNDS = "索引超出分区边界！";
  public static final String INITIAL_CAPACITY_IS_NEGATIVE = "初始容量 (%d) 为负数";
  public static final String INITIAL_CAPACITY_EXCEEDS_LIMIT = "初始容量 (%d) 超过 %d";
  public static final String INPUT_ROW_UTILS_SHOULD_NOT_BE_INSTANTIATED =
      "InputRowUtils 不应被实例化。";
  public static final String INVALID_AGGREGATION_FUNCTION = "无效的聚合函数：";
  public static final String INVALID_TEXT_INPUT_FOR_BOOLEAN =
      "布尔类型的文本输入无效：%s";
  public static final String INVALID_VALUE = "无效的值：%f";
  public static final String LEFT_CHILD_OF_JOIN_NODE_DOESNT_CONTAIN_LEFT_JOIN_KEY =
      "JoinNode 的左子节点不包含左连接键。";
  public static final String MAX_TUPLE_SIZE_OF_TS_BLOCK_IS = "maxTupleSizeOfTsBlock 为：{}";
  public static final String MEMORY_IS_NOT_ENOUGH_FOR_CURRENT_QUERY = "当前查询内存不足。";
  public static final String MERGE_SORT_HEAP_SHOULD_BE_EMPTY = "归并排序堆应为空！";
  public static final String MODULUS_BY_ZERO = "对零取模";
  public static final String MULTIPLE_I_OBJECT_FILE_SERVICE_PROVIDER_FOUND =
      "找到多个 IObjectFileServiceProvider";
  public static final String MULTIPLE_I_TEMPORARY_QUERY_DATA_FILE_SERVICE_PROVIDER_FOUND =
      "找到多个 ITemporaryQueryDataFileServiceProvider";
  public static final String NOT_ENOUGH_MEMORY_FOR_SORTING = "排序内存不足";
  public static final String NOT_YET_IMPLEMENTED = "尚未实现";
  public static final String NO_I_OBJECT_FILE_SERVICE_PROVIDER_FOUND =
      "未找到 IObjectFileServiceProvider";
  public static final String NO_I_TEMPORARY_QUERY_DATA_FILE_SERVICE_PROVIDER_FOUND =
      "未找到 ITemporaryQueryDataFileServiceProvider";
  public static final String OFFSET_LESS_THAN_ZERO = "偏移量 %d 小于 0。";
  public static final String ONLY_ONE_TUPLE_CAN_BE_SENT_EACH_TIME =
      "由于内存限制，每次只能发送一个元组，oneTupleSize: {}B, maxReturnSize: {}B";
  public static final String OR_OPERATOR_ONLY_ACCEPTS_BOOLEAN_OPERANDS =
      "OR 运算符只接受布尔操作数";
  public static final String PERCENTAGE_SHOULD_BE_IN_0_1 = "百分比应在 [0,1] 范围内，实际为 ";
  public static final String READ_OBJECT_IS_NOT_SUPPORTED = "不支持 readObject";
  public static final String RESULT_TS_BLOCK_CANNOT_BE_NULL = "结果 tsBlock 不能为空";
  public static final String RIGHT_CHILD_OF_JOIN_NODE_DOESNT_CONTAIN_RIGHT_JOIN_KEY =
      "JoinNode 的右子节点不包含右连接键。";
  public static final String SHOULD_CALL_THE_CONCRETE_VISIT_XX_METHOD =
      "应该调用具体的 visitXX() 方法";
  public static final String STATE_FOR_GROUP_NOT_FOUND = "未找到分组 %d 的状态";
  public static final String SUM_SHOULD_NEVER_BE_ZERO = "sum 不应为零。";
  public static final String THIS_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVING_INPUTS =
      "该累加器不支持移除输入！";
  public static final String THIS_IS_A_UTILITY_CLASS_AND_CANNOT_BE_INSTANTIATED =
      "这是一个工具类，不能被实例化";
  public static final String TYPE_OF_LEFT_ASOF_JOIN_KEY_IS_NOT_TIMESTAMP =
      "左侧 ASOF 连接键的类型不是 TIMESTAMP";
  public static final String TYPE_OF_RIGHT_ASOF_JOIN_KEY_IS_NOT_TIMESTAMP =
      "右侧 ASOF 连接键的类型不是 TIMESTAMP";
  public static final String UDAF_NOT_SUPPORT_CALCULATE_FROM_STATISTICS =
      "UDAF 目前不支持从统计信息计算";
  public static final String UNBOUND_FOLLOWING_NOT_ALLOWED_IN_FRAME_START =
      "帧起始位置不允许使用 UNBOUND FOLLOWING！";
  public static final String UNBOUND_PRECEDING_NOT_ALLOWED_IN_FRAME_END =
      "帧结束位置不允许使用 UNBOUND PRECEDING！";
  public static final String UNBOUND_PRECEDING_NOT_ALLOWED_IN_FRAME_START =
      "帧起始位置不允许使用 UNBOUND PRECEDING！";
  public static final String UNEXPECTED_ANCHOR_TYPE = "意外的锚点类型：";
  public static final String UNEXPECTED_EXTRACT_FIELD = "意外的提取字段：";
  public static final String UNEXPECTED_SIZE_FOR_LAST_SEQUENCE = "最后一个序列的大小异常：";
  public static final String UNEXPECTED_SKIP_TO_POSITION = "意外的 SKIP TO 位置：";
  public static final String UNEXPECTED_VALUE_FOR_REMAINDER = "余数的值异常：";
  public static final String UNEXPECTED_VALUE_FOR_SEQUENCES = "序列的值异常：";
  public static final String UNHANDLED_LITERAL_TYPE = "未处理的字面量类型：";
  public static final String UNKNOWN_DATA_TYPE = "未知数据类型：";
  public static final String UNKNOWN_DATATYPE = "未知数据类型：";
  public static final String UNKNOWN_RANKING_TYPE = "未知排名类型：";
  public static final String UNKNOWN_SIGN = "未知符号：";
  public static final String UNKNOWN_TYPE = "未知类型：";
  public static final String UNREACHABLE = "不可达！";
  public static final String UNSUPPORTED_ARITHMETIC_OPERATOR = "不支持的算术运算符：";
  public static final String UNSUPPORTED_ASOF_JOIN_TYPE = "不支持的 ASOF 连接类型：";
  public static final String UNSUPPORTED_CAST_TO_TYPE = "不支持的转换目标类型：";
  public static final String UNSUPPORTED_COLUMN_TRANSFORMER = "不支持的 ColumnTransformer";
  public static final String UNSUPPORTED_COMPARISON_OPERATOR = "不支持的比较运算符：";
  public static final String UNSUPPORTED_DATA_TYPE = "不支持的数据类型：";
  public static final String UNSUPPORTED_DATA_TYPE_LOWER = "不支持的数据类型：";
  public static final String UNSUPPORTED_FRAME_BOUND_TYPE = "不支持的帧边界类型：";
  public static final String UNSUPPORTED_FUNCTION_KIND = "不支持的函数类型：";
  public static final String UNSUPPORTED_JOIN_TYPE = "不支持的连接类型：";
  public static final String UNSUPPORTED_LOGICAL_OPERATOR = "不支持的逻辑运算符：";
  public static final String UNSUPPORTED_TYPE = "不支持的类型：";
  public static final String UNSUPPORTED_TYPE_BINARY = "不支持的类型";
  public static final String UNSUPPORTED_TYPE_CLASS = "不支持的类型：";
  public static final String UNSUPPORTED_TYPE_FOR_ARITHMETIC_OPERATION =
      "不支持的算术运算类型：";
  public static final String UNSUPPORTED_TYPE_IN_GENERIC_LITERAL =
      "GenericLiteral 中不支持的类型：";
  public static final String WEIGHT_MUST_BE_GE_1 = "权重必须 >= 1，实际为 ";
  public static final String PUSHED_ELEMENT_IS_NULL =
      "推入的元素为 null";

  public static final String FAILED_TO_DELETE_TEMP_DIR = "删除临时目录 {} 失败。";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_WRONG_OBJECT_FILE_PATH_D317B1AB = "wrong object 文件 路径: ";
  public static final String EXCEPTION_OFFSET_ARG_GREATER_THAN_EQUAL_OBJECT_SIZE_ARG_FILE_PATH_31936A19 = "offset %d is greater than or equal to object size %d, 文件 路径 is %s";
  public static final String EXCEPTION_READ_OBJECT_SIZE_ARG_TOO_LARGE_SIZE_2G_FILE_PATH_7D4A4D10 = "读取 object size %s is too large (size > 2G), 文件 路径 is %s";
  public static final String LOG_FAILED_CLOSE_FILE_METHOD_DEREGISTER_ARG_BECAUSE_ARG_1744AC60 = "无法关闭 文件 in method de注册(%s),，原因：%s";
  public static final String LOG_FAILED_CLEAN_DIR_METHOD_DEREGISTER_ARG_BECAUSE_ARG_F53193E5 = "无法clean dir in method de注册(%s),，原因：%s";
  public static final String EXCEPTION_TYPE_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVE_INPUT_905AFAA1 = "This type of accumulator 不support 移除 input!";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_VARIANCE_ARG_17A45959 = "不支持的data type in aggregation variance : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_0A19C2A8 = "不支持的data type in CentralMoment Aggregation: ";
  public static final String EXCEPTION_UNKNOWN_FUNCTION_ARG_NODE_ARG_927DA6A7 = "未知的function %s on 节点: %d.";
  public static final String EXCEPTION_INLIST_LITERAL_TIMESTAMP_CAN_ONLY_LONGLITERAL_DOUBLELITERAL_GENERICLITERAL_CURRENT_A3105E67 = "InList Literal for TIMESTAMP can only be LongLiteral, DoubleLiteral and GenericLiteral, 当前is ";
  public static final String EXCEPTION_PARTITION_HANDLER_FINISHED_CANNOT_ADD_MORE_DATA_D8E31A3E = "The partition handler is finished, cannot add more data.";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_PATTERN_VARIABLE_NOT_PRESENT_MATCH_2891B276 = "AFTER MATCH SKIP TO 失败: pattern variable 不是present in match";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_CANNOT_SKIP_FIRST_ROW_MATCH_71D4093B = "AFTER MATCH SKIP TO 失败: cannot skip to first row of match";
  public static final String EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_COPYING_3E03976C = "aggregate function %s 不support copying";
  public static final String EXCEPTION_UNSUPPORTED_BUILT_WINDOW_FUNCTION_NAME_F994ED27 = "不支持的built-in window function name: ";
  public static final String EXCEPTION_UNSUPPORTED_DEFAULT_VALUE_S_DATA_TYPE_LAG_BEB00511 = "不支持的default value's data type in Lag: ";
  public static final String EXCEPTION_CANNOT_COMPARE_VALUES_DIFFERENT_TYPES_A95EDA7F = "无法compare values of different types: ";
  public static final String EXCEPTION_VS_AEDAB253 = " vs. ";
  public static final String EXCEPTION_TARGET_TYPE_NOT_GENERICDATATYPE_CB87EF37 = "Target type 不是a GenericDataType: ";
  public static final String EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_5C50C92E = "不支持的expression type: ";
  public static final String EXCEPTION_IDENTITYLINEARFILL_S_NEEDPREPAREFORNEXT_METHOD_SHOULD_ALWAYS_RETURN_FALSE_FF7AD73B = "IdentityLinearFill's needPrepareForNext() method 应always return false.";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_ARG_37F52124 = "不支持的data type in LAST: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_2FCF5ADA = "不支持的data type in LAST Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_B3F2EE42 = "不支持的data type in LAST aggregation: %s";
  public static final String EXCEPTION_SIZE_CHILDRENCOLUMNS_DATATYPES_SHOULD_SAME_6EA43EA8 = "The size of childrenColumns and dataTypes 应be the same.";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_8D31C6CA = "不支持的data type in FIRST_BY Aggregation: %s";
  public static final String EXCEPTION_APPROX_PERCENTILE_REQUIRES_2_3_ARGUMENTS_BUT_GOT_ARG_D78590AA = "APPROX_PERCENTILE requires 2 or 3 arguments, but got %d";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_PERCENTILE_AGGREGATION_ARG_CFEC0431 = "不支持的data type in APPROX_PERCENTILE Aggregation: %s";
  public static final String EXCEPTION_APPROXPERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_2BB01365 = "ApproxPercentileAccumulator 不support statistics";
  public static final String EXCEPTION_NO_EXACT_DOUBLE_REPRESENTATION_LONG_ARG_3E5B7550 = "no exact double representation for long: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_COVARIANCE_AGGREGATION_ARG_643BEDD0 = "不支持的data type in Covariance Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_8C41FBCC = "不支持的data type in LAST_BY Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_ARG_74B9A3A8 = "不支持的data type in CentralMoment Aggregation: %s";
  public static final String EXCEPTION_SECOND_THIRD_ARGUMENT_MUST_GREATER_THAN_0_BUT_GOT_K_0AFFFA6A = "The second and third argument 必须be greater than 0, but got k=";
  public static final String EXCEPTION_CAPACITY_E92593AD = ", capacity=";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_AVG_ARG_4E63A3C3 = "不支持的data type in aggregation AVG : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CORRELATION_AGGREGATION_ARG_2FF29597 = "不支持的data type in Correlation Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_ARG_D5B6C6F9 = "不支持的data type in FIRST: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_B2D27BB9 = "不支持的data type in FIRST Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_2652C363 = "不支持的data type in FIRST aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_VARIANCE_AGGREGATION_ARG_C641D425 = "不支持的data type in VARIANCE Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_7D59F7B2 = "不支持的data type : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_COUNT_DISTINCT_AGGREGATION_ARG_58F0391E = "不支持的data type in APPROX_COUNT_DISTINCT Aggregation: %s";
  public static final String EXCEPTION_APPROXCOUNTDISTINCTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_81005A79 = "ApproxCountDistinctAccumulator 不support statistics";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_AGGREGATION_ARG_CCB09C60 = "不支持的data type in MAX Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_SUM_AGGREGATION_ARG_92F5A18D = "不支持的data type in SUM Aggregation: %s";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_8CA29DC2 = "distinct values has exceeded the threshold %s when calculate Mode";
  public static final String EXCEPTION_APPROXMOSTFREQUENTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_DB1E218B = "ApproxMostFrequentAccumulator 不support statistics";
  public static final String EXCEPTION_DISTINCT_AGGREGATION_FUNCTION_STATE_CAN_NOT_COPIED_34D0A276 = "Distinct aggregation function state 不能be copied";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_REGRESSION_AGGREGATION_ARG_7BB08DA2 = "不支持的data type in Regression Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MIN_AGGREGATION_ARG_CB73B158 = "不支持的data type in MIN Aggregation: %s";
  public static final String EXCEPTION_INVALID_FORMAT_SERIALIZED_HISTOGRAM_GOT_ENCODING_A8E50EA5 = "无效的format for 序列化d histogram, got encoding: ";
  public static final String EXCEPTION_MAX_STANDARD_ERROR_MUST_ARG_ARG_ARG_EB8443D6 = "Max Standard 错误 必须be in [%s, %s]: %s";
  public static final String EXCEPTION_CANNOT_MERGE_HYPERLOGLOG_INSTANCES_DIFFERENT_PRECISION_9DCF13BC = "无法merge HyperLogLog instances with different precision";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_ONE_GROUP_A3F5A1D3 = "distinct values has exceeded the threshold %s when calculate MODE in one group";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_MIN_AGGREGATION_ARG_982BEBD5 = "不支持的data type in MAX_BY/MIN_BY Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_AF4DA98F = "不支持的data type EXTREME Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_276BE220 = "不支持的data type in EXTREME Aggregation: %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AVG_AGGREGATION_ARG_66BF3128 = "不支持的data type in AVG Aggregation: %s";
  public static final String EXCEPTION_CAN_T_READ_NEW_TSBLOCK_FILESPILLERREADER_072AF71D = "Can't 读取 a new tsBlock in 文件Spiller读取er: ";
  public static final String EXCEPTION_CAN_T_CLOSE_FILECHANNEL_FILESPILLERREADER_E46979F0 = "Can't 关闭 文件Channel in 文件Spiller读取er: ";
  public static final String EXCEPTION_CREATE_FILE_ERROR_B8B379CF = "创建 文件 错误: ";
  public static final String EXCEPTION_CAN_T_WRITE_INTERMEDIATE_SORTED_DATA_FILE_0027961E = "Can't 写入 intermediate sorted data to 文件: ";
  public static final String EXCEPTION_CAN_T_GET_FILE_FILESPILLERREADER_CHECK_IF_FILE_EXISTS_DEED83D9 = "Can't get 文件 for 文件Spiller读取er, check if the 文件 exists: ";
  public static final String EXCEPTION_LONG_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B3F9016B = "long value %d is out of range of integer value.";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B0E6DDED = "Float value %f is out of range of integer value.";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_LONG_VALUE_62F8153E = "Float value %f is out of range of long value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_BAB52E11 = "Double value %f is out of range of integer value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_LONG_VALUE_5793A91E = "Double value %f is out of range of long value.";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_DB914FA0 = "Double value %f is out of range of float value.";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_D171B313 = "Text value %s is out of range of float value.";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_DOUBLE_VALUE_C0589D83 = "Text value %s is out of range of double value.";
  public static final String EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_ARG_678B759C = "不支持的source dataType: %s";
  public static final String EXCEPTION_CANNOT_CAST_ARG_ARG_TYPE_8266A2C6 = "无法cast %s to %s type";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_NUM_MUST_REPRESENTABLE_BITS_SPECIFIED_ARG_241AD2FE =
      "Argument 异常, the scalar function num 必须be representable with the bits specified. %s cannot be"
      + " represented with %s bits.";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_BIT_COUNT_BITS_MUST_BETWEEN_2_6A365911 = "Argument 异常, the scalar function bit_count bits 必须be between 2 and 64.";
  public static final String LOG_ERROR_OCCURRED_DURING_EXECUTING_UDTF_PLEASE_CHECK_WHETHER_IMPLEMENTATION_UDF_7A719D38 =
      "错误 occurred during executing UDTF, please check whether the implementation of UDF is correct"
      + " according to the udf-api description.";
  public static final String EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDTF_ARG_ARG_PLEASE_CHECK_WHETHER_4E706370 =
      "错误 occurred during executing UDTF#%s: %s, please check whether the implementation of UDF is"
      + " correct according to the udf-api description.";
  public static final String EXCEPTION_UNSUPPORTED_TYPE_FF7F518D = "不支持的Type: ";
  public static final String EXCEPTION_LOGICALORMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_E3A2B2FA = "LogicalOrMultiColumnTransformer 不support doTransform with selection";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E = "不支持的data type: %s";
  public static final String EXCEPTION_LOGICALANDMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_2507527D = "LogicalAndMultiColumnTransformer 不support doTransform with selection";
  public static final String EXCEPTION_ERROR_OCCURS_EVALUATING_USER_DEFINED_SCALAR_FUNCTION_05903C18 = "错误 occurs when evaluating 用户-defined scalar function ";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_MD5_INVALID_INPUT_FORMAT_EMPTY_KEY_DED1C525 = "无法execute function hmac_md5，原因：an 无效的input format, the 为空 key 不是allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA1_INVALID_INPUT_FORMAT_EMPTY_KEY_518063E5 = "无法execute function hmac_sha1，原因：an 无效的input format, the 为空 key 不是allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA256_INVALID_INPUT_FORMAT_EMPTY_KEY_2F6AD64E = "无法execute function hmac_sha256，原因：an 无效的input format, the 为空 key 不是allowed in HMAC operation.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA512_INVALID_INPUT_FORMAT_EMPTY_KEY_3671AF09 = "无法execute function hmac_sha512，原因：an 无效的input format, the 为空 key 不是allowed in HMAC operation.";
  public static final String EXCEPTION_INVALID_FORMAT_STRING_ARG_ARG_05853138 = "无效的format string: %s (%s)";
  public static final String EXCEPTION_YEAR_MUST_BETWEEN_1000_9999_8FBB94AA = "Year 必须be between 1000 and 9999.";
  public static final String EXCEPTION_UNSUPPORTED_PRECISION_CDB58979 = "不支持的precision: ";
  public static final String EXCEPTION_UNKNOWN_PRECISION_0119BEB0 = "未知的precision: ";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_LENGTH_MUST_NOT_LESS_THAN_072B7355 = "Argument 异常,the scalar function substring length 必须not be less than 0";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_MUST_NOT_GREATER_THAN_F0BA2A56 =
      "Argument 异常,the scalar function substring beginPosition 必须not be greater than the string"
      + " length";
  public static final String EXCEPTION_TYPE_DB38663D = "type ";
  public static final String EXCEPTION_NOT_SUPPORTED_GENERATEPROBLEMATICVALUESTRING_624BB179 = " 不是supported in generateProblematicValueString()";
  public static final String EXCEPTION_BASE64_LENGTH_MUST_MULTIPLE_4_INCLUDING_PADDING_8717829D = "Base64 length 必须be a multiple of 4 (including padding '=')";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_32_MUST_4_C1214BB5 = "The length of the input BLOB of function from_big_endian_32 必须be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_64_MUST_8_797F1A26 = "The length of the input BLOB of function from_big_endian_64 必须be 8.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_32_MUST_4_0FBBD083 = "The length of the input BLOB of function from_little_endian_32 必须be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_64_MUST_8_771F7A96 = "The length of the input BLOB of function from_little_endian_64 必须be 8.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_32_BIG_ENDIAN_MUST_4_51AF4EF0 = "The length of the input BLOB of function from_ieee754_32_big_endian 必须be 4.";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_64_BIG_ENDIAN_MUST_8_56F33455 = "The length of the input BLOB of function from_ieee754_64_big_endian 必须be 8.";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_INVALID_TARGET_SIZE_5054AE0B =
      "无法execute function '%s'，原因：the value %s corresponding to a 无效的target size, the allowed range"
      + " is [0, %d].";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_EMPTY_PADDING_STRING_F1CE573C = "无法execute function '%s' due the value %s corresponding to a 为空 padding string.";
  public static final String EXCEPTION_FOUND_NO_COLUMN_ARG_ARG_8CF632F7 = "Found no column %s in %s";
  public static final String EXCEPTION_SORT_ITEM_ARG_NOT_INCLUDED_CHILDREN_S_OUTPUT_COLUMNS_CC911999 = "Sort Item %s 不是included in children's output columns";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFTOUTPUTSYMBOL_0C8AA216 = "Left child of Join节点 doesn't contain LeftOutputSymbol ";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHTOUTPUTSYMBOL_10A86F63 = "Right child of Join节点 doesn't contain RightOutputSymbol ";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFT_ASOF_MAIN_JOIN_2850234A = "Left child of Join节点 doesn't contain left ASOF main join key.";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHT_ASOF_MAIN_JOIN_1A9631C9 = "Right child of Join节点 doesn't contain right ASOF main join key.";
  public static final String EXCEPTION_JOIN_KEY_TYPE_MISMATCH_LEFT_JOIN_KEY_TYPE_072E692E = "Join key type mismatch. Left join key type: ";
  public static final String EXCEPTION_RIGHT_JOIN_KEY_TYPE_56895767 = ", right join key type: ";
  public static final String EXCEPTION_ROW_COUNT_MUST_0_1_8D44189F = "Row count 必须be 0 or 1";
  public static final String EXCEPTION_COLUMN_ARG_CANNOT_RESOLVED_508579EC = "Column '%s' cannot be resolved.";
  public static final String EXCEPTION_REQUESTED_VALUE_UNKNOWN_CAPTURE_WAS_IT_REGISTERED_PATTERN_C77DF7E9 = "Requested value for 未知的Capture. Was it 注册ed in the Pattern?";
  public static final String EXCEPTION_TARGET_NODE_INDEX_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_75BFB7C3 = "Target node index 必须为 greater than 或 equal 到 one";
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
  public static final String EXCEPTION_INVALID_STARTINGPOSITION_COLON_ARG_7B4D49C0 = "无效的 startingPosition: %s";
  public static final String EXCEPTION_ROWID_DOES_NOT_MATCH_THIS_TSBLOCK_18F9EB4D = "rowId does not match this TsBlock";
  public static final String EXCEPTION_CURSOR_STILL_ACTIVE_E9462D3B = "Cursor still active";
  public static final String EXCEPTION_NO_ACTIVE_CURSOR_C89C9944 = "No active cursor";
  public static final String EXCEPTION_NOT_YET_ADVANCED_1600C5A7 = "Not yet advanced";
  public static final String EXCEPTION_SHOULD_NOT_ATTEMPT_COMPACTION_WHEN_TSBLOCK_IS_LOCKED_DF775E6B = "Should not attempt compaction when TsBlock is locked";
  public static final String EXCEPTION_STRATEGY_IS_NULL_DDA2C67F = "strategy 不能为空";
  public static final String EXCEPTION_ROWIDEVICTIONLISTENER_IS_NULL_457EFBEE = "rowIdEvictionListener 不能为空";
  public static final String EXCEPTION_TOPN_MUST_BE_GREATER_THAN_ZERO_43866D3E = "topN 必须为 greater than zero";
  public static final String EXCEPTION_NO_ROOT_TO_PEEK_43C3D8D5 = "No root 到 peek";
  public static final String EXCEPTION_GROUP_ID_HAS_AN_EMPTY_HEAP_C9EDB841 = "Group ID has an empty heap";
  public static final String EXCEPTION_POPANDINSERT_LEFT_PAREN_RIGHT_PAREN_REQUIRES_AT_LEAST_A_ROOT_NODE_0CA0DD58 = "popAndInsert() requires at least a root node";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext 不能为空";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "child operator 不能为空";
  public static final String EXCEPTION_LIMIT_MUST_BE_AT_LEAST_ZERO_E960530F = "limit 必须为 at least zero";
  public static final String EXCEPTION_OFFSET_MUST_BE_AT_LEAST_ZERO_21BB6BB6 = "offset 必须为 at least zero";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks 不能为空";
  public static final String EXCEPTION_FILLARRAY_SHOULD_NOT_BE_NULL_OR_EMPTY_118FB134 = "fillArray 不能为空 或 empty";
  public static final String EXCEPTION_UNIQUE_ROW_ID_EXCEEDS_A_LIMIT_COLON_ARG_71EE4B7D = "Unique row id exceeds a limit: %s";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_FOR_PREVIOUSFILLWITHGROUPOPERATOR_SHOULD_NEVER_BE_NEGATIVE_ABA9C2B1 = "helperColumnIndex for PreviousFillWithGroupOperator should never be negative";
  public static final String EXCEPTION_OUTPUTCOLUMNCOUNT_IS_NOT_EQUAL_TO_VALUE_COLUMN_COUNT_OF_CHILD_OPERATOR_QUOTE_S_T_8E30BAD8 = "outputColumnCount is not equal 到 value column count of child operator's TsBlock";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_SHOULD_BE_RESOLVED_WHEN_TIMEBOUND_EXISTS_01BE233B = "helperColumnIndex should be resolved when timeBound exists";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "type 不能为空";
  public static final String EXCEPTION_LOGICALINDEXNAVIGATION_IS_NULL_3B31CD23 = "logicalIndexNavigation 不能为空";
  public static final String EXCEPTION_OVERRIDING_AGGREGATIONS_FOR_CHILD_THREAD_9C84E47D = "overriding aggregations for child thread";
  public static final String EXCEPTION_SKIP_TO_NAVIGATION_IS_MISSING_FOR_SKIP_TO_ARG_8A303D73 = "skip 到 navigation is missing for SKIP TO %s";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "labels 不能为空";
  public static final String EXCEPTION_LOGICAL_OFFSET_MUST_BE_GREATER_THAN_EQUALS_0_COMMA_ACTUAL_COLON_ARG_539807BF = "logical offset 必须为 >= 0, actual: %s";
  public static final String EXCEPTION_CURRENT_ROW_IS_OUT_OF_BOUNDS_OF_THE_MATCH_D8D8B611 = "current row is out of bounds of the match";
  public static final String EXCEPTION_EVALUATIONS_IS_NULL_F3F72380 = "evaluations 不能为空";
  public static final String EXCEPTION_PARTITION_IS_NULL_27FD9756 = "partition 不能为空";
  public static final String EXCEPTION_LABELNAMES_IS_NULL_EEFDB807 = "labelNames 不能为空";
  public static final String EXCEPTION_BOUNDSIGNATURE_IS_NULL_D1077180 = "boundSignature 不能为空";
  public static final String EXCEPTION_ACCUMULATO_IS_NULL_D51F532F = "accumulato 不能为空";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IS_NULL_82EC5B1C = "patternAggregationTracker 不能为空";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IN_INCONSISTENT_STATE_C6FF5294 = "PatternAggregationTracker in inconsistent state";
  public static final String EXCEPTION_SETEVALUATOR_IN_INCONSISTENT_STATE_D5FAA900 = "SetEvaluator in inconsistent state";
  public static final String EXCEPTION_ACCUMULATOR_IS_NULL_EF0C1DFF = "accumulator 不能为空";
  public static final String EXCEPTION_INTERMEDIATETYPE_IS_NULL_D0D9B957 = "intermediateType 不能为空";
  public static final String EXCEPTION_INPUTCHANNELS_IS_NULL_647DA393 = "inputChannels 不能为空";
  public static final String EXCEPTION_ARRAY_IS_NULL_BCF2EEB1 = "array 不能为空";
  public static final String EXCEPTION_USED_SLOTS_COUNT_IS_NEGATIVE_49F017C5 = "used slots count is negative";
  public static final String EXCEPTION_USED_SLOTS_COUNT_EXCEEDS_ARRAY_SIZE_6DCD8C7E = "used slots count exceeds array size";
  public static final String EXCEPTION_ARRAY_INDEX_OUT_OF_BOUNDS_35C8A83F = "array index out of bounds";
  public static final String EXCEPTION_EXCLUSIONS_IS_NULL_336ED5E7 = "exclusions 不能为空";
  public static final String EXCEPTION_INSTRUCTIONS_IS_NULL_A9CDA591 = "instructions 不能为空";
  public static final String EXCEPTION_LABELMAPPING_IS_NULL_535461C7 = "labelMapping 不能为空";
  public static final String EXCEPTION_INVALID_PATTERN_COLON_PERMUTATION_WITH_SINGLE_ELEMENT_DOT_RUN_IRROWPATTERNFLATTE_3FF56E02 = "无效的 pattern: permutation with single element. run IrRowPatternFlattener first";
  public static final String EXCEPTION_INVALID_MIN_VALUE_COLON_ARG_8D899A2E = "无效的 min value: %s";
  public static final String EXCEPTION_INVALID_RANGE_COLON_LEFT_PAREN_ARG_COMMA_ARG_RIGHT_PAREN_EF9544AA = "无效的 range: (%s, %s)";
  public static final String EXCEPTION_NEXTCOLUMN_QUOTE_S_TIME_SHOULD_BE_GREATER_THAN_CURRENT_TIME_334CB115 = "nextColumn's time should be greater than current time";
  public static final String EXCEPTION_HASHCHANNEL_IS_NULL_A5E70672 = "hashChannel 不能为空";
  public static final String EXCEPTION_MARKDISTINCTCHANNELS_IS_NULL_7130C56A = "markDistinctChannels 不能为空";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_SHOULD_BE_BINARYCOLUMN_24E654CD = "intermediate input 和 output of LAST should be BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_FIRST_BY_SHOULD_BE_3_BE623E5E = "Length of input Column[] for FIRST_BY should be 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_BY_SHOULD_BE_BINARYCOLUMN_F0D867AB = "intermediate input 和 output of FIRST_BY should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_COVARIANCE_SHOULD_BE_2_786B97E6 = "Input of Covariance should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_SHOULD_BE_BINARYCOLUMN_3B5148FA = "intermediate input 和 output should be BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LAST_BY_SHOULD_BE_3_D759D45F = "Length of input Column[] for LAST_BY should be 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_BY_SHOULD_BE_BINARYCOLUMN_87700792 = "intermediate input 和 output of LAST_BY should be BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_UDAF_SHOULD_BE_BINARYCOLUMN_6F28900F = "intermediate input 和 output of UDAF should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82 = "argument of AVG should be one column";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB = "intermediate input 和 output of AVG should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_CORRELATION_SHOULD_BE_2_64BB37B5 = "Input of Correlation should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_SHOULD_BE_BINARYCOLUMN_57CE212C = "intermediate input 和 output of FIRST should be BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_APPROX_COUNT_DISTINCT_SHOULD_BE_BINARYCOLUMN_F444D3BE = "intermediate input 和 output of APPROX_COUNT_DISTINCT should be BinaryColumn";
  public static final String EXCEPTION_SELECTEDPOSITIONS_IS_NULL_2300C002 = "selectedPositions 不能为空";
  public static final String EXCEPTION_RETAINEDPOSITIONS_IS_NULL_E62F9B0D = "retainedPositions 不能为空";
  public static final String EXCEPTION_POSITIONCOUNT_IS_NEGATIVE_2FACCDCA = "positionCount is negative";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_IS_NEGATIVE_A8A7FDA1 = "selectedPositionCount is negative";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_CANNOT_BE_GREATER_THAN_POSITIONCOUNT_871FF8DA = "selectedPositionCount 无法 be greater than positionCount";
  public static final String EXCEPTION_SELECTEDPOSITION_IS_SMALLER_THAN_SELECTEDPOSITIONCOUNT_465B9220 = "selectedPosition is smaller than selectedPositionCount";
  public static final String EXCEPTION_BLOCK_POSITION_COUNT_DOES_NOT_MATCH_CURRENT_POSITION_COUNT_FE4BBE3D = "Block position count does not match current position count";
  public static final String EXCEPTION_GETSELECTEDPOSITIONS_NOT_AVAILABLE_WHEN_IN_SELECTALL_MODE_CBE671D3 = "getSelectedPositions not available when in selectAll mode";
  public static final String EXCEPTION_ARGUMENT_OF_MAX_SHOULD_BE_ONE_COLUMN_FC251F55 = "argument of MAX should be one column";
  public static final String EXCEPTION_ARGUMENT_OF_SUM_SHOULD_BE_ONE_COLUMN_D6E636D1 = "argument of SUM should be one column";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_2BB3E0AF = "intermediate input 和 output of Mode should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_LEFT_PAREN_RIGHT_PAREN_SHOULD_BE_ONE_COLUMN_9158A9FE = "argument of COUNT(*) should be one column";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "Wrong inputDataTypes size.";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_SHOULD_BE_ONE_COLUMN_906D6B19 = "argument of COUNT should be one column";
  public static final String EXCEPTION_INTERMEDIATE_OUTPUT_OF_REGRESSION_SHOULD_BE_BINARYCOLUMN_E74D77ED = "intermediate output of Regression should be BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_REGRESSION_SHOULD_BE_2_3893EEE3 = "Input of Regression should be 2";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_MAX_BY_SLASH_MIN_BY_SHOULD_3767E905 = "Length of input Column[] for MAX_BY/MIN_BY should be 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MAX_BY_SLASH_MIN_BY_SHOULD_BE_BINARYCOLUMN_82B1BE6B = "intermediate input 和 output of MAX_BY/MIN_BY should be BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_MIN_SHOULD_BE_ONE_COLUMN_F8AA1E88 = "argument of MIN should be one column";
  public static final String EXCEPTION_STEP_IS_NULL_F83262DA = "step 不能为空";
  public static final String EXCEPTION_MASKCHANNEL_IS_NULL_571AD53D = "maskChannel 不能为空";
  public static final String EXCEPTION_EXPECTED_1_INPUT_CHANNEL_FOR_INTERMEDIATE_AGGREGATION_3190C507 = "期望 1 input channel for intermediate aggregation";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value 不能为空";
  public static final String EXCEPTION_VALUE_MUST_BE_POSITIVE_24FE7959 = "value 必须为 positive";
  public static final String EXCEPTION_NUMBEROFBUCKETS_MUST_BE_A_POWER_OF_2_COMMA_ACTUAL_COLON_ARG_476E74ED = "numberOfBuckets 必须为 a power of 2, actual: %s";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_0D03B323 = "intermediate input 和 output of MODE should be BinaryColumn";
  public static final String EXCEPTION_AGGREGATION_BUFFER_IS_FULL_233DAF3E = "Aggregation buffer is full";
  public static final String EXCEPTION_PAGE_IS_NULL_4AA19E1C = "page 不能为空";
  public static final String EXCEPTION_SPILL_IS_NOT_SUPPORTED_E6E35549 = "spill is not supported";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LASTBY_SLASH_FIRSTBY_SHOUL_9E836ABA = "Length of input Column[] for LastBy/FirstBy should be 3";
  public static final String EXCEPTION_THE_SIZE_OF_READYQUEUE_CANNOT_BE_NEGATIVE_DOT_01D8D0CB = "The size of readyQueue 无法 be negative.";
  public static final String EXCEPTION_THE_SIZE_BETWEEN_WHENTRANSFORMERS_AND_THENTRANSFORMERS_NEEDS_TO_BE_SAME_AC796883 = "the size between whenTransformers 和 thenTransformers needs 到 be same";
  public static final String EXCEPTION_EXCEED_MAX_CALL_TIMES_OF_GETCOLUMN_69C77C7E = "Exceed max call times of getColumn";
  public static final String EXCEPTION_FILTER_IS_NOT_SUPPORTED_IN_ARG_DOT_FILTER_IS_ARG_DOT_417C4F3C = "Filter is not supported in %s. Filter is %s.";
  public static final String EXCEPTION_NULL_9B41EF67 = "null";
  public static final String EXCEPTION_ARG_MUST_HAVE_JOIN_KEYS_DOT_C24DAB2D = "%s must have join keys.";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEOUTPUTSYMBOL_DOT_527996EC = "Source of SemiJoinNode doesn't contain sourceOutputSymbol.";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEJOINSYMBOL_DOT_32209273 = "Source of SemiJoinNode doesn't contain sourceJoinSymbol.";
  public static final String EXCEPTION_FILTERINGSOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_FILTERINGSOURCEJOINSYMBOL__1B75DDE2 = "FilteringSource of SemiJoinNode doesn't contain filteringSourceJoinSymbol.";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name 不能为空";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "function 不能为空";
  public static final String EXCEPTION_PREVIOUS_IS_NULL_056F5D52 = "previous 不能为空";
  public static final String EXCEPTION_PROPERTY_IS_NULL_1C6980FF = "property 不能为空";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "pattern 不能为空";
  public static final String EXCEPTION_CAPTURES_IS_NULL_75EACA5A = "captures 不能为空";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "predicate 不能为空";
  public static final String EXCEPTION_EXPECTEDCLASS_IS_NULL_B619CCC7 = "期望Class 不能为空";
  public static final String EXCEPTION_EXPECTEDVALUE_CAN_QUOTE_T_BE_NULL_DOT_USE_ISNULL_LEFT_PAREN_RIGHT_PAREN_PATTERN__FC25E374 = "期望Value can't be null. Use isNull() pattern instead.";
  public static final String EXCEPTION_CAPTURE_IS_NULL_C54AA710 = "capture 不能为空";
  public static final String EXCEPTION_PROPERTYPATTERN_IS_NULL_37185AE7 = "propertyPattern 不能为空";
  public static final String EXCEPTION_TARGET_NODE_MUST_EXIST_2C8D8F3A = "目标节点必须存在";
  public static final String EXCEPTION_HEAP_MUST_HAVE_AT_LEAST_ONE_NODE_BEFORE_STARTING_TRAVERSAL_A1E65C70 = "heap 在开始遍历前必须至少有一个节点";
  public static final String EXCEPTION_NEW_CHILD_SHOULDN_QUOTE_T_EXIST_YET_1EB3B129 = "新子节点不应已存在";
  public static final String EXCEPTION_ROWID_AND_UNIQUEVALUE_MASK_OVERLAPS_9A092E09 = "RowId 和 uniqueValue 掩码重叠";

}
