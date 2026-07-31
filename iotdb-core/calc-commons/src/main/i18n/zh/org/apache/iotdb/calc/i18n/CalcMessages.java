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
  public static final String EXCEPTION_WRONG_OBJECT_FILE_PATH_D317B1AB = "错误的对象文件路径：";
  public static final String EXCEPTION_OFFSET_ARG_GREATER_THAN_EQUAL_OBJECT_SIZE_ARG_FILE_PATH_31936A19 = "偏移量 %d 大于或等于对象大小 %d，文件路径为 %s";
  public static final String EXCEPTION_READ_OBJECT_SIZE_ARG_TOO_LARGE_SIZE_2G_FILE_PATH_7D4A4D10 = "读取的对象大小 %s 过大（大小 > 2G），文件路径为 %s";
  public static final String LOG_FAILED_CLOSE_FILE_METHOD_DEREGISTER_ARG_BECAUSE_ARG_1744AC60 = "在方法 deregister(%s) 中关闭文件失败，原因：%s";
  public static final String LOG_FAILED_CLEAN_DIR_METHOD_DEREGISTER_ARG_BECAUSE_ARG_F53193E5 = "在方法 deregister(%s) 中清理目录失败，原因：%s";
  public static final String EXCEPTION_TYPE_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVE_INPUT_905AFAA1 = "该类型的累加器不支持移除输入！";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_VARIANCE_ARG_17A45959 = "variance 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_0A19C2A8 = "CentralMoment 聚合中不支持的数据类型：";
  public static final String EXCEPTION_UNKNOWN_FUNCTION_ARG_NODE_ARG_927DA6A7 = "未知函数 %s，所在节点：%d。";
  public static final String EXCEPTION_INLIST_LITERAL_TIMESTAMP_CAN_ONLY_LONGLITERAL_DOUBLELITERAL_GENERICLITERAL_CURRENT_A3105E67 = "TIMESTAMP 的 InList Literal 只能是 LongLiteral、DoubleLiteral 或 GenericLiteral，当前为";
  public static final String EXCEPTION_PARTITION_HANDLER_FINISHED_CANNOT_ADD_MORE_DATA_D8E31A3E = "分区处理器已结束，不能再添加数据。";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_PATTERN_VARIABLE_NOT_PRESENT_MATCH_2891B276 = "匹配后跳转失败：匹配中不存在模式变量";
  public static final String EXCEPTION_AFTER_MATCH_SKIP_FAILED_CANNOT_SKIP_FIRST_ROW_MATCH_71D4093B = "匹配后跳转失败：不能跳到匹配的第一行";
  public static final String EXCEPTION_AGGREGATE_FUNCTION_ARG_DOES_NOT_SUPPORT_COPYING_3E03976C = "聚合函数 %s 不支持复制";
  public static final String EXCEPTION_UNSUPPORTED_BUILT_WINDOW_FUNCTION_NAME_F994ED27 = "不支持的内置窗口函数名称：";
  public static final String EXCEPTION_UNSUPPORTED_DEFAULT_VALUE_S_DATA_TYPE_LAG_BEB00511 = "Lag 中不支持的默认值数据类型：";
  public static final String EXCEPTION_CANNOT_COMPARE_VALUES_DIFFERENT_TYPES_A95EDA7F = "无法比较不同类型的值：";
  public static final String EXCEPTION_VS_AEDAB253 = " 与 ";
  public static final String EXCEPTION_TARGET_TYPE_NOT_GENERICDATATYPE_CB87EF37 = "目标类型不是 GenericDataType：";
  public static final String EXCEPTION_UNSUPPORTED_EXPRESSION_TYPE_5C50C92E = "不支持的表达式类型：";
  public static final String EXCEPTION_IDENTITYLINEARFILL_S_NEEDPREPAREFORNEXT_METHOD_SHOULD_ALWAYS_RETURN_FALSE_FF7AD73B = "IdentityLinearFill 的 needPrepareForNext() 方法应始终返回 false。";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_ARG_37F52124 = "LAST 中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_2FCF5ADA = "LAST 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_B3F2EE42 = "LAST 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_SIZE_CHILDRENCOLUMNS_DATATYPES_SHOULD_SAME_6EA43EA8 = "childrenColumns 和 dataTypes 的大小应相同。";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_8D31C6CA = "FIRST_BY 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_APPROX_PERCENTILE_REQUIRES_2_3_ARGUMENTS_BUT_GOT_ARG_D78590AA = "APPROX_PERCENTILE 需要 2 个或 3 个参数，但实际为 %d";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_PERCENTILE_AGGREGATION_ARG_CFEC0431 = "APPROX_PERCENTILE 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_APPROXPERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_2BB01365 = "ApproxPercentileAccumulator 不支持统计信息";
  public static final String EXCEPTION_NO_EXACT_DOUBLE_REPRESENTATION_LONG_ARG_3E5B7550 = "long 没有精确的 double 表示：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_COVARIANCE_AGGREGATION_ARG_643BEDD0 = "Covariance 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_LAST_AGGREGATION_ARG_8C41FBCC = "LAST_BY 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CENTRALMOMENT_AGGREGATION_ARG_74B9A3A8 = "CentralMoment 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_SECOND_THIRD_ARGUMENT_MUST_GREATER_THAN_0_BUT_GOT_K_0AFFFA6A = "第二个和第三个参数必须大于 0，实际 k=";
  public static final String EXCEPTION_CAPACITY_E92593AD = "，容量=";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AGGREGATION_AVG_ARG_4E63A3C3 = "AVG 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_CORRELATION_AGGREGATION_ARG_2FF29597 = "Correlation 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_ARG_D5B6C6F9 = "FIRST 中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_B2D27BB9 = "FIRST 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_FIRST_AGGREGATION_ARG_2652C363 = "FIRST 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_VARIANCE_AGGREGATION_ARG_C641D425 = "VARIANCE 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_7D59F7B2 = "不支持的数据类型 : %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_APPROX_COUNT_DISTINCT_AGGREGATION_ARG_58F0391E = "APPROX_COUNT_DISTINCT 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_APPROXCOUNTDISTINCTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_81005A79 = "ApproxCountDistinctAccumulator 不支持统计信息";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_AGGREGATION_ARG_CCB09C60 = "MAX 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_SUM_AGGREGATION_ARG_92F5A18D = "SUM 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_8CA29DC2 = "计算 MODE 时不同值数量已超过阈值 %s";
  public static final String EXCEPTION_APPROXMOSTFREQUENTACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_DB1E218B = "ApproxMostFrequentAccumulator 不支持统计信息";
  public static final String EXCEPTION_DISTINCT_AGGREGATION_FUNCTION_STATE_CAN_NOT_COPIED_34D0A276 = "DISTINCT 聚合函数状态不能复制";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_REGRESSION_AGGREGATION_ARG_7BB08DA2 = "Regression 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MIN_AGGREGATION_ARG_CB73B158 = "MIN 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_INVALID_FORMAT_SERIALIZED_HISTOGRAM_GOT_ENCODING_A8E50EA5 = "序列化直方图格式无效，编码为：";
  public static final String EXCEPTION_MAX_STANDARD_ERROR_MUST_ARG_ARG_ARG_EB8443D6 = "最大标准误差必须在 [%s, %s] 范围内：%s";
  public static final String EXCEPTION_CANNOT_MERGE_HYPERLOGLOG_INSTANCES_DIFFERENT_PRECISION_9DCF13BC = "无法合并精度不同的 HyperLogLog 实例";
  public static final String EXCEPTION_DISTINCT_VALUES_HAS_EXCEEDED_THRESHOLD_ARG_CALCULATE_MODE_ONE_GROUP_A3F5A1D3 = "在单个分组中计算 MODE 时不同值数量已超过阈值 %s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_MAX_MIN_AGGREGATION_ARG_982BEBD5 = "MAX_BY/MIN_BY 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_AF4DA98F = "EXTREME 聚合不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_EXTREME_AGGREGATION_ARG_276BE220 = "EXTREME 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_AVG_AGGREGATION_ARG_66BF3128 = "AVG 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_CAN_T_READ_NEW_TSBLOCK_FILESPILLERREADER_072AF71D = "无法在 FileSpillerReader 中读取新的 tsBlock：";
  public static final String EXCEPTION_CAN_T_CLOSE_FILECHANNEL_FILESPILLERREADER_E46979F0 = "无法关闭 FileSpillerReader 中的 FileChannel：";
  public static final String EXCEPTION_CREATE_FILE_ERROR_B8B379CF = "创建文件错误：";
  public static final String EXCEPTION_CAN_T_WRITE_INTERMEDIATE_SORTED_DATA_FILE_0027961E = "无法将中间排序数据写入文件：";
  public static final String EXCEPTION_CAN_T_GET_FILE_FILESPILLERREADER_CHECK_IF_FILE_EXISTS_DEED83D9 = "无法获取 FileSpillerReader 的文件，请检查文件是否存在：";
  public static final String EXCEPTION_LONG_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B3F9016B = "long 值 %d 超出 int 值范围。";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_B0E6DDED = "float 值 %f 超出 int 值范围。";
  public static final String EXCEPTION_FLOAT_VALUE_ARG_OUT_RANGE_LONG_VALUE_62F8153E = "float 值 %f 超出 long 值范围。";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_INTEGER_VALUE_BAB52E11 = "double 值 %f 超出 int 值范围。";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_LONG_VALUE_5793A91E = "double 值 %f 超出 long 值范围。";
  public static final String EXCEPTION_DOUBLE_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_DB914FA0 = "double 值 %f 超出 float 值范围。";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_FLOAT_VALUE_D171B313 = "Text 值 %s 超出 float 值范围。";
  public static final String EXCEPTION_TEXT_VALUE_ARG_OUT_RANGE_DOUBLE_VALUE_C0589D83 = "Text 值 %s 超出 double 值范围。";
  public static final String EXCEPTION_UNSUPPORTED_SOURCE_DATATYPE_ARG_678B759C = "不支持的源 dataType：%s";
  public static final String EXCEPTION_CANNOT_CAST_ARG_ARG_TYPE_8266A2C6 = "无法将 %s 转换为 %s 类型";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_NUM_MUST_REPRESENTABLE_BITS_SPECIFIED_ARG_241AD2FE =
      "参数异常，标量函数 num 必须能用指定的位数表示。%s 不能用 %s 位表示。";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_BIT_COUNT_BITS_MUST_BETWEEN_2_6A365911 = "参数异常，标量函数 bit_count 的位数必须在 2 到 64 之间。";
  public static final String LOG_ERROR_OCCURRED_DURING_EXECUTING_UDTF_PLEASE_CHECK_WHETHER_IMPLEMENTATION_UDF_7A719D38 =
      "执行 UDTF 时发生错误，请根据 UDF API 描述检查 UDF 实现是否正确。";
  public static final String EXCEPTION_ERROR_OCCURRED_DURING_EXECUTING_UDTF_ARG_ARG_PLEASE_CHECK_WHETHER_4E706370 =
      "执行 UDTF#%s 时发生错误：%s，请根据 UDF API 描述检查 UDF 实现是否正确。";
  public static final String EXCEPTION_UNSUPPORTED_TYPE_FF7F518D = "不支持的类型: ";
  public static final String EXCEPTION_LOGICALORMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_E3A2B2FA = "LogicalOrMultiColumnTransformer 不支持带 selection 的 doTransform";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_B411C29E = "不支持的数据类型: %s";
  public static final String EXCEPTION_LOGICALANDMULTICOLUMNTRANSFORMER_DO_NOT_SUPPORT_DOTRANSFORM_SELECTION_2507527D = "LogicalAndMultiColumnTransformer 不支持带 selection 的 doTransform";
  public static final String EXCEPTION_ERROR_OCCURS_EVALUATING_USER_DEFINED_SCALAR_FUNCTION_05903C18 = "计算用户自定义标量函数时发生错误：";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_MD5_INVALID_INPUT_FORMAT_EMPTY_KEY_DED1C525 = "无法执行函数 hmac_md5，原因：输入格式无效，HMAC 操作不允许空密钥。";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA1_INVALID_INPUT_FORMAT_EMPTY_KEY_518063E5 = "无法执行函数 hmac_sha1，原因：输入格式无效，HMAC 操作不允许空密钥。";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA256_INVALID_INPUT_FORMAT_EMPTY_KEY_2F6AD64E = "无法执行函数 hmac_sha256，原因：输入格式无效，HMAC 操作不允许空密钥。";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_HMAC_SHA512_INVALID_INPUT_FORMAT_EMPTY_KEY_3671AF09 = "无法执行函数 hmac_sha512，原因：输入格式无效，HMAC 操作不允许空密钥。";
  public static final String EXCEPTION_INVALID_FORMAT_STRING_ARG_ARG_05853138 = "无效的格式字符串：%s (%s)";
  public static final String EXCEPTION_YEAR_MUST_BETWEEN_1000_9999_8FBB94AA = "年份必须在 1000 到 9999 之间。";
  public static final String EXCEPTION_UNSUPPORTED_PRECISION_CDB58979 = "不支持的精度：";
  public static final String EXCEPTION_UNKNOWN_PRECISION_0119BEB0 = "未知精度：";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_LENGTH_MUST_NOT_LESS_THAN_072B7355 = "参数异常，标量函数 substring 的长度参数不能小于 0";
  public static final String EXCEPTION_ARGUMENT_EXCEPTION_SCALAR_FUNCTION_SUBSTRING_BEGINPOSITION_MUST_NOT_GREATER_THAN_F0BA2A56 =
      "参数异常，标量函数 substring 的 beginPosition 不能大于字符串长度";
  public static final String EXCEPTION_TYPE_DB38663D = "类型：";
  public static final String EXCEPTION_NOT_SUPPORTED_GENERATEPROBLEMATICVALUESTRING_624BB179 = "在 generateProblematicValueString() 中不受支持";
  public static final String EXCEPTION_BASE64_LENGTH_MUST_MULTIPLE_4_INCLUDING_PADDING_8717829D = "Base64 长度必须是 4 的倍数（包括填充字符 '='）";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_32_MUST_4_C1214BB5 = "函数 from_big_endian_32 的输入 BLOB 长度必须为 4。";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_BIG_ENDIAN_64_MUST_8_797F1A26 = "函数 from_big_endian_64 的输入 BLOB 长度必须为 8。";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_32_MUST_4_0FBBD083 = "函数 from_little_endian_32 的输入 BLOB 长度必须为 4。";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_LITTLE_ENDIAN_64_MUST_8_771F7A96 = "函数 from_little_endian_64 的输入 BLOB 长度必须为 8。";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_32_BIG_ENDIAN_MUST_4_51AF4EF0 = "函数 from_ieee754_32_big_endian 的输入 BLOB 长度必须为 4。";
  public static final String EXCEPTION_LENGTH_INPUT_BLOB_FUNCTION_IEEE754_64_BIG_ENDIAN_MUST_8_56F33455 = "函数 from_ieee754_64_big_endian 的输入 BLOB 长度必须为 8。";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_INVALID_TARGET_SIZE_5054AE0B =
      "无法执行函数 '%s'，原因：值 %s 对应无效的目标大小，允许范围为 [0, %d]。";
  public static final String EXCEPTION_FAILED_EXECUTE_FUNCTION_ARG_VALUE_ARG_CORRESPONDING_EMPTY_PADDING_STRING_F1CE573C = "无法执行函数 '%s'，原因：值 %s 对应空的填充字符串。";
  public static final String EXCEPTION_FOUND_NO_COLUMN_ARG_ARG_8CF632F7 = "未找到列 %s，所在位置：%s";
  public static final String EXCEPTION_SORT_ITEM_ARG_NOT_INCLUDED_CHILDREN_S_OUTPUT_COLUMNS_CC911999 = "排序项 %s 未包含在子节点输出列中";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFTOUTPUTSYMBOL_0C8AA216 = "JoinNode 的左子节点不包含 LeftOutputSymbol";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHTOUTPUTSYMBOL_10A86F63 = "JoinNode 的右子节点不包含 RightOutputSymbol";
  public static final String EXCEPTION_LEFT_CHILD_JOINNODE_DOESN_T_CONTAIN_LEFT_ASOF_MAIN_JOIN_2850234A = "JoinNode 的左子节点不包含左侧 ASOF 主连接键。";
  public static final String EXCEPTION_RIGHT_CHILD_JOINNODE_DOESN_T_CONTAIN_RIGHT_ASOF_MAIN_JOIN_1A9631C9 = "JoinNode 的右子节点不包含右侧 ASOF 主连接键。";
  public static final String EXCEPTION_JOIN_KEY_TYPE_MISMATCH_LEFT_JOIN_KEY_TYPE_072E692E = "连接键类型不匹配。左连接键类型：";
  public static final String EXCEPTION_RIGHT_JOIN_KEY_TYPE_56895767 = "，右连接键类型：";
  public static final String EXCEPTION_ROW_COUNT_MUST_0_1_8D44189F = "行数必须为 0 或 1";
  public static final String EXCEPTION_COLUMN_ARG_CANNOT_RESOLVED_508579EC = "无法解析列 '%s'。";
  public static final String EXCEPTION_REQUESTED_VALUE_UNKNOWN_CAPTURE_WAS_IT_REGISTERED_PATTERN_C77DF7E9 = "请求了未知 Capture 的值。是否已在 Pattern 中注册？";
  public static final String EXCEPTION_TARGET_NODE_INDEX_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_75BFB7C3 = "目标节点索引必须大于或等于 1";
  public static final String EXCEPTION_ALREADY_AT_TARGET_32DFD57F = "已到达目标";
  public static final String EXCEPTION_PARTIALRESULT_OF_COVARIANCE_SHOULD_BE_1_E6A950B1 = "Covariance 的 partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_COVARIANCE_SHOULD_BE_1_75D3F4FD = "Covariance 的输入数量应为 1";
  public static final String EXCEPTION_COVARIANCE_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_8C088DC6 = "Covariance 状态计数小于已移除状态计数";
  public static final String EXCEPTION_PARTIALRESULT_OF_VARIANCE_SHOULD_BE_1_9C281868 = "variance 的 partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_VARIANCE_SHOULD_BE_1_1BC7A702 = "variance 的输入数量应为 1";
  public static final String EXCEPTION_PARTIALRESULT_OF_CENTRALMOMENT_SHOULD_BE_1_B7C0B4B9 = "CentralMoment 的 partialResult 应为 1";
  public static final String EXCEPTION_PARTIALRESULT_SHOULD_BE_1_399D5DC3 = "partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_CENTRALMOMENT_SHOULD_BE_1_FD23B170 = "CentralMoment 的输入数量应为 1";
  public static final String EXCEPTION_CENTRALMOMENT_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_B87864BA = "CentralMoment 状态计数小于已移除状态计数";
  public static final String EXCEPTION_PARTIALRESULT_OF_CORRELATION_SHOULD_BE_1_D5A8CED6 = "Correlation 的 partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_CORRELATION_SHOULD_BE_1_5D666DB5 = "Correlation 的输入数量应为 1";
  public static final String EXCEPTION_CORRELATION_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_66C90757 = "Correlation 状态计数小于已移除状态计数";
  public static final String EXCEPTION_PARTIALRESULT_OF_REGRESSION_SHOULD_BE_1_B7ED33FA = "Regression 的 partialResult 应为 1";
  public static final String EXCEPTION_INPUT_OF_REGRESSION_SHOULD_BE_1_9AF4E8EC = "Regression 的输入数量应为 1";
  public static final String EXCEPTION_REGRESSION_STATE_COUNT_IS_SMALLER_THAN_REMOVED_STATE_COUNT_4415A3AE = "Regression 状态计数小于已移除状态计数";
  public static final String EXCEPTION_INVALID_STARTINGPOSITION_COLON_ARG_7B4D49C0 = "无效的 startingPosition: %s";
  public static final String EXCEPTION_ROWID_DOES_NOT_MATCH_THIS_TSBLOCK_18F9EB4D = "rowId 与当前 TsBlock 不匹配";
  public static final String EXCEPTION_CURSOR_STILL_ACTIVE_E9462D3B = "游标仍处于活动状态";
  public static final String EXCEPTION_NO_ACTIVE_CURSOR_C89C9944 = "没有活动游标";
  public static final String EXCEPTION_NOT_YET_ADVANCED_1600C5A7 = "尚未前进";
  public static final String EXCEPTION_SHOULD_NOT_ATTEMPT_COMPACTION_WHEN_TSBLOCK_IS_LOCKED_DF775E6B = "TsBlock 锁定时不应尝试压缩";
  public static final String EXCEPTION_STRATEGY_IS_NULL_DDA2C67F = "策略不能为空";
  public static final String EXCEPTION_ROWIDEVICTIONLISTENER_IS_NULL_457EFBEE = "rowIdEvictionListener 不能为空";
  public static final String EXCEPTION_TOPN_MUST_BE_GREATER_THAN_ZERO_43866D3E = "topN 必须大于 0";
  public static final String EXCEPTION_NO_ROOT_TO_PEEK_43C3D8D5 = "没有可查看的根节点";
  public static final String EXCEPTION_GROUP_ID_HAS_AN_EMPTY_HEAP_C9EDB841 = "分组 ID 的堆为空";
  public static final String EXCEPTION_POPANDINSERT_LEFT_PAREN_RIGHT_PAREN_REQUIRES_AT_LEAST_A_ROOT_NODE_0CA0DD58 = "popAndInsert() 至少需要一个根节点";
  public static final String EXCEPTION_OPERATORCONTEXT_IS_NULL_D15B1EDB = "operatorContext 不能为空";
  public static final String EXCEPTION_CHILD_OPERATOR_IS_NULL_8860113C = "子算子不能为空";
  public static final String EXCEPTION_LIMIT_MUST_BE_AT_LEAST_ZERO_E960530F = "LIMIT 必须至少为 0";
  public static final String EXCEPTION_OFFSET_MUST_BE_AT_LEAST_ZERO_21BB6BB6 = "偏移量必须至少为 0";
  public static final String EXCEPTION_TSBLOCKS_IS_NULL_02287FD8 = "tsBlocks 不能为空";
  public static final String EXCEPTION_FILLARRAY_SHOULD_NOT_BE_NULL_OR_EMPTY_118FB134 = "fillArray 不能为 null 或空";
  public static final String EXCEPTION_UNIQUE_ROW_ID_EXCEEDS_A_LIMIT_COLON_ARG_71EE4B7D = "唯一行 ID 超出限制：%s";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_FOR_PREVIOUSFILLWITHGROUPOPERATOR_SHOULD_NEVER_BE_NEGATIVE_ABA9C2B1 = "PreviousFillWithGroupOperator 的 helperColumnIndex 不应为负数";
  public static final String EXCEPTION_OUTPUTCOLUMNCOUNT_IS_NOT_EQUAL_TO_VALUE_COLUMN_COUNT_OF_CHILD_OPERATOR_QUOTE_S_T_8E30BAD8 = "outputColumnCount 不等于子算子 TsBlock 的值列数量";
  public static final String EXCEPTION_HELPERCOLUMNINDEX_SHOULD_BE_RESOLVED_WHEN_TIMEBOUND_EXISTS_01BE233B = "存在 timeBound 时应解析 helperColumnIndex";
  public static final String EXCEPTION_TYPE_IS_NULL_16A3D3EB = "类型不能为空";
  public static final String EXCEPTION_LOGICALINDEXNAVIGATION_IS_NULL_3B31CD23 = "logicalIndexNavigation 不能为空";
  public static final String EXCEPTION_OVERRIDING_AGGREGATIONS_FOR_CHILD_THREAD_9C84E47D = "正在覆盖子线程的聚合";
  public static final String EXCEPTION_SKIP_TO_NAVIGATION_IS_MISSING_FOR_SKIP_TO_ARG_8A303D73 = "跳转到 %s 缺少跳转导航";
  public static final String EXCEPTION_LABELS_IS_NULL_F4FBBECE = "标签不能为空";
  public static final String EXCEPTION_LOGICAL_OFFSET_MUST_BE_GREATER_THAN_EQUALS_0_COMMA_ACTUAL_COLON_ARG_539807BF = "逻辑偏移量必须 >= 0，实际为：%s";
  public static final String EXCEPTION_CURRENT_ROW_IS_OUT_OF_BOUNDS_OF_THE_MATCH_D8D8B611 = "当前行超出匹配范围";
  public static final String EXCEPTION_EVALUATIONS_IS_NULL_F3F72380 = "求值列表不能为空";
  public static final String EXCEPTION_PARTITION_IS_NULL_27FD9756 = "分区不能为空";
  public static final String EXCEPTION_LABELNAMES_IS_NULL_EEFDB807 = "labelNames 不能为空";
  public static final String EXCEPTION_BOUNDSIGNATURE_IS_NULL_D1077180 = "boundSignature 不能为空";
  public static final String EXCEPTION_ACCUMULATO_IS_NULL_D51F532F = "累加器不能为空";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IS_NULL_82EC5B1C = "patternAggregationTracker 不能为空";
  public static final String EXCEPTION_PATTERNAGGREGATIONTRACKER_IN_INCONSISTENT_STATE_C6FF5294 = "PatternAggregationTracker 状态不一致";
  public static final String EXCEPTION_SETEVALUATOR_IN_INCONSISTENT_STATE_D5FAA900 = "SetEvaluator 状态不一致";
  public static final String EXCEPTION_ACCUMULATOR_IS_NULL_EF0C1DFF = "累加器不能为空";
  public static final String EXCEPTION_INTERMEDIATETYPE_IS_NULL_D0D9B957 = "intermediateType 不能为空";
  public static final String EXCEPTION_INPUTCHANNELS_IS_NULL_647DA393 = "inputChannels 不能为空";
  public static final String EXCEPTION_ARRAY_IS_NULL_BCF2EEB1 = "数组不能为空";
  public static final String EXCEPTION_USED_SLOTS_COUNT_IS_NEGATIVE_49F017C5 = "已用槽位数量为负数";
  public static final String EXCEPTION_USED_SLOTS_COUNT_EXCEEDS_ARRAY_SIZE_6DCD8C7E = "已用槽位数量超过数组大小";
  public static final String EXCEPTION_ARRAY_INDEX_OUT_OF_BOUNDS_35C8A83F = "数组索引越界";
  public static final String EXCEPTION_EXCLUSIONS_IS_NULL_336ED5E7 = "排除项不能为空";
  public static final String EXCEPTION_INSTRUCTIONS_IS_NULL_A9CDA591 = "指令不能为空";
  public static final String EXCEPTION_LABELMAPPING_IS_NULL_535461C7 = "labelMapping 不能为空";
  public static final String EXCEPTION_INVALID_PATTERN_COLON_PERMUTATION_WITH_SINGLE_ELEMENT_DOT_RUN_IRROWPATTERNFLATTE_3FF56E02 = "无效的模式：单元素排列。请先运行 IrRowPatternFlattener";
  public static final String EXCEPTION_INVALID_MIN_VALUE_COLON_ARG_8D899A2E = "无效的最小值：%s";
  public static final String EXCEPTION_INVALID_RANGE_COLON_LEFT_PAREN_ARG_COMMA_ARG_RIGHT_PAREN_EF9544AA = "无效范围：(%s, %s)";
  public static final String EXCEPTION_NEXTCOLUMN_QUOTE_S_TIME_SHOULD_BE_GREATER_THAN_CURRENT_TIME_334CB115 = "nextColumn 的时间应大于当前时间";
  public static final String EXCEPTION_HASHCHANNEL_IS_NULL_A5E70672 = "hashChannel 不能为空";
  public static final String EXCEPTION_MARKDISTINCTCHANNELS_IS_NULL_7130C56A = "markDistinctChannels 不能为空";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_SHOULD_BE_BINARYCOLUMN_24E654CD = "LAST 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_FIRST_BY_SHOULD_BE_3_BE623E5E = "FIRST_BY 的输入 Column[] 长度应为 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_BY_SHOULD_BE_BINARYCOLUMN_F0D867AB = "FIRST_BY 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_COVARIANCE_SHOULD_BE_2_786B97E6 = "Covariance 的输入数量应为 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_SHOULD_BE_BINARYCOLUMN_3B5148FA = "中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LAST_BY_SHOULD_BE_3_D759D45F = "LAST_BY 的输入 Column[] 长度应为 3";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_LAST_BY_SHOULD_BE_BINARYCOLUMN_87700792 = "LAST_BY 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_UDAF_SHOULD_BE_BINARYCOLUMN_6F28900F = "UDAF 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82 = "AVG 的参数应为一列";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB = "AVG 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_CORRELATION_SHOULD_BE_2_64BB37B5 = "Correlation 的输入数量应为 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_FIRST_SHOULD_BE_BINARYCOLUMN_57CE212C = "FIRST 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_APPROX_COUNT_DISTINCT_SHOULD_BE_BINARYCOLUMN_F444D3BE = "APPROX_COUNT_DISTINCT 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_SELECTEDPOSITIONS_IS_NULL_2300C002 = "selectedPositions 不能为空";
  public static final String EXCEPTION_RETAINEDPOSITIONS_IS_NULL_E62F9B0D = "retainedPositions 不能为空";
  public static final String EXCEPTION_POSITIONCOUNT_IS_NEGATIVE_2FACCDCA = "positionCount 为负数";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_IS_NEGATIVE_A8A7FDA1 = "selectedPositionCount 为负数";
  public static final String EXCEPTION_SELECTEDPOSITIONCOUNT_CANNOT_BE_GREATER_THAN_POSITIONCOUNT_871FF8DA = "selectedPositionCount 不能大于 positionCount";
  public static final String EXCEPTION_SELECTEDPOSITION_IS_SMALLER_THAN_SELECTEDPOSITIONCOUNT_465B9220 = "selectedPosition 小于 selectedPositionCount";
  public static final String EXCEPTION_BLOCK_POSITION_COUNT_DOES_NOT_MATCH_CURRENT_POSITION_COUNT_FE4BBE3D = "Block 位置数与当前位置数不匹配";
  public static final String EXCEPTION_GETSELECTEDPOSITIONS_NOT_AVAILABLE_WHEN_IN_SELECTALL_MODE_CBE671D3 = "selectAll 模式下 getSelectedPositions 不可用";
  public static final String EXCEPTION_ARGUMENT_OF_MAX_SHOULD_BE_ONE_COLUMN_FC251F55 = "MAX 的参数应为一列";
  public static final String EXCEPTION_ARGUMENT_OF_SUM_SHOULD_BE_ONE_COLUMN_D6E636D1 = "SUM 的参数应为一列";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_2BB3E0AF = "Mode 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_LEFT_PAREN_RIGHT_PAREN_SHOULD_BE_ONE_COLUMN_9158A9FE = "COUNT(*) 的参数应为一列";
  public static final String EXCEPTION_WRONG_INPUTDATATYPES_SIZE_DOT_675FF289 = "inputDataTypes 大小错误。";
  public static final String EXCEPTION_ARGUMENT_OF_COUNT_SHOULD_BE_ONE_COLUMN_906D6B19 = "COUNT 的参数应为一列";
  public static final String EXCEPTION_INTERMEDIATE_OUTPUT_OF_REGRESSION_SHOULD_BE_BINARYCOLUMN_E74D77ED = "Regression 的中间输出应为 BinaryColumn";
  public static final String EXCEPTION_INPUT_OF_REGRESSION_SHOULD_BE_2_3893EEE3 = "Regression 的输入数量应为 2";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_MAX_BY_SLASH_MIN_BY_SHOULD_3767E905 = "MAX_BY/MIN_BY 的输入 Column[] 长度应为 2";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MAX_BY_SLASH_MIN_BY_SHOULD_BE_BINARYCOLUMN_82B1BE6B = "MAX_BY/MIN_BY 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_ARGUMENT_OF_MIN_SHOULD_BE_ONE_COLUMN_F8AA1E88 = "MIN 的参数应为一列";
  public static final String EXCEPTION_STEP_IS_NULL_F83262DA = "步骤不能为空";
  public static final String EXCEPTION_MASKCHANNEL_IS_NULL_571AD53D = "maskChannel 不能为空";
  public static final String EXCEPTION_EXPECTED_1_INPUT_CHANNEL_FOR_INTERMEDIATE_AGGREGATION_3190C507 = "中间聚合期望 1 个输入通道";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "值不能为空";
  public static final String EXCEPTION_VALUE_MUST_BE_POSITIVE_24FE7959 = "值必须为正数";
  public static final String EXCEPTION_NUMBEROFBUCKETS_MUST_BE_A_POWER_OF_2_COMMA_ACTUAL_COLON_ARG_476E74ED = "numberOfBuckets 必须是 2 的幂，实际为：%s";
  public static final String EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_MODE_SHOULD_BE_BINARYCOLUMN_0D03B323 = "MODE 的中间输入和输出应为 BinaryColumn";
  public static final String EXCEPTION_AGGREGATION_BUFFER_IS_FULL_233DAF3E = "聚合缓冲区已满";
  public static final String EXCEPTION_PAGE_IS_NULL_4AA19E1C = "页面不能为空";
  public static final String EXCEPTION_SPILL_IS_NOT_SUPPORTED_E6E35549 = "不支持 spill";
  public static final String EXCEPTION_LENGTH_OF_INPUT_COLUMN_LEFT_BRACKET_RIGHT_BRACKET_FOR_LASTBY_SLASH_FIRSTBY_SHOUL_9E836ABA = "LAST_BY/FIRST_BY 的输入 Column[] 长度应为 3";
  public static final String EXCEPTION_THE_SIZE_OF_READYQUEUE_CANNOT_BE_NEGATIVE_DOT_01D8D0CB = "readyQueue 的大小不能为负数。";
  public static final String EXCEPTION_THE_SIZE_BETWEEN_WHENTRANSFORMERS_AND_THENTRANSFORMERS_NEEDS_TO_BE_SAME_AC796883 = "whenTransformers 和 thenTransformers 的大小应相同";
  public static final String EXCEPTION_EXCEED_MAX_CALL_TIMES_OF_GETCOLUMN_69C77C7E = "超过 getColumn 的最大调用次数";
  public static final String EXCEPTION_TOPK_RUNTIME_FILTER_REQUIRES_SINGLE_TIME_ORDER_BY_7EED6208 =
      "TopK Runtime Filter 要求 ORDER BY 单个 INT64 或 TIMESTAMP 类型的 time 列";
  public static final String EXCEPTION_FILTER_IS_NOT_SUPPORTED_IN_ARG_DOT_FILTER_IS_ARG_DOT_417C4F3C = "%s 不支持过滤条件。过滤条件为 %s。";
  public static final String EXCEPTION_NULL_9B41EF67 = "null";
  public static final String EXCEPTION_ARG_MUST_HAVE_JOIN_KEYS_DOT_C24DAB2D = "%s 必须包含连接键。";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEOUTPUTSYMBOL_DOT_527996EC = "SemiJoinNode 的 Source 不包含 sourceOutputSymbol。";
  public static final String EXCEPTION_SOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_SOURCEJOINSYMBOL_DOT_32209273 = "SemiJoinNode 的 Source 不包含 sourceJoinSymbol。";
  public static final String EXCEPTION_FILTERINGSOURCE_OF_SEMIJOINNODE_DOESN_QUOTE_T_CONTAIN_FILTERINGSOURCEJOINSYMBOL__1B75DDE2 = "SemiJoinNode 的 FilteringSource 不包含 filteringSourceJoinSymbol。";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "名称不能为空";
  public static final String EXCEPTION_FUNCTION_IS_NULL_E0FA4B62 = "函数不能为空";
  public static final String EXCEPTION_PREVIOUS_IS_NULL_056F5D52 = "前一个值不能为空";
  public static final String EXCEPTION_PROPERTY_IS_NULL_1C6980FF = "属性不能为空";
  public static final String EXCEPTION_PATTERN_IS_NULL_AC4E239A = "模式不能为空";
  public static final String EXCEPTION_CAPTURES_IS_NULL_75EACA5A = "捕获列表不能为空";
  public static final String EXCEPTION_PREDICATE_IS_NULL_22E687A9 = "谓词不能为空";
  public static final String EXCEPTION_EXPECTEDCLASS_IS_NULL_B619CCC7 = "期望类不能为空";
  public static final String EXCEPTION_EXPECTEDVALUE_CAN_QUOTE_T_BE_NULL_DOT_USE_ISNULL_LEFT_PAREN_RIGHT_PAREN_PATTERN__FC25E374 = "expectedValue 不能为 null，请改用 isNull() 模式。";
  public static final String EXCEPTION_CAPTURE_IS_NULL_C54AA710 = "捕获不能为空";
  public static final String EXCEPTION_PROPERTYPATTERN_IS_NULL_37185AE7 = "propertyPattern 不能为空";
  public static final String EXCEPTION_TARGET_NODE_MUST_EXIST_2C8D8F3A = "目标节点必须存在";
  public static final String EXCEPTION_HEAP_MUST_HAVE_AT_LEAST_ONE_NODE_BEFORE_STARTING_TRAVERSAL_A1E65C70 = "堆在开始遍历前必须至少有一个节点";
  public static final String EXCEPTION_NEW_CHILD_SHOULDN_QUOTE_T_EXIST_YET_1EB3B129 = "新子节点不应已存在";
  public static final String EXCEPTION_ROWID_AND_UNIQUEVALUE_MASK_OVERLAPS_9A092E09 = "RowId 和 uniqueValue 掩码重叠";

  // --- Execution ---

  public static final String ERROR_SETTING_FUTURE_STATE_FOR =
      "为 {} 设置 future 状态时出错";
  public static final String ERROR_NOTIFYING_STATE_CHANGE_LISTENER_FOR =
      "通知 {} 的状态变更监听器时出错";
  public static final String SERVER_IS_SHUTTING_DOWN =
      "服务器正在关闭";
  public static final String EXCEPTION_EXECUTOR_IS_NULL_7FBE03A4 = "executor 不能为空";
  public static final String EXCEPTION_INITIALSTATE_IS_NULL_8992A39F = "initialState 不能为空";
  public static final String EXCEPTION_TERMINALSTATES_IS_NULL_E0FC2A93 = "terminalStates 不能为空";
  public static final String EXCEPTION_EXPECTEDSTATE_IS_NULL_5E8C2F32 = "expectedState 不能为空";
  public static final String EXCEPTION_CURRENTSTATE_IS_NULL_AEDB20DB = "currentState 不能为空";
  public static final String EXCEPTION_STATECHANGELISTENER_IS_NULL_635AE7D2 =
      "stateChangeListener 不能为空";
  public static final String EXCEPTION_ARG_CANNOT_TRANSITION_FROM_ARG_TO_ARG_8C680D30 =
      "%s 无法从 %s 转换到 %s";
  public static final String
      EXCEPTION_CANNOT_FIRE_STATE_CHANGE_EVENT_WHILE_HOLDING_THE_LOCK_35243BC4 =
          "持有锁时无法触发状态变更事件。";
  public static final String EXCEPTION_CANNOT_NOTIFY_WHILE_HOLDING_THE_LOCK_15625D48 =
      "持有锁时无法通知。";
  public static final String EXCEPTION_CANNOT_SET_STATE_WHILE_HOLDING_THE_LOCK_FA358188 =
      "持有锁时无法设置状态。";
  public static final String
      EXCEPTION_CANNOT_WAIT_FOR_STATE_CHANGE_WHILE_HOLDING_THE_LOCK_CBD9F784 =
          "持有锁时无法等待状态变更。";
  public static final String EXCEPTION_NEWSTATE_IS_NULL_D29A5454 = "newState 不能为空";

  public static final String EXCEPTION_PERCENTAGE_SHOULD_BE_IN_0_1_GOT_7A2C2F83 =
      "百分比应在 [0,1] 范围内，实际为 ";
  public static final String EXCEPTION_PERCENTILE_REQUIRES_2_ARGUMENTS_BUT_GOT_ARG_F3F1882F =
      "PERCENTILE 需要 2 个参数，但传入了 %d 个";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_PERCENTILE_AGGREGATION_ARG_E0C2513D =
      "Percentile 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_IN_PERCENTILE_AGGREGATION_ARG_8ACED24D =
      "PERCENTILE 聚合中不支持的数据类型：%s";
  public static final String EXCEPTION_PERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_66308C79 =
      "PercentileAccumulator 不支持统计信息";
}
