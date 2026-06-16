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

  // --- Execution ---

  public static final String ERROR_SETTING_FUTURE_STATE_FOR =
      "为 {} 设置 future 状态时出错";
  public static final String ERROR_NOTIFYING_STATE_CHANGE_LISTENER_FOR =
      "通知 {} 的状态变更监听器时出错";
  public static final String SERVER_IS_SHUTTING_DOWN =
      "服务器正在关闭";
}
