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

public final class SchemaMessages {

  // MNode 相关消息
  public static final String MEASUREMENT_NODE_CANNOT_GET_CHILD =
      "当前节点 %s 是 MeasurementMNode，无法获取子节点 %s";
  public static final String MEASUREMENT_NODE_CANNOT_GET_CHILD_LOG =
      "当前节点 {} 是 MeasurementMNode，无法获取子节点 {}";
  public static final String UNDEFINED_MNODE_TYPE = "未定义的 MNode 类型 %s";
  public static final String WRONG_MNODE_TYPE = "错误的 MNode 类型";
  public static final String WRONG_NODE_TYPE = "错误的节点类型";
  public static final String INVALID_INPUT = "无效的输入：%s";

  // Template 相关消息
  public static final String PATH_ALREADY_EXISTS = "路径已存在";

  // Table 相关消息
  public static final String CANNOT_REMOVE_TAG_COLUMN = "无法移除 tag 列：%s";
  public static final String UNKNOWN_TABLE_UPDATE_OPERATION_TYPE =
      "未知的 table 更新操作类型%s";

  // Column 相关消息
  public static final String UNSUPPORTED_COLUMN_TYPE_IN_TSFILE =
      "TsFile 中不支持的列类型：%s";
  public static final String UNKNOWN_COLUMN_TYPE = "未知的列类型：%s";
  public static final String SIZE_SHOULD_NOT_BE_NEGATIVE_ONE = "size 不应为 -1";

  // View 相关消息
  public static final String INVALID_VIEW_EXPRESSION_TYPE = "无效的 viewExpression 类型：%s";
  public static final String UNSUPPORTED_METHOD_FOR_LOGICAL_VIEW_SCHEMA =
      "LogicalViewSchema 不支持此方法";
  public static final String CANNOT_CALCULATE_VIEW_SCHEMA_SIZE =
      "序列化之前无法计算 view schema 的大小。";
  public static final String VIEW_STORES_ILLEGAL_PATH =
      "measurementID 为 [%s] 的 view 已损坏，其存储了非法路径 [%s]。";
  public static final String UNEXPECTED_VALUE_IN_LIKE_VIEW_EXPRESSION =
      "LikeViewExpression 中出现了意外的值：%s";
  public static final String SCHEMA_CASCADE_VALUE_MUST_BE_BOOLEAN =
      "schema_cascade 的值必须是布尔字面量，当前为 %s";

  // Filter 相关消息
  public static final String UNSUPPORTED_SCHEMA_FILTER_TYPE =
      "不支持的 schema filter 类型：%s";
  public static final String INPUT_SINGLE_FILTER_MUST_BE_DEVICE_ID_FILTER =
      "输入的单个 filter 必须是 DeviceIdFilter";

  // TTL 相关消息
  public static final String NOT_TTL_RULE_SET_FOR = "未设置 TTL 规则：%s";

  private SchemaMessages() {}

  public static final String PATH_DUPLICATED = "路径重复：";
  public static final String UNKNOWN_TABLE_UPDATE_OP_TYPE = "未知的表更新操作类型";
  public static final String SCHEMA_INVALID_INPUT = "无效输入：";

  public static final String TOO_MANY_DOTS_IN_TABLE_NAME =
      "表名中含有过多的点号：%s";

  public static final String S_IS_NULL =
      "%s 为 null";
}
