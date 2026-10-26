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

package org.apache.iotdb.db.i18n;

public final class DataNodeSchemaMessages {

  private DataNodeSchemaMessages() {}

  public static final String
      EXCEPTION_CANNOT_RECOVER_DUPLICATED_SCHEMAREGION_ARG_FOUND_IN_DATABASES_ARG_AND_ARG_D9F60E05 =
          "无法恢复重复的 SchemaRegion [%s]，其同时存在于数据库 [%s] 和 [%s] 中。";

  public static final String FAILED_TO_RECOVER_TAG_INDEX =
      "恢复测点 [{}] 的 tag 索引失败，数据库为 [{}]，SchemaRegion 为 [{}]，tag 偏移量为 [{}]。";
  public static final String SCHEMA_REGION_FAILED_TO_RECOVER =
      "SchemaRegion [%d] 在 StorageGroup [%s] 中恢复失败。";
  public static final String SDT_COMPRESSION_DEVIATION_REQUIRED = "SDT 压缩必须设置 compdev 属性";
  public static final String SDT_COMPRESSION_DEVIATION_NEGATIVE = "SDT 属性 compdev 必须为非负数，实际值为 [%s]";
  public static final String SDT_COMPRESSION_DEVIATION_FORMAT_ERROR =
      "SDT 属性 compdev 必须为数字，实际值为 [%s]";
  public static final String SDT_COMPRESSION_MAX_GREATER_THAN_MIN =
      "SDT 属性 compmaxtime [%s] 必须大于 compmintime [%s]";
  public static final String SDT_COMPRESSION_TIME_NEGATIVE = "SDT 属性 %s 必须为非负数，实际值为 [%s]";
  public static final String SDT_COMPRESSION_TIME_FORMAT_ERROR = "SDT 属性 %s 必须为 64 位整数，实际值为 [%s]";
  public static final String SDT_ENABLED_NO_COMPRESSION_TIME =
      "时间序列 [{}] 已启用 SDT 但未设置属性 [{}]，使用默认值 [{}]。";
  public static final String CREATE_TEMPLATE_ERROR = "在 ConfigNode 上创建元数据模板 [%s] 失败：%s";
  public static final String GET_ALL_TEMPLATE_ERROR = "从 ConfigNode 获取元数据模板列表失败：%s";
  public static final String GET_TEMPLATE_INFO_ERROR = "从 ConfigNode 获取元数据模板 [%s] 失败：%s";
  public static final String ILLEGAL_PATH_LOG = "非法路径 {}";
}
