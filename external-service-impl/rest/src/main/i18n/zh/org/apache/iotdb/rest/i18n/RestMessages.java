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

package org.apache.iotdb.rest.i18n;

public final class RestMessages {

  // --- RestService ---
  public static final String REST_SERVICE_START_FAILED = "RestService 启动失败：{}";
  public static final String REST_SERVICE_START_SUCCESS = "RestService 启动成功";
  public static final String REST_SERVICE_STOP_FAILED = "RestService 停止失败：{}";

  // --- StatementConstructionHandler (v1 / v2 / table) ---
  public static final String INVALID_INPUT = "无效输入：";

  // --- RequestValidationHandler (v2) ---
  public static final String PREFIX_PATHS_EMPTY = "prefix_paths 不能为空";
  public static final String SQL_SHOULD_NOT_BE_NULL = "sql 不能为空";
  public static final String ROW_LIMIT_SHOULD_BE_POSITIVE = "row_limit 应为正数";
  public static final String ROW_LIMIT_CAMEL_SHOULD_BE_POSITIVE = "rowLimit 应为正数";
  public static final String PREFIX_PATHS_NOT_NULL = "prefix_paths 不能为空";
  public static final String TIMESTAMPS_NOT_NULL = "timestamps 不能为空";
  public static final String IS_ALIGNED_NOT_NULL = "is_aligned 不能为空";
  public static final String IS_ALIGNED_CAMEL_NOT_NULL = "isAligned 不能为空";
  public static final String DEVICE_NOT_NULL = "device 不能为空";
  public static final String DEVICE_ID_NOT_NULL = "deviceId 不能为空";
  public static final String DATA_TYPES_NOT_NULL = "data_types 不能为空";
  public static final String DATA_TYPES_CAMEL_NOT_NULL = "dataTypes 不能为空";
  public static final String MEASUREMENTS_NOT_NULL = "measurements 不能为空";
  public static final String VALUES_NOT_NULL = "values 不能为空";
  public static final String DEVICES_NOT_NULL = "devices 不能为空";
  public static final String DATA_TYPES_LIST_NOT_NULL = "data_types_list 不能为空";
  public static final String VALUES_LIST_NOT_NULL = "values_list 不能为空";
  public static final String MEASUREMENTS_LIST_NOT_NULL = "measurements_list 不能为空";
  public static final String EXPRESSION_NOT_NULL = "expression 不能为空";
  public static final String PREFIX_PATH_NOT_NULL = "prefix_path 不能为空";
  public static final String PREFIX_PATH_CAMEL_NOT_NULL = "prefixPath 不能为空";
  public static final String START_TIME_NOT_NULL = "start_time 不能为空";
  public static final String START_TIME_CAMEL_NOT_NULL = "startTime 不能为空";
  public static final String END_TIME_NOT_NULL = "end_time 不能为空";
  public static final String END_TIME_CAMEL_NOT_NULL = "endTime 不能为空";
  public static final String DATABASE_NOT_NULL = "database 不能为空";
  public static final String TABLE_NOT_NULL = "table 不能为空";
  public static final String COLUMN_NAMES_NOT_NULL = "column_names 不能为空";
  public static final String COLUMN_CATEGORIES_NOT_NULL = "column_categories 不能为空";
  public static final String COLUMN_NAMES_AND_COLUMN_CATEGORIES_SIZE_MISMATCH =
      "column_names 和 column_categories 的数量应相同";
  public static final String COLUMN_CATEGORIES_AND_DATA_TYPES_SIZE_MISMATCH =
      "column_categories 和 data_types 的数量应相同";
  public static final String VALUES_AND_TIMESTAMPS_SIZE_MISMATCH =
      "values 和 timestamps 的数量应相同";
  public static final String ILLEGAL_TABLE_DATA_TYPE = "%s 是 %s 的非法数据类型";
  public static final String ILLEGAL_DEVICE_MEASUREMENT_DATA_TYPE =
      "%s 是 %s.%s 的非法数据类型";
  public static final String ROW_VALUES_SIZE_MISMATCH =
      "第 %d 行的 values 数量与 data_types 数量不相等";
  public static final String ERROR_MESSAGE_SEPARATOR = "，";

  private RestMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_0521CEDE = "不支持的数据类型: ";

}
