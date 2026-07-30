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
  public static final String
      MESSAGE_DATASET_ROW_SIZE_EXCEEDED_THE_GIVEN_MAX_ROW_SIZE_ARG_7A3F6452 =
          "数据集行数超过给定的最大行数（%d）";
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

  // --- RequestSizeLimitFilter ---
  public static final String
      MESSAGE_REST_REQUEST_BODY_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_424392C6 =
          "REST 请求体超过限制 %d 字节。请执行 SET CONFIGURATION"
              + " 'rest_max_request_body_size_in_bytes'='<bytes>' 增大限制。";
  public static final String
      MESSAGE_REST_REQUEST_BODY_MEMORY_QUOTA_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_TOTAL_CONCURRENT_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_F07B9DDD =
          "REST 请求体内存配额超过限制 %d 字节。请执行 SET CONFIGURATION"
              + " 'rest_max_total_concurrent_request_body_size_in_bytes'='<bytes>' 增大配额。";

  // --- RequestLimitChecker ---
  public static final String MESSAGE_INSERTTABLET_REQUEST_8647CA58 = "insertTablet 请求";
  public static final String MESSAGE_INSERTRECORDS_REQUEST_93E12369 = "insertRecords 请求";
  public static final String MESSAGE_TABLE_INSERTTABLET_REQUEST_573D371C =
      "table insertTablet 请求";
  public static final String EXCEPTION_ARG_ROW_COUNT_ARG_EXCEEDS_LIMIT_ARG_EE427E4B =
      "%s 行数 %d 超过限制 %d";
  public static final String EXCEPTION_ARG_COLUMN_COUNT_ARG_EXCEEDS_LIMIT_ARG_DEE9637E =
      "%s 列数 %d 超过限制 %d";
  public static final String EXCEPTION_ARG_VALUE_COUNT_ARG_EXCEEDS_LIMIT_ARG_77F95703 =
      "%s value 数 %d 超过限制 %d";

  // --- RequestValidationHandler ---
  public static final String
      EXCEPTION_MEASUREMENTS_AND_DATATYPES_SHOULD_HAVE_THE_SAME_SIZE_FF715FA9 =
          "measurements 和 dataTypes 的数量应相同";
  public static final String EXCEPTION_VALUES_AND_DATATYPES_SHOULD_HAVE_THE_SAME_SIZE_5BC1D604 =
      "values 和 dataTypes 的数量应相同";
  public static final String
      EXCEPTION_EACH_VALUE_COLUMN_SHOULD_HAVE_THE_SAME_SIZE_AS_TIMESTAMPS_523598BD =
          "每个 value 列的数量应与 timestamps 数量相同";
  public static final String
      EXCEPTION_MEASUREMENTS_AND_DATA_TYPES_SHOULD_HAVE_THE_SAME_SIZE_8526F19A =
          "measurements 和 data_types 的数量应相同";
  public static final String EXCEPTION_VALUES_AND_DATA_TYPES_SHOULD_HAVE_THE_SAME_SIZE_0BAE701D =
      "values 和 data_types 的数量应相同";
  public static final String
      EXCEPTION_DEVICES_TIMESTAMPS_MEASUREMENTS_LIST_DATA_TYPES_LIST_AND_VALUES_LIST_SHOULD_HAVE_THE_SAME_SIZE_5983AAC2 =
          "devices、timestamps、measurements_list、data_types_list 和 values_list 的数量应相同";
  public static final String
      EXCEPTION_EACH_INSERTRECORDS_ROW_SHOULD_HAVE_THE_SAME_NUMBER_OF_MEASUREMENTS_DATA_TYPES_AND_VALUES_AD58AEF2 =
          "每个 insertRecords 行中的 measurements、data types 和 values 数量应相同";

  private RestMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_0521CEDE = "不支持的数据类型: ";

}
