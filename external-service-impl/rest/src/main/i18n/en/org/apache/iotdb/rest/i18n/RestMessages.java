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
  public static final String REST_SERVICE_START_FAILED = "RestService failed to start: {}";
  public static final String REST_SERVICE_START_SUCCESS = "start RestService successfully";
  public static final String REST_SERVICE_STOP_FAILED = "RestService failed to stop: {}";

  // --- StatementConstructionHandler (v1 / v2 / table) ---
  public static final String INVALID_INPUT = "Invalid input: ";

  // --- RequestValidationHandler (v2) ---
  public static final String PREFIX_PATHS_EMPTY = "prefix_paths should not be empty";
  public static final String SQL_SHOULD_NOT_BE_NULL = "sql should not be null";
  public static final String ROW_LIMIT_SHOULD_BE_POSITIVE = "row_limit should be positive";
  public static final String ROW_LIMIT_CAMEL_SHOULD_BE_POSITIVE = "rowLimit should be positive";
  public static final String
      MESSAGE_DATASET_ROW_SIZE_EXCEEDED_THE_GIVEN_MAX_ROW_SIZE_ARG_7A3F6452 =
          "Dataset row size exceeded the given max row size (%d)";
  public static final String PREFIX_PATHS_NOT_NULL = "prefix_paths should not be null";
  public static final String TIMESTAMPS_NOT_NULL = "timestamps should not be null";
  public static final String IS_ALIGNED_NOT_NULL = "is_aligned should not be null";
  public static final String IS_ALIGNED_CAMEL_NOT_NULL = "isAligned should not be null";
  public static final String DEVICE_NOT_NULL = "device should not be null";
  public static final String DEVICE_ID_NOT_NULL = "deviceId should not be null";
  public static final String DATA_TYPES_NOT_NULL = "data_types should not be null";
  public static final String DATA_TYPES_CAMEL_NOT_NULL = "dataTypes should not be null";
  public static final String MEASUREMENTS_NOT_NULL = "measurements should not be null";
  public static final String VALUES_NOT_NULL = "values should not be null";
  public static final String DEVICES_NOT_NULL = "devices should not be null";
  public static final String DATA_TYPES_LIST_NOT_NULL = "data_types_list should not be null";
  public static final String VALUES_LIST_NOT_NULL = "values_list should not be null";
  public static final String MEASUREMENTS_LIST_NOT_NULL = "measurements_list should not be null";
  public static final String EXPRESSION_NOT_NULL = "expression should not be null";
  public static final String PREFIX_PATH_NOT_NULL = "prefix_path should not be null";
  public static final String PREFIX_PATH_CAMEL_NOT_NULL = "prefixPath should not be null";
  public static final String START_TIME_NOT_NULL = "start_time should not be null";
  public static final String START_TIME_CAMEL_NOT_NULL = "startTime should not be null";
  public static final String END_TIME_NOT_NULL = "end_time should not be null";
  public static final String END_TIME_CAMEL_NOT_NULL = "endTime should not be null";
  public static final String DATABASE_NOT_NULL = "database should not be null";
  public static final String TABLE_NOT_NULL = "table should not be null";
  public static final String COLUMN_NAMES_NOT_NULL = "column_names should not be null";
  public static final String COLUMN_CATEGORIES_NOT_NULL =
      "column_categories should not be null";
  public static final String COLUMN_NAMES_AND_COLUMN_CATEGORIES_SIZE_MISMATCH =
      "column_names and column_categories should have the same size";
  public static final String COLUMN_CATEGORIES_AND_DATA_TYPES_SIZE_MISMATCH =
      "column_categories and data_types should have the same size";
  public static final String VALUES_AND_TIMESTAMPS_SIZE_MISMATCH =
      "values and timestamps should have the same size";
  public static final String ILLEGAL_TABLE_DATA_TYPE =
      "The %s data type of %s is illegal";
  public static final String ILLEGAL_DEVICE_MEASUREMENT_DATA_TYPE =
      "The %s data type of %s.%s is illegal";
  public static final String ROW_VALUES_SIZE_MISMATCH =
      "The number of values in the %dth row is not equal to the data_types size";
  public static final String ERROR_MESSAGE_SEPARATOR = ",";

  private RestMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNSUPPORTED_DATA_TYPE_0521CEDE = "unsupported data type: ";

}
