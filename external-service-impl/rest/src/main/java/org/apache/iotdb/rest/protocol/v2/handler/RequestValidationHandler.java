/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.v2.handler;

import org.apache.iotdb.rest.i18n.RestMessages;
import org.apache.iotdb.rest.protocol.handler.RequestLimitChecker;
import org.apache.iotdb.rest.protocol.v2.model.ExpressionRequest;
import org.apache.iotdb.rest.protocol.v2.model.InsertRecordsRequest;
import org.apache.iotdb.rest.protocol.v2.model.InsertTabletRequest;
import org.apache.iotdb.rest.protocol.v2.model.PrefixPathList;
import org.apache.iotdb.rest.protocol.v2.model.SQL;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RequestValidationHandler {

  private RequestValidationHandler() {}

  public static void validateSQL(SQL sql) {
    Objects.requireNonNull(sql.getSql(), RestMessages.SQL_SHOULD_NOT_BE_NULL);
    if (sql.getRowLimit() != null) {
      Validate.isTrue(sql.getRowLimit() > 0, RestMessages.ROW_LIMIT_SHOULD_BE_POSITIVE);
    }
  }

  public static void validatePrefixPaths(PrefixPathList prefixPathList) {
    Objects.requireNonNull(prefixPathList.getPrefixPaths(), RestMessages.PREFIX_PATHS_NOT_NULL);
    if (prefixPathList.getPrefixPaths().isEmpty()) {
      throw new IllegalArgumentException(RestMessages.PREFIX_PATHS_EMPTY);
    }
  }

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), RestMessages.TIMESTAMPS_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getIsAligned(), RestMessages.IS_ALIGNED_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getDevice(), RestMessages.DEVICE_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), RestMessages.DATA_TYPES_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getMeasurements(), RestMessages.MEASUREMENTS_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getValues(), RestMessages.VALUES_NOT_NULL);
    if (insertTabletRequest.getMeasurements().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException(
          RestMessages.EXCEPTION_MEASUREMENTS_AND_DATA_TYPES_SHOULD_HAVE_THE_SAME_SIZE_8526F19A);
    }
    if (insertTabletRequest.getValues().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException(
          RestMessages.EXCEPTION_VALUES_AND_DATA_TYPES_SHOULD_HAVE_THE_SAME_SIZE_0BAE701D);
    }
    int rowCount = insertTabletRequest.getTimestamps().size();
    int columnCount = insertTabletRequest.getMeasurements().size();
    RequestLimitChecker.checkRowCount(RestMessages.MESSAGE_INSERTTABLET_REQUEST_8647CA58, rowCount);
    RequestLimitChecker.checkColumnCount(
        RestMessages.MESSAGE_INSERTTABLET_REQUEST_8647CA58, columnCount);
    RequestLimitChecker.checkValueCount(
        RestMessages.MESSAGE_INSERTTABLET_REQUEST_8647CA58, (long) rowCount * columnCount);
    for (List<Object> column : insertTabletRequest.getValues()) {
      if (column.size() != rowCount) {
        throw new IllegalArgumentException(
            RestMessages
                .EXCEPTION_EACH_VALUE_COLUMN_SHOULD_HAVE_THE_SAME_SIZE_AS_TIMESTAMPS_523598BD);
      }
    }
    List<String> errorMessages = new ArrayList<>();
    String device = insertTabletRequest.getDevice();
    for (int i = 0; i < insertTabletRequest.getMeasurements().size(); i++) {
      String dataType = insertTabletRequest.getDataTypes().get(i);
      String measurement = insertTabletRequest.getMeasurements().get(i);
      if (isDataType(dataType)) {
        errorMessages.add(
            String.format(
                RestMessages.ILLEGAL_DEVICE_MEASUREMENT_DATA_TYPE, dataType, device, measurement));
      }
    }
    if (!errorMessages.isEmpty()) {
      throw new RuntimeException(String.join(RestMessages.ERROR_MESSAGE_SEPARATOR, errorMessages));
    }
  }

  public static void validateInsertRecordsRequest(InsertRecordsRequest insertRecordsRequest) {
    Objects.requireNonNull(insertRecordsRequest.getTimestamps(), RestMessages.TIMESTAMPS_NOT_NULL);
    Objects.requireNonNull(insertRecordsRequest.getIsAligned(), RestMessages.IS_ALIGNED_NOT_NULL);
    Objects.requireNonNull(insertRecordsRequest.getDevices(), RestMessages.DEVICES_NOT_NULL);
    Objects.requireNonNull(
        insertRecordsRequest.getDataTypesList(), RestMessages.DATA_TYPES_LIST_NOT_NULL);
    Objects.requireNonNull(insertRecordsRequest.getValuesList(), RestMessages.VALUES_LIST_NOT_NULL);
    Objects.requireNonNull(
        insertRecordsRequest.getMeasurementsList(), RestMessages.MEASUREMENTS_LIST_NOT_NULL);
    int rowCount = insertRecordsRequest.getDevices().size();
    if (insertRecordsRequest.getTimestamps().size() != rowCount
        || insertRecordsRequest.getMeasurementsList().size() != rowCount
        || insertRecordsRequest.getDataTypesList().size() != rowCount
        || insertRecordsRequest.getValuesList().size() != rowCount) {
      throw new IllegalArgumentException(
          RestMessages
              .EXCEPTION_DEVICES_TIMESTAMPS_MEASUREMENTS_LIST_DATA_TYPES_LIST_AND_VALUES_LIST_SHOULD_HAVE_THE_SAME_SIZE_5983AAC2);
    }
    RequestLimitChecker.checkRowCount(
        RestMessages.MESSAGE_INSERTRECORDS_REQUEST_93E12369, rowCount);
    List<String> errorMessages = new ArrayList<>();
    long valueCount = 0;
    for (int i = 0; i < insertRecordsRequest.getDataTypesList().size(); i++) {
      String device = insertRecordsRequest.getDevices().get(i);
      List<String> measurements = insertRecordsRequest.getMeasurementsList().get(i);
      List<String> dataTypes = insertRecordsRequest.getDataTypesList().get(i);
      List<Object> values = insertRecordsRequest.getValuesList().get(i);
      if (measurements.size() != dataTypes.size() || values.size() != dataTypes.size()) {
        throw new IllegalArgumentException(
            RestMessages
                .EXCEPTION_EACH_INSERTRECORDS_ROW_SHOULD_HAVE_THE_SAME_NUMBER_OF_MEASUREMENTS_DATA_TYPES_AND_VALUES_AD58AEF2);
      }
      RequestLimitChecker.checkColumnCount(
          RestMessages.MESSAGE_INSERTRECORDS_REQUEST_93E12369, measurements.size());
      valueCount += values.size();
      for (int c = 0; c < insertRecordsRequest.getDataTypesList().get(i).size(); c++) {
        String dataType = insertRecordsRequest.getDataTypesList().get(i).get(c);
        String measurement = measurements.get(c);
        if (isDataType(dataType)) {
          errorMessages.add(
              String.format(
                  RestMessages.ILLEGAL_DEVICE_MEASUREMENT_DATA_TYPE,
                  dataType,
                  device,
                  measurement));
        }
      }
    }
    RequestLimitChecker.checkValueCount(
        RestMessages.MESSAGE_INSERTRECORDS_REQUEST_93E12369, valueCount);
    if (!errorMessages.isEmpty()) {
      throw new RuntimeException(String.join(RestMessages.ERROR_MESSAGE_SEPARATOR, errorMessages));
    }
  }

  private static boolean isDataType(String dataType) {
    try {
      TSDataType.valueOf(dataType.toUpperCase());
    } catch (IllegalArgumentException e) {
      return true;
    }
    return false;
  }

  public static void validateExpressionRequest(ExpressionRequest expressionRequest) {
    Objects.requireNonNull(expressionRequest.getExpression(), RestMessages.EXPRESSION_NOT_NULL);
    Objects.requireNonNull(expressionRequest.getPrefixPath(), RestMessages.PREFIX_PATH_NOT_NULL);
    Objects.requireNonNull(expressionRequest.getStartTime(), RestMessages.START_TIME_NOT_NULL);
    Objects.requireNonNull(expressionRequest.getEndTime(), RestMessages.END_TIME_NOT_NULL);
  }
}
