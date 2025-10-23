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

package org.apache.iotdb.db.protocol.rest.v2.handler;

import org.apache.iotdb.db.protocol.rest.v2.model.ExpressionRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertRecordsRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.PrefixPathList;
import org.apache.iotdb.db.protocol.rest.v2.model.SQL;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RequestValidationHandler {

  private RequestValidationHandler() {}

  public static void validateSQL(SQL sql) {
    Objects.requireNonNull(sql.getSql(), "sql should not be null");
    if (sql.getRowLimit() != null) {
      Validate.isTrue(sql.getRowLimit() > 0, "row_limit should be positive");
    }
  }

  public static void validatePrefixPaths(PrefixPathList prefixPathList) {
    Objects.requireNonNull(prefixPathList.getPrefixPaths(), "prefix_paths should not be null");
    if (prefixPathList.getPrefixPaths().isEmpty()) {
      throw new IllegalArgumentException("prefix_paths should not be empty");
    }
  }

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), "timestamps should not be null");
    Objects.requireNonNull(insertTabletRequest.getIsAligned(), "is_aligned should not be null");
    Objects.requireNonNull(insertTabletRequest.getDevice(), "device should not be null");
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), "data_types should not be null");
    Objects.requireNonNull(
        insertTabletRequest.getMeasurements(), "measurements should not be null");
    Objects.requireNonNull(insertTabletRequest.getValues(), "values should not be null");
    List<String> errorMessages = new ArrayList<>();
    String device = insertTabletRequest.getDevice();
    for (int i = 0; i < insertTabletRequest.getMeasurements().size(); i++) {
      String dataType = insertTabletRequest.getDataTypes().get(i);
      String measurement = insertTabletRequest.getMeasurements().get(i);
      if (isDataType(dataType)) {
        errorMessages.add(
            "The " + dataType + " data type of " + device + "." + measurement + " is illegal");
      }
    }
    if (!errorMessages.isEmpty()) {
      throw new RuntimeException(String.join(",", errorMessages));
    }
  }

  public static void validateInsertRecordsRequest(InsertRecordsRequest insertRecordsRequest) {
    Objects.requireNonNull(insertRecordsRequest.getTimestamps(), "timestamps should not be null");
    Objects.requireNonNull(insertRecordsRequest.getIsAligned(), "is_aligned should not be null");
    Objects.requireNonNull(insertRecordsRequest.getDevices(), "devices should not be null");
    Objects.requireNonNull(
        insertRecordsRequest.getDataTypesList(), "data_types_list should not be null");
    Objects.requireNonNull(insertRecordsRequest.getValuesList(), "values_list should not be null");
    Objects.requireNonNull(
        insertRecordsRequest.getMeasurementsList(), "measurements_list should not be null");
    List<String> errorMessages = new ArrayList<>();
    for (int i = 0; i < insertRecordsRequest.getDataTypesList().size(); i++) {
      String device = insertRecordsRequest.getDevices().get(i);
      List<String> measurements = insertRecordsRequest.getMeasurementsList().get(i);
      for (int c = 0; c < insertRecordsRequest.getDataTypesList().get(i).size(); c++) {
        String dataType = insertRecordsRequest.getDataTypesList().get(i).get(c);
        String measurement = measurements.get(c);
        if (isDataType(dataType)) {
          errorMessages.add(
              "The " + dataType + " data type of " + device + "." + measurement + " is illegal");
        }
      }
    }
    if (!errorMessages.isEmpty()) {
      throw new RuntimeException(String.join(",", errorMessages));
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
    Objects.requireNonNull(expressionRequest.getExpression(), "expression should not be null");
    Objects.requireNonNull(expressionRequest.getPrefixPath(), "prefix_path should not be null");
    Objects.requireNonNull(expressionRequest.getStartTime(), "start_time should not be null");
    Objects.requireNonNull(expressionRequest.getEndTime(), "end_time should not be null");
  }
}
