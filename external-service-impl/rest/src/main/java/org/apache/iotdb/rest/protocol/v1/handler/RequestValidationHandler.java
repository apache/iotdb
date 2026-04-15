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

package org.apache.iotdb.rest.protocol.v1.handler;

import org.apache.iotdb.rest.protocol.handler.RequestLimitChecker;
import org.apache.iotdb.rest.protocol.v1.model.ExpressionRequest;
import org.apache.iotdb.rest.protocol.v1.model.InsertTabletRequest;
import org.apache.iotdb.rest.protocol.v1.model.SQL;

import org.apache.tsfile.external.commons.lang3.Validate;

import java.util.List;
import java.util.Objects;

public class RequestValidationHandler {

  private RequestValidationHandler() {}

  public static void validateSQL(SQL sql) {
    Objects.requireNonNull(sql.getSql(), "sql should not be null");
    if (sql.getRowLimit() != null) {
      Validate.isTrue(sql.getRowLimit() > 0, "rowLimit should be positive");
    }
  }

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), "timestamps should not be null");
    Objects.requireNonNull(insertTabletRequest.getIsAligned(), "isAligned should not be null");
    Objects.requireNonNull(insertTabletRequest.getDeviceId(), "deviceId should not be null");
    Objects.requireNonNull(insertTabletRequest.getMeasurements(), "measurements should not be null");
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), "dataTypes should not be null");
    Objects.requireNonNull(insertTabletRequest.getValues(), "values should not be null");

    if (insertTabletRequest.getMeasurements().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException("measurements and dataTypes should have the same size");
    }
    if (insertTabletRequest.getValues().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException("values and dataTypes should have the same size");
    }

    int rowCount = insertTabletRequest.getTimestamps().size();
    int columnCount = insertTabletRequest.getMeasurements().size();
    RequestLimitChecker.checkRowCount("insertTablet request", rowCount);
    RequestLimitChecker.checkColumnCount("insertTablet request", columnCount);
    RequestLimitChecker.checkValueCount("insertTablet request", (long) rowCount * columnCount);

    for (List<Object> column : insertTabletRequest.getValues()) {
      if (column.size() != rowCount) {
        throw new IllegalArgumentException(
            "Each value column should have the same size as timestamps");
      }
    }
  }

  public static void validateExpressionRequest(ExpressionRequest expressionRequest) {
    Objects.requireNonNull(expressionRequest.getExpression(), "expression should not be null");
    Objects.requireNonNull(expressionRequest.getPrefixPath(), "prefixPath should not be null");
    Objects.requireNonNull(expressionRequest.getStartTime(), "startTime should not be null");
    Objects.requireNonNull(expressionRequest.getEndTime(), "endTime should not be null");
  }
}
