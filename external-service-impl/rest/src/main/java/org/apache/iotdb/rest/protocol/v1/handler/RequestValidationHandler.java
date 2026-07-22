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

import org.apache.iotdb.rest.i18n.RestMessages;
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
    Objects.requireNonNull(sql.getSql(), RestMessages.SQL_SHOULD_NOT_BE_NULL);
    if (sql.getRowLimit() != null) {
      Validate.isTrue(sql.getRowLimit() > 0, RestMessages.ROW_LIMIT_CAMEL_SHOULD_BE_POSITIVE);
    }
  }

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), RestMessages.TIMESTAMPS_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getIsAligned(), RestMessages.IS_ALIGNED_CAMEL_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getDeviceId(), RestMessages.DEVICE_ID_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getMeasurements(), RestMessages.MEASUREMENTS_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getDataTypes(), RestMessages.DATA_TYPES_CAMEL_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getValues(), RestMessages.VALUES_NOT_NULL);

    if (insertTabletRequest.getMeasurements().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException(
          RestMessages.EXCEPTION_MEASUREMENTS_AND_DATATYPES_SHOULD_HAVE_THE_SAME_SIZE_FF715FA9);
    }
    if (insertTabletRequest.getValues().size() != insertTabletRequest.getDataTypes().size()) {
      throw new IllegalArgumentException(
          RestMessages.EXCEPTION_VALUES_AND_DATATYPES_SHOULD_HAVE_THE_SAME_SIZE_5BC1D604);
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
  }

  public static void validateExpressionRequest(ExpressionRequest expressionRequest) {
    Objects.requireNonNull(expressionRequest.getExpression(), RestMessages.EXPRESSION_NOT_NULL);
    Objects.requireNonNull(
        expressionRequest.getPrefixPath(), RestMessages.PREFIX_PATH_CAMEL_NOT_NULL);
    Objects.requireNonNull(
        expressionRequest.getStartTime(), RestMessages.START_TIME_CAMEL_NOT_NULL);
    Objects.requireNonNull(expressionRequest.getEndTime(), RestMessages.END_TIME_CAMEL_NOT_NULL);
  }
}
