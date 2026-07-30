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

package org.apache.iotdb.rest.protocol.table.v1.handler;

import org.apache.iotdb.rest.i18n.RestMessages;
import org.apache.iotdb.rest.protocol.handler.RequestLimitChecker;
import org.apache.iotdb.rest.protocol.table.v1.model.InsertTabletRequest;
import org.apache.iotdb.rest.protocol.table.v1.model.SQL;

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

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getDatabase(), RestMessages.DATABASE_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getTable(), RestMessages.TABLE_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getColumnNames(), RestMessages.COLUMN_NAMES_NOT_NULL);
    Objects.requireNonNull(
        insertTabletRequest.getColumnCategories(), RestMessages.COLUMN_CATEGORIES_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), RestMessages.DATA_TYPES_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), RestMessages.TIMESTAMPS_NOT_NULL);
    Objects.requireNonNull(insertTabletRequest.getValues(), RestMessages.VALUES_NOT_NULL);
    List<String> errorMessages = new ArrayList<>();
    String table = insertTabletRequest.getTable();
    if (insertTabletRequest.getColumnCategories().size() == 0
        || insertTabletRequest.getColumnCategories().size()
            != insertTabletRequest.getColumnNames().size()) {
      errorMessages.add(RestMessages.COLUMN_NAMES_AND_COLUMN_CATEGORIES_SIZE_MISMATCH);
    }
    if (insertTabletRequest.getColumnCategories().size()
        != insertTabletRequest.getDataTypes().size()) {
      errorMessages.add(RestMessages.COLUMN_CATEGORIES_AND_DATA_TYPES_SIZE_MISMATCH);
    }
    if (insertTabletRequest.getTimestamps().size() != insertTabletRequest.getValues().size()) {
      errorMessages.add(RestMessages.VALUES_AND_TIMESTAMPS_SIZE_MISMATCH);
    }

    int rowCount = insertTabletRequest.getTimestamps().size();
    int columnCount = insertTabletRequest.getColumnNames().size();
    RequestLimitChecker.checkRowCount(
        RestMessages.MESSAGE_TABLE_INSERTTABLET_REQUEST_573D371C, rowCount);
    RequestLimitChecker.checkColumnCount(
        RestMessages.MESSAGE_TABLE_INSERTTABLET_REQUEST_573D371C, columnCount);
    RequestLimitChecker.checkValueCount(
        RestMessages.MESSAGE_TABLE_INSERTTABLET_REQUEST_573D371C, (long) rowCount * columnCount);

    for (int i = 0; i < insertTabletRequest.getDataTypes().size(); i++) {
      String dataType = insertTabletRequest.getDataTypes().get(i);
      if (isDataType(dataType)) {
        errorMessages.add(String.format(RestMessages.ILLEGAL_TABLE_DATA_TYPE, dataType, table));
      }
    }

    int dataTypeSize = insertTabletRequest.getDataTypes().size();
    for (int i = 0; i < insertTabletRequest.getValues().size(); i++) {
      List<Object> values = insertTabletRequest.getValues().get(i);
      if (dataTypeSize != values.size()) {
        errorMessages.add(String.format(RestMessages.ROW_VALUES_SIZE_MISMATCH, i + 1));
      }
    }

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
}
