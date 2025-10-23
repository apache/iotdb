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

package org.apache.iotdb.db.protocol.rest.table.v1.handler;

import org.apache.iotdb.db.protocol.rest.table.v1.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.table.v1.model.SQL;

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

  public static void validateInsertTabletRequest(InsertTabletRequest insertTabletRequest) {
    Objects.requireNonNull(insertTabletRequest.getDatabase(), "database should not be null");
    Objects.requireNonNull(insertTabletRequest.getTable(), "table should not be null");
    Objects.requireNonNull(insertTabletRequest.getColumnNames(), "column_names should not be null");
    Objects.requireNonNull(
        insertTabletRequest.getColumnCategories(), "column_categories should not be null");
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), "data_types should not be null");
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), "timestamps should not be null");
    Objects.requireNonNull(insertTabletRequest.getValues(), "values should not be null");
    List<String> errorMessages = new ArrayList<>();
    String table = insertTabletRequest.getTable();
    if (insertTabletRequest.getColumnCategories().size() == 0
        || insertTabletRequest.getColumnCategories().size()
            != insertTabletRequest.getColumnNames().size()) {
      errorMessages.add("column_names and column_categories should have the same size");
    }
    if (insertTabletRequest.getColumnCategories().size()
        != insertTabletRequest.getDataTypes().size()) {
      errorMessages.add("column_categories and data_types should have the same size");
    }
    if (insertTabletRequest.getTimestamps().size() != insertTabletRequest.getValues().size()) {
      errorMessages.add("values and timestamps should have the same size");
    }

    for (int i = 0; i < insertTabletRequest.getDataTypes().size(); i++) {
      String dataType = insertTabletRequest.getDataTypes().get(i);
      if (isDataType(dataType)) {
        errorMessages.add("The " + dataType + " data type of " + table + " is illegal");
      }
    }

    int dataTypeSize = insertTabletRequest.getDataTypes().size();
    for (int i = 0; i < insertTabletRequest.getValues().size(); i++) {
      List<Object> values = insertTabletRequest.getValues().get(i);
      if (dataTypeSize != values.size()) {
        errorMessages.add(
            "The number of values in the "
                + (i + 1)
                + "th row is not equal to the data_types size");
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
}
