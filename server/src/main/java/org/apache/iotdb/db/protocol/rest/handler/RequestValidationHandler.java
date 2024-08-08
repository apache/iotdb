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

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.protocol.rest.model.ExpressionRequest;
import org.apache.iotdb.db.protocol.rest.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.model.SQL;

import org.apache.commons.lang3.Validate;

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
    Objects.requireNonNull(insertTabletRequest.getTimestamps(), "timestamps should not be null");
    Objects.requireNonNull(insertTabletRequest.getIsAligned(), "is_aligned should not be null");
    Objects.requireNonNull(insertTabletRequest.getDevice(), "device should not be null");
    Objects.requireNonNull(insertTabletRequest.getDataTypes(), "data_types should not be null");
    Objects.requireNonNull(insertTabletRequest.getValues(), "values should not be null");
  }

  public static void validateExpressionRequest(ExpressionRequest expressionRequest) {
    Objects.requireNonNull(expressionRequest.getExpression(), "expression should not be null");
    Objects.requireNonNull(expressionRequest.getPrefixPath(), "prefix_path should not be null");
    Objects.requireNonNull(expressionRequest.getStartTime(), "start_time should not be null");
    Objects.requireNonNull(expressionRequest.getEndTime(), "end_time should not be null");
  }
}
