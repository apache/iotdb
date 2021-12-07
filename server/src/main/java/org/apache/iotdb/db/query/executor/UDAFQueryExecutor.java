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
package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDAFPlan;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class UDAFQueryExecutor {
  private final String WRONG_BOOLEAN_TYPE_MESSAGE =
      "Boolean type is not supported in nested aggregation expression.";
  private final String WRONG_TEXT_TYPE_MESSAGE =
      "Text type is not supported in nested aggregation expression.";
  protected UDAFPlan udafPlan;
  private Set<ResultColumn> deduplicatedResultColumns;

  public UDAFQueryExecutor(UDAFPlan udafPlan) {
    this.udafPlan = udafPlan;
    init();
  }

  private void init() {
    deduplicatedResultColumns = new LinkedHashSet<>();
    for (ResultColumn resultColumn : udafPlan.getResultColumns()) {
      deduplicatedResultColumns.add(resultColumn);
    }
  }

  public SingleDataSet convertInnerAggregationDataset(SingleDataSet singleDataSet)
      throws QueryProcessException, IOException {
    RowRecord innerRowRecord = singleDataSet.nextWithoutConstraint();
    List<Field> innerAggregationResults = innerRowRecord.getFields();
    RowRecord record = new RowRecord(0);
    ArrayList<TSDataType> dataTypes = new ArrayList<>();
    ArrayList<PartialPath> paths = new ArrayList<>();
    for (ResultColumn resultColumn : deduplicatedResultColumns) {
      dataTypes.add(TSDataType.DOUBLE);
      paths.add(null);
      record.addField(
          resultColumn.getExpression().evaluateNestedExpressions(innerAggregationResults, udafPlan),
          TSDataType.DOUBLE);
    }
    for (ResultColumn resultColumn : this.udafPlan.getResultColumns()) {
      resultColumn.setDataType(TSDataType.DOUBLE);
    }
    SingleDataSet dataSet = new SingleDataSet(paths, dataTypes);
    dataSet.setRecord(record);
    return dataSet;
  }
}
