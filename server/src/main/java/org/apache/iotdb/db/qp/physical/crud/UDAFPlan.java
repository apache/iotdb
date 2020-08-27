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

package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.udf.core.UDAFExecutor;
import org.apache.iotdb.db.query.udf.core.UDFContext;

public class UDAFPlan extends AggregationPlan implements UDFPlan {

  protected List<Integer> columnIndex2DeduplicatedExecutorIndex;
  protected Map<String, Integer> column2DeduplicatedExecutorIndex;

  protected List<String> deduplicatedColumns;
  protected List<UDAFExecutor> deduplicatedExecutors;

  public UDAFPlan() {
    super();
    setOperatorType(Operator.OperatorType.UDAF);
  }

  @Override
  public void initializeUdfExecutors(List<UDFContext> udfContexts) throws QueryProcessException {
    columnIndex2DeduplicatedExecutorIndex = new ArrayList<>();
    column2DeduplicatedExecutorIndex = new HashMap<>();
    deduplicatedColumns = new ArrayList<>();
    deduplicatedExecutors = new ArrayList<>();

    for (UDFContext context : udfContexts) {
      if (context == null) {
        columnIndex2DeduplicatedExecutorIndex.add(null);
        continue;
      }

      String column = context.getColumn();
      Integer index = column2DeduplicatedExecutorIndex.get(column);
      if (index != null) {
        columnIndex2DeduplicatedExecutorIndex.add(index);
        continue;
      }

      index = deduplicatedExecutors.size();
      columnIndex2DeduplicatedExecutorIndex.add(index);
      deduplicatedColumns.add(column);
      UDAFExecutor executor = new UDAFExecutor(context);
      executor.initializeUDF();
      deduplicatedExecutors.add(executor);
      column2DeduplicatedExecutorIndex.put(column, index);
    }
  }

  public UDAFExecutor getExecutor(int index) {
    Integer executorIndex = columnIndex2DeduplicatedExecutorIndex.get(index);
    return executorIndex == null ? null : deduplicatedExecutors.get(executorIndex);
  }

  public UDAFExecutor getExecutor(String column) {
    Integer executorIndex = column2DeduplicatedExecutorIndex.get(column);
    return executorIndex == null ? null : deduplicatedExecutors.get(executorIndex);
  }

  public List<UDAFExecutor> getDeduplicatedExecutors() {
    return deduplicatedExecutors;
  }

  public String getColumn(int index) {
    Integer columnIndex = columnIndex2DeduplicatedExecutorIndex.get(index);
    return columnIndex == null ? null : deduplicatedColumns.get(columnIndex);
  }

  public List<String> getDeduplicatedColumns() {
    return deduplicatedColumns;
  }
}
