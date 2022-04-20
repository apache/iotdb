/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;

/**
 * GroupByLevelOperator is a one-to-one operator, which accepts one tsBlock as input and group its
 * columns by given levels and column names, and returns grouped TsBlock.
 */
public class GroupByLevelOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  Operator child;

  /**
   * Means the ith column will be calculated by which columns. For example, if groupedColumns is
   * [[0,2] [1,3]], which means the first column will be calculated by the 0th and 2nd column from
   * inputTsBlock.
   */
  private final List<List<Integer>> groupedColumns;
  /**
   * Means the aggregation type of groupedColumns, the size of aggregationTypes should be equals to
   * groupedColumns.
   */
  private final List<AggregationType> aggregationTypes;
  /**
   * Means the dataType of groupedColumns, which is calculated by AggregationType and datatype of
   * seriesPath.
   */
  private final List<TSDataType> dataTypes;

  /** Used to calculate result of output TsBlock. */
  private AggregateResult[] aggregateResultList;

  private TsBlockBuilder outputTsBlockBuilder;

  GroupByLevelOperator(
      OperatorContext operatorContext,
      Operator child,
      List<List<Integer>> groupedColumns,
      List<AggregationType> aggregationTypes,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.groupedColumns = groupedColumns;
    this.aggregationTypes = aggregationTypes;
    this.dataTypes = dataTypes;
    this.aggregateResultList = new AggregateResult[groupedColumns.size()];
    for (int i = 0; i < aggregateResultList.length; i++) {
      // Since we don't calculate by raw data, the ascending order is doesn't matter.
      this.aggregateResultList[i] =
          AggregateResultFactory.getAggrResultByType(
              aggregationTypes.get(i), dataTypes.get(i), true);
    }
    outputTsBlockBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    ListenableFuture<Void> blocked = child.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    TsBlock inputTsBlock = child.next();

    outputTsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = outputTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = outputTsBlockBuilder.getValueColumnBuilders();
    for (int i = 0; i < inputTsBlock.getPositionCount(); i++) {
      timeColumnBuilder.writeLong(inputTsBlock.getTimeColumn().getLong(i));

      for (int j = 0; j < aggregateResultList.length; j++) {
        aggregateResultList[j].reset();
        // merge results
        for (int columnIndex : this.groupedColumns.get(j)) {
          aggregateResultList[j].merge(inputTsBlock.getColumn(columnIndex).getObject(i));
        }
        columnBuilders[j].writeObject(aggregateResultList[i].getResult());
      }
    }

    return outputTsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() throws IOException {
    return child.isFinished();
  }
}
