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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

public class FilterAndProjectOperator implements ProcessOperator {

  private final Operator inputOperator;

  private List<LeafColumnTransformer> filterLeafColumnTransformerList;

  private ColumnTransformer filterOutputTransformer;

  private List<ColumnTransformer> commonTransformerList;

  private List<LeafColumnTransformer> projectLeafColumnTransformerList;

  private List<ColumnTransformer> projectOutputTransformerList;

  private final TsBlockBuilder filterTsBlockBuilder;

  private final boolean hasNonMappableUDF;

  private final OperatorContext operatorContext;

  public FilterAndProjectOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> filterOutputDataTypes,
      List<LeafColumnTransformer> filterLeafColumnTransformerList,
      ColumnTransformer filterOutputTransformer,
      List<ColumnTransformer> commonTransformerList,
      List<LeafColumnTransformer> projectLeafColumnTransformerList,
      List<ColumnTransformer> projectOutputTransformerList,
      boolean hasNonMappableUDF) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.filterTsBlockBuilder = new TsBlockBuilder(8, filterOutputDataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    TsBlock input = inputOperator.next();
    if (input == null) {
      return null;
    }

    TsBlock filterResult = getFilterTsBlock(input);

    // contains non-mappable udf, we leave calculation for TransformOperator
    if (hasNonMappableUDF) {
      return filterResult;
    }
    return getTransformedTsBlock(filterResult);
  }

  /**
   * Return the TsBlock that contains both initial input columns and columns of common
   * subexpressions after filtering
   *
   * @param input
   * @return
   */
  private TsBlock getFilterTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    for (LeafColumnTransformer leafColumnTransformer : filterLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    filterOutputTransformer.tryEvaluate();

    Column filterColumn = filterOutputTransformer.getColumn();

    // reuse this builder
    filterTsBlockBuilder.reset();

    final TimeColumnBuilder timeBuilder = filterTsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    List<Column> resultColumns = new ArrayList<>();
    for (int i = 0, n = input.getValueColumnCount(); i < n; i++) {
      resultColumns.add(input.getColumn(i));
    }

    // todo: remove this if, add calculated common sub expressions anyway
    if (!hasNonMappableUDF) {
      // get result of calculated common sub expressions
      for (ColumnTransformer columnTransformer : commonTransformerList) {
        resultColumns.add(columnTransformer.getColumn());
      }
    }

    // construct result TsBlock of filter
    int rowCount = 0;
    for (int i = 0, n = resultColumns.size(); i < n; i++) {
      Column curColumn = resultColumns.get(i);
      for (int j = 0; j < positionCount; j++) {
        if (!filterColumn.isNull(j) && filterColumn.getBoolean(j)) {
          if (i == 0) {
            rowCount++;
            timeBuilder.writeLong(originTimeColumn.getLong(j));
          }
          if (curColumn.isNull(j)) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].write(curColumn, j);
          }
        }
      }
    }

    filterTsBlockBuilder.declarePositions(rowCount);
    return filterTsBlockBuilder.build();
  }

  private TsBlock getTransformedTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed pre calculated data
    for (LeafColumnTransformer leafColumnTransformer : projectLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    List<Column> resultColumns = new ArrayList<>();
    for (ColumnTransformer columnTransformer : projectOutputTransformerList) {
      columnTransformer.tryEvaluate();
      resultColumns.add(columnTransformer.getColumn());
    }
    return TsBlock.wrapBlocksWithoutCopy(
        positionCount, originTimeColumn, resultColumns.toArray(new Column[0]));
  }

  @Override
  public boolean hasNext() {
    return inputOperator.hasNext();
  }

  @Override
  public boolean isFinished() {
    return inputOperator.isFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }
}
