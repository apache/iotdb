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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;
import java.util.function.Function;

public class SeriesWindowManager implements IWindowManager {

  private final SeriesWindow seriesWindow;
  private boolean initialized;
  private boolean needSkip;
  // need a skip before update result to get full info about keep value and endTime in window
  // there are two skips in one process of group by series.
  private boolean isFirstSkip;
  private final Function<Long, Boolean> keepEvaluator;

  public SeriesWindowManager(SeriesWindowParameter seriesWindowParameter) {
    this.seriesWindow = new SeriesWindow(seriesWindowParameter);
    this.needSkip = false;
    this.keepEvaluator = initKeepEvaluator(seriesWindowParameter.getKeepExpression());
  }

  @Override
  public boolean isCurWindowInit() {
    return this.initialized;
  }

  @Override
  public void initCurWindow(TsBlock tsBlock) {
    this.initialized = true;
    this.seriesWindow.setTimeInitialized(false);
    this.seriesWindow.setKeep(0);
  }

  @Override
  public boolean hasNext(boolean hasMoreData) {
    return hasMoreData;
  }

  @Override
  public void next() {
    this.needSkip = true;
    this.initialized = false;
    isFirstSkip = true;
  }

  @Override
  public IWindow getCurWindow() {
    return seriesWindow;
  }

  @Override
  public TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock) {
    if (!needSkip) {
      return inputTsBlock;
    }

    if (inputTsBlock == null || inputTsBlock.isEmpty()) {
      return inputTsBlock;
    }

    Column controlColumn = seriesWindow.getControlColumn(inputTsBlock);
    TimeColumn timeColumn = inputTsBlock.getTimeColumn();
    int i = 0, size = inputTsBlock.getPositionCount();
    int k = 0;
    for (; i < size; i++) {
      if (isIgnoringNull() && controlColumn.isNull(i)) continue;
      if (isFirstSkip && (controlColumn.isNull(i) || !controlColumn.getBoolean(i))) {
        break;
      } else if (!isFirstSkip && !controlColumn.isNull(i) && controlColumn.getBoolean(i)) {
        break;
      }
      // judge whether we need update endTime
      if (isFirstSkip) {
        k++;
        long currentTime = timeColumn.getLong(i);
        if (seriesWindow.getEndTime() < currentTime) {
          seriesWindow.setEndTime(currentTime);
        }
      }
    }

    if (isFirstSkip) {
      isFirstSkip = false;
      seriesWindow.setKeep(seriesWindow.getKeep() + k);
    } else if (i < size) {
      // we can create a new window beginning at index i of inputTsBlock
      needSkip = false;
    }

    return inputTsBlock.subTsBlock(i);
  }

  @Override
  public TsBlockBuilder createResultTsBlockBuilder(List<Aggregator> aggregators) {
    List<TSDataType> dataTypes = getResultDataTypes(aggregators);
    // Judge whether we need output endTime column.
    if (seriesWindow.isOutputEndTime()) {
      dataTypes.add(0, TSDataType.INT64);
    }
    return new TsBlockBuilder(dataTypes);
  }

  @Override
  public void appendAggregationResult(
      TsBlockBuilder resultTsBlockBuilder, List<Aggregator> aggregators) {
    if (!keepEvaluator.apply(seriesWindow.getKeep())) {
      for (Aggregator aggregator : aggregators) aggregator.reset();
      return;
    }
    // Use the start time of eventWindow as default output time.
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(seriesWindow.getStartTime());

    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    int columnIndex = 0;
    if (seriesWindow.isOutputEndTime()) {
      columnBuilders[0].writeLong(seriesWindow.getEndTime());
      columnIndex = 1;
    }
    for (Aggregator aggregator : aggregators) {
      ColumnBuilder[] columnBuilder = new ColumnBuilder[aggregator.getOutputType().length];
      columnBuilder[0] = columnBuilders[columnIndex++];
      if (columnBuilder.length > 1) {
        columnBuilder[1] = columnBuilders[columnIndex++];
      }
      aggregator.outputResult(columnBuilder);
    }
    resultTsBlockBuilder.declarePosition();
  }

  @Override
  public boolean needSkipInAdvance() {
    return true;
  }

  @Override
  public boolean isIgnoringNull() {
    return seriesWindow.ignoringNull();
  }

  @Override
  public void setKeep(long keep) {
    seriesWindow.setKeep(seriesWindow.getKeep() + keep);
  }

  private static Function<Long, Boolean> initKeepEvaluator(Expression keepExpression) {
    // We have check semantic in FE,
    // keep expression must be ConstantOperand or CompareBinaryExpression here
    if (keepExpression instanceof ConstantOperand) {
      return keep -> keep >= Long.parseLong(keepExpression.toString());
    } else {
      long constant =
          Long.parseLong(
              ((CompareBinaryExpression) keepExpression)
                  .getRightExpression()
                  .getExpressionString());
      switch (keepExpression.getExpressionType()) {
        case LESS_THAN:
          return keep -> keep < constant;
        case LESS_EQUAL:
          return keep -> keep <= constant;
        case GREATER_THAN:
          return keep -> keep > constant;
        case GREATER_EQUAL:
          return keep -> keep >= constant;
        case EQUAL_TO:
          return keep -> keep == constant;
        case NON_EQUAL:
          return keep -> keep != constant;
        default:
          throw new IllegalArgumentException(
              "unsupported expression type: " + keepExpression.getExpressionType());
      }
    }
  }
}
