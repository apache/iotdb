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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.db.mpp.execution.operator.window.IWindow;
import org.apache.iotdb.db.mpp.execution.operator.window.TimeWindow;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AccumulatorTest {

  private TsBlock rawData;
  private Statistics statistics;
  private TimeRange defaultTimeRange = new TimeRange(0, Long.MAX_VALUE);

  private TimeWindow timeWindow = new TimeWindow(defaultTimeRange);

  @Before
  public void setUp() {
    initInputTsBlock();
  }

  private void initInputTsBlock() {
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE);
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypes);
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = 0; i < 100; i++) {
      timeColumnBuilder.writeLong(i);
      columnBuilders[0].writeDouble(i * 1.0);
      tsBlockBuilder.declarePosition();
    }
    rawData = tsBlockBuilder.build();

    statistics = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics.update(100L, 100d);
  }

  public Column[] getControlTimeAndValueColumn(IWindow curWindow, int columnIndex) {
    Column[] columns = new Column[3];
    columns[0] = curWindow.getControlColumn(rawData);
    columns[1] = rawData.getTimeColumn();
    columns[2] = rawData.getColumn(columnIndex);
    return columns;
  }

  @Test
  public void avgAccumulatorTest() {
    Accumulator avgAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.AVG, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, avgAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    intermediateResult[1] = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    avgAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(avgAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    intermediateResult[1] = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(100, intermediateResult[0].build().getLong(0));
    Assert.assertEquals(4950d, intermediateResult[1].build().getDouble(0), 0.001);

    // add intermediate result as input
    avgAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(49.5d, finalResult.build().getDouble(0), 0.001);

    avgAccumulator.reset();
    avgAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void countAccumulatorTest() {
    Accumulator countAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.COUNT, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    countAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(countAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(100, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    countAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(200, finalResult.build().getLong(0));

    countAccumulator.reset();
    countAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1, finalResult.build().getLong(0));
  }

  @Test
  public void extremeAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.EXTREME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    extremeAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void firstValueAccumulatorTest() {
    Accumulator firstValueAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.FIRST_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, firstValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    firstValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    firstValueAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertTrue(firstValueAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    firstValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(0L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    firstValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0L, finalResult.build().getDouble(0), 0.001);

    firstValueAccumulator.reset();
    firstValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void lastValueAccumulatorTest() {
    Accumulator lastValueAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.LAST_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, lastValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    lastValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    lastValueAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    lastValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(99L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    lastValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99L, finalResult.build().getDouble(0), 0.001);

    lastValueAccumulator.reset();
    lastValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void maxTimeAccumulatorTest() {
    Accumulator maxTimeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MAX_TIME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    maxTimeAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(maxTimeAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    maxTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99, finalResult.build().getLong(0));

    maxTimeAccumulator.reset();
    maxTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100, finalResult.build().getLong(0));
  }

  @Test
  public void minTimeAccumulatorTest() {
    Accumulator minTimeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MIN_TIME, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    minTimeAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertTrue(minTimeAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    minTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    minTimeAccumulator.reset();
    minTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100, finalResult.build().getLong(0));
  }

  @Test
  public void maxValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MAX_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    extremeAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(99d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(99d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void minValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.MIN_VALUE, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    extremeAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void sumAccumulatorTest() {
    Accumulator sumAccumulator =
        AccumulatorFactory.createAccumulator(AggregationType.SUM, TSDataType.DOUBLE, true);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] controlTimeAndValueColumn = getControlTimeAndValueColumn(timeWindow, 0);
    sumAccumulator.addInput(controlTimeAndValueColumn, timeWindow);
    Assert.assertFalse(sumAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(4950d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    sumAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(9900d, finalResult.build().getDouble(0), 0.001);

    sumAccumulator.reset();
    sumAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(100d, finalResult.build().getDouble(0), 0.001);
  }
}
