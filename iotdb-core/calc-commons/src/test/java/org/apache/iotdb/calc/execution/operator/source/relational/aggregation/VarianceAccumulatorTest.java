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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.aggregation.VarianceAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedVarianceAccumulator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class VarianceAccumulatorTest {

  @Test
  public void testVarianceAccumulatorsReadAllNumericTypes() {
    TSDataType[] dataTypes = {
      TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE
    };
    Column[] valueColumns = {
      new IntColumn(2, Optional.empty(), new int[] {1, 3}),
      new LongColumn(2, Optional.empty(), new long[] {1, 3}),
      new FloatColumn(2, Optional.empty(), new float[] {1, 3}),
      new DoubleColumn(2, Optional.empty(), new double[] {1, 3})
    };
    for (int i = 0; i < dataTypes.length; i++) {
      TSDataType dataType = dataTypes[i];
      Column valueColumn = valueColumns[i];

      VarianceAccumulator treeAccumulator =
          new VarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      treeAccumulator.addInput(new Column[] {valueColumn, valueColumn}, /* bitMap= */ null);
      DoubleColumnBuilder treeResult = new DoubleColumnBuilder(null, 1);
      treeAccumulator.outputFinal(treeResult);
      Assert.assertEquals(1.0, treeResult.build().getDouble(0), 0.0);

      TableVarianceAccumulator tableAccumulator =
          new TableVarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      tableAccumulator.addInput(
          new Column[] {valueColumn},
          AggregationMask.createSelectAll(valueColumn.getPositionCount()));
      DoubleColumnBuilder tableResult = new DoubleColumnBuilder(null, 1);
      tableAccumulator.evaluateFinal(tableResult);
      Assert.assertEquals(1.0, tableResult.build().getDouble(0), 0.0);

      GroupedVarianceAccumulator groupedAccumulator =
          new GroupedVarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      groupedAccumulator.setGroupCount(1);
      groupedAccumulator.addInput(
          new int[] {0, 0},
          new Column[] {valueColumn},
          AggregationMask.createSelectAll(valueColumn.getPositionCount()));
      DoubleColumnBuilder groupedResult = new DoubleColumnBuilder(null, 1);
      groupedAccumulator.evaluateFinal(0, groupedResult);
      Assert.assertEquals(1.0, groupedResult.build().getDouble(0), 0.0);
    }
  }

  @Test
  public void testVarianceAccumulatorsStillRejectTemporalTypes() {
    TSDataType[] dataTypes = {TSDataType.DATE, TSDataType.TIMESTAMP};
    Column[] valueColumns = {
      new IntColumn(2, Optional.empty(), new int[] {1, 3}, TSDataType.DATE),
      new LongColumn(2, Optional.empty(), new long[] {1, 3})
    };
    for (int i = 0; i < dataTypes.length; i++) {
      TSDataType dataType = dataTypes[i];
      Column valueColumn = valueColumns[i];

      VarianceAccumulator treeAccumulator =
          new VarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      Assert.assertThrows(
          UnSupportedDataTypeException.class,
          () ->
              treeAccumulator.addInput(
                  new Column[] {valueColumn, valueColumn}, /* bitMap= */ null));

      TableVarianceAccumulator tableAccumulator =
          new TableVarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      Assert.assertThrows(
          UnSupportedDataTypeException.class,
          () ->
              tableAccumulator.addInput(
                  new Column[] {valueColumn},
                  AggregationMask.createSelectAll(valueColumn.getPositionCount())));
      Assert.assertThrows(
          UnSupportedDataTypeException.class,
          () -> tableAccumulator.removeInput(new Column[] {valueColumn}));

      GroupedVarianceAccumulator groupedAccumulator =
          new GroupedVarianceAccumulator(dataType, VarianceAccumulator.VarianceType.VAR_POP);
      Assert.assertThrows(
          UnSupportedDataTypeException.class,
          () ->
              groupedAccumulator.addInput(
                  new int[] {0, 0},
                  new Column[] {valueColumn},
                  AggregationMask.createSelectAll(valueColumn.getPositionCount())));
    }
  }
}
