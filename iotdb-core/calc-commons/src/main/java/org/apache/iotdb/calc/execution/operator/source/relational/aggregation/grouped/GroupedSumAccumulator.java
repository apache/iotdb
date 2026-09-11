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
package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupedSumAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedSumAccumulator.class);
  private final TSDataType argumentDataType;
  private final TypeServices.ColumnToDoubleConverter valueConverter;
  private final BooleanBigArray initResult = new BooleanBigArray();
  private final DoubleBigArray sumValues = new DoubleBigArray();

  public GroupedSumAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
    Type type = Type.fromTsDataType(argumentDataType);
    this.valueConverter =
        TypeServices.AGGREGATION_NUMERIC_COLUMN_TO_DOUBLE_CONVERTER_SERVICE
            .call(type)
            .create(
                () ->
                    new UnSupportedDataTypeException(
                        String.format(
                            CalcMessages.UNSUPPORTED_DATA_TYPE_IN_SUM_AGGREGATION,
                            argumentDataType)));
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + initResult.sizeOf() + sumValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    initResult.ensureCapacity(groupCount);
    sumValues.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 1, "argument of SUM should be one column");
    addInput(groupIds, arguments[0], mask);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {

    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], argument.getDouble(i));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    if (!initResult.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId));
    }
  }

  private void addInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          initResult.set(groupIds[i], true);
          sumValues.add(groupIds[i], valueConverter.convert(column, i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          initResult.set(groupIds[position], true);
          sumValues.add(groupIds[position], valueConverter.convert(column, position));
        }
      }
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!initResult.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    initResult.reset();
    sumValues.reset();
  }
}
