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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.doubleToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

public class GroupedAvgAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedAvgAccumulator.class);
  private final TSDataType argumentDataType;
  private final TypeServices.GroupedAvgInput inputStrategy;
  private final LongBigArray countValues = new LongBigArray();
  private final DoubleBigArray sumValues = new DoubleBigArray();

  public GroupedAvgAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
    Type type = Type.fromTsDataType(argumentDataType);
    this.inputStrategy = TypeServices.GROUPED_AVG_INPUT_SERVICE.call(type);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + countValues.sizeOf() + sumValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    countValues.ensureCapacity(groupCount);
    sumValues.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    checkArgument(
        arguments.length == 1,
        CalcMessages.EXCEPTION_ARGUMENT_OF_AVG_SHOULD_BE_ONE_COLUMN_82162B82);
    inputStrategy.addInput(groupIds, arguments[0], mask, sumValues, countValues);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB);

    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        deserialize(groupIds[i], argument.getBinary(i).getValues());
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_AVG_SHOULD_BE_BINARYCOLUMN_9CCDD5EB);
    if (countValues.get(groupId) == 0) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeState(groupId)));
    }
  }

  private void deserialize(int groupId, byte[] bytes) {
    countValues.add(groupId, BytesUtils.bytesToLong(bytes, Long.BYTES));
    sumValues.add(groupId, BytesUtils.bytesToDouble(bytes, Long.BYTES));
  }

  private byte[] serializeState(int groupId) {
    byte[] bytes = new byte[Long.BYTES + Double.BYTES];
    longToBytes(countValues.get(groupId), bytes, 0);
    doubleToBytes(sumValues.get(groupId), bytes, Long.BYTES);
    return bytes;
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    long countValue = countValues.get(groupId);
    if (countValue == 0) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId) / countValue);
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    countValues.reset();
    sumValues.reset();
  }
}
