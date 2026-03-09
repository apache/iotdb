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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.MaskedRecordIterator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.RecordIterator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.ObjectBigArray;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupedUserDefinedAggregateAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedUserDefinedAggregateAccumulator.class);
  private final AggregateFunction aggregateFunction;
  private final ObjectBigArray<State> stateArray;
  private final List<Type> inputDataTypes;

  public GroupedUserDefinedAggregateAccumulator(
      AggregateFunction aggregateFunction, List<Type> inputDataTypes) {
    this.aggregateFunction = aggregateFunction;
    this.stateArray = new ObjectBigArray<>();
    this.inputDataTypes = inputDataTypes;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setGroupCount(long groupCount) {
    stateArray.ensureCapacity(groupCount);
  }

  private State getOrCreateState(int groupId) {
    State state = stateArray.get(groupId);
    if (state == null) {
      state = aggregateFunction.createState();
      stateArray.set(groupId, state);
    }
    return state;
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    RecordIterator iterator =
        mask.isSelectAll()
            ? new RecordIterator(
                Arrays.asList(arguments), inputDataTypes, arguments[0].getPositionCount())
            : new MaskedRecordIterator(Arrays.asList(arguments), inputDataTypes, mask);

    int index = 0;
    if (mask.isSelectAll()) {
      while (iterator.hasNext()) {
        int groupId = groupIds[index];
        index++;
        State state = getOrCreateState(groupId);
        aggregateFunction.addInput(state, iterator.next());
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      while (iterator.hasNext()) {
        int groupId = groupIds[selectedPositions[index]];
        index++;
        State state = getOrCreateState(groupId);
        aggregateFunction.addInput(state, iterator.next());
      }
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of UDAF should be BinaryColumn");

    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        State otherState = aggregateFunction.createState();
        Binary otherStateBinary = argument.getBinary(i);
        otherState.deserialize(otherStateBinary.getValues());
        aggregateFunction.combineState(getOrCreateState(groupIds[i]), otherState);
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of UDAF should be BinaryColumn");
    if (stateArray.get(groupId) == null) {
      throw new IllegalStateException(String.format("State for group %d is not found", groupId));
    }
    byte[] bytes = stateArray.get(groupId).serialize();
    columnBuilder.writeBinary(new Binary(bytes));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    ResultValue resultValue = new ResultValue(columnBuilder);
    aggregateFunction.outputFinal(getOrCreateState(groupId), resultValue);
  }

  @Override
  public void prepareFinal() {
    // do nothing
  }

  @Override
  public void reset() {
    stateArray.reset();
  }

  @Override
  public void close() {
    aggregateFunction.beforeDestroy();
    stateArray.forEach(
        state -> {
          if (state != null) {
            state.destroyState();
          }
        });
  }
}
