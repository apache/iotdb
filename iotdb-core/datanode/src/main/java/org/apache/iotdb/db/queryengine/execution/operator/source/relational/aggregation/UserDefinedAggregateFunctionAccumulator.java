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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;

public class UserDefinedAggregateFunctionAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UserDefinedAggregateFunctionAccumulator.class);
  private final AggregateFunction aggregateFunction;
  private final State state;

  public UserDefinedAggregateFunctionAccumulator(AggregateFunction aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
    this.state = aggregateFunction.createState();
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new UserDefinedAggregateFunctionAccumulator(aggregateFunction);
  }

  @Override
  public void addInput(Column[] arguments) {
    aggregateFunction.addInput(state, arguments);
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of UDAF should be BinaryColumn");
    State otherState = aggregateFunction.createState();
    Binary otherStateBinary = argument.getBinary(0);
    otherState.deserialize(otherStateBinary.getValues());

    aggregateFunction.combineState(state, otherState);
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of UDAF should be BinaryColumn");
    byte[] bytes = state.serialize();
    columnBuilder.writeBinary(new Binary(bytes));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    ResultValue resultValue = new ResultValue(columnBuilder);
    aggregateFunction.outputFinal(state, resultValue);
  }

  @Override
  public boolean hasFinalResult() {
    // TODO
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    // TODO
  }

  @Override
  public void reset() {
    state.reset();
  }
}
