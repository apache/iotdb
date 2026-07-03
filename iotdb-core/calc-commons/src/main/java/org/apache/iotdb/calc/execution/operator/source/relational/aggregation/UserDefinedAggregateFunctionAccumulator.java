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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.rpc.TSStatusCode.EXECUTE_UDF_ERROR;

public class UserDefinedAggregateFunctionAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UserDefinedAggregateFunctionAccumulator.class);
  private final AggregateFunctionAnalysis analysis;
  private final AggregateFunction aggregateFunction;
  private final FunctionArguments functionArguments;
  private final List<Type> inputDataTypes;
  private State state;
  private final IoTDBLocal ioTDBLocal;
  private boolean init;

  public UserDefinedAggregateFunctionAccumulator(
      AggregateFunctionAnalysis analysis,
      AggregateFunction aggregateFunction,
      FunctionArguments functionArguments,
      List<Type> inputDataTypes,
      IoTDBLocal ioTDBLocal) {
    this(analysis, aggregateFunction, functionArguments, inputDataTypes, ioTDBLocal, false);
  }

  private UserDefinedAggregateFunctionAccumulator(
      AggregateFunctionAnalysis analysis,
      AggregateFunction aggregateFunction,
      FunctionArguments functionArguments,
      List<Type> inputDataTypes,
      IoTDBLocal ioTDBLocal,
      boolean init) {
    checkArgument(ioTDBLocal != null, "IoTDBLocal must not be null for UDAF");
    this.analysis = analysis;
    this.aggregateFunction = aggregateFunction;
    this.functionArguments = functionArguments;
    this.inputDataTypes = inputDataTypes;
    this.ioTDBLocal = ioTDBLocal;
    this.init = init;
    this.state = init ? aggregateFunction.createState() : null;
  }

  private void initIfNeeded() {
    if (init) {
      return;
    }
    try {
      aggregateFunction.beforeStart(functionArguments, ioTDBLocal);
      init = true;
      // create State after beforeStart
      state = aggregateFunction.createState();
    } catch (UDFException e) {
      throw new IoTDBRuntimeException(e, EXECUTE_UDF_ERROR.getStatusCode());
    }
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new UserDefinedAggregateFunctionAccumulator(
        analysis, aggregateFunction, functionArguments, inputDataTypes, ioTDBLocal, init);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    initIfNeeded();
    RecordIterator iterator =
        mask.isSelectAll()
            ? new RecordIterator(
                Arrays.asList(arguments), inputDataTypes, arguments[0].getPositionCount())
            : new MaskedRecordIterator(Arrays.asList(arguments), inputDataTypes, mask);
    while (iterator.hasNext()) {
      aggregateFunction.addInput(state, iterator.next(), ioTDBLocal);
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    initIfNeeded();
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of UDAF should be BinaryColumn");
    State otherState = aggregateFunction.createState();
    for (int i = 0; i < argument.getPositionCount(); i++) {
      otherState.reset();
      Binary otherStateBinary = argument.getBinary(i);
      otherState.deserialize(otherStateBinary.getValues());
      aggregateFunction.combineState(state, otherState, ioTDBLocal);
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    initIfNeeded();
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of UDAF should be BinaryColumn");
    byte[] bytes = state.serialize();
    columnBuilder.writeBinary(new Binary(bytes));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    initIfNeeded();
    ResultValue resultValue = new ResultValue(columnBuilder);
    aggregateFunction.outputFinal(state, resultValue, ioTDBLocal);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    // UDAF not support calculate from statistics now
    throw new UnsupportedOperationException(
        CalcMessages.UDAF_NOT_SUPPORT_CALCULATE_FROM_STATISTICS);
  }

  @Override
  public void reset() {
    if (!init) {
      return;
    }
    state.reset();
  }

  @Override
  public void removeInput(Column[] arguments) {
    if (!analysis.isRemovable()) {
      throw new UnsupportedOperationException(
          CalcMessages.THIS_ACCUMULATOR_DOES_NOT_SUPPORT_REMOVING_INPUTS);
    }
    RecordIterator iterator =
        new RecordIterator(
            Arrays.asList(arguments), inputDataTypes, arguments[0].getPositionCount());
    while (iterator.hasNext()) {
      aggregateFunction.remove(state, iterator.next());
    }
  }

  @Override
  public boolean removable() {
    return analysis.isRemovable();
  }

  @Override
  public void close() {
    // ensure beforeStart was called
    initIfNeeded();
    aggregateFunction.beforeDestroy(ioTDBLocal);
    ioTDBLocal.close();
    state.destroyState();
  }
}
