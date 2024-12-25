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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.NodeRef;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.builder.EvaluationDAGBuilder;
import org.apache.iotdb.db.queryengine.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.input.TsBlockInputDataSet;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TransformOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TransformOperator.class);

  protected final float udfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  protected final float udfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  protected final float udfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  protected final OperatorContext operatorContext;
  protected final Operator inputOperator;
  protected final boolean keepNull;

  protected QueryDataSetInputLayer inputLayer;
  protected UDTFContext udtfContext;
  protected LayerReader[] transformers;
  protected List<TSDataType> outputDataTypes;

  protected TimeSelector timeHeap;
  protected TsBlock[] outputColumns;
  protected int[] currentIndexes;
  protected boolean[] shouldIterateReadersToNextValid;

  private final String udtfQueryId;

  @SuppressWarnings("squid:S107")
  public TransformOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      boolean keepNull,
      ZoneId zoneId,
      Map<NodeRef<Expression>, TSDataType> expressionTypes,
      boolean isAscending)
      throws QueryProcessException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.keepNull = keepNull;
    // use DriverTaskID().getFullId() to ensure that udtfQueryId for each TransformOperator is
    // unique
    this.udtfQueryId = operatorContext.getDriverContext().getDriverTaskID().getFullId();

    initInputLayer(inputDataTypes);
    initUdtfContext(outputExpressions, zoneId);
    initTransformers(inputLocations, outputExpressions, expressionTypes);

    outputColumns = new TsBlock[transformers.length];
    currentIndexes = new int[transformers.length];

    timeHeap = new TimeSelector(transformers.length << 1, isAscending);
    shouldIterateReadersToNextValid = new boolean[outputExpressions.length];
    Arrays.fill(shouldIterateReadersToNextValid, true);
  }

  private void initInputLayer(List<TSDataType> inputDataTypes) throws QueryProcessException {
    inputLayer =
        new QueryDataSetInputLayer(
            udtfQueryId,
            udfReaderMemoryBudgetInMB,
            new TsBlockInputDataSet(inputOperator, inputDataTypes));
  }

  private void initUdtfContext(Expression[] outputExpressions, ZoneId zoneId) {
    udtfContext = new UDTFContext(zoneId);
    udtfContext.constructUdfExecutors(outputExpressions);
  }

  protected void initTransformers(
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    UDFManagementService.getInstance().acquireLock();
    try {
      // This statement must be surrounded by the registration lock.
      UDFClassLoaderManager.getInstance().initializeUDFQuery(udtfQueryId);
      // UDF executors will be initialized at the same time
      transformers =
          new EvaluationDAGBuilder(
                  udtfQueryId,
                  inputLayer,
                  inputLocations,
                  outputExpressions,
                  expressionTypes,
                  udtfContext,
                  udfTransformerMemoryBudgetInMB + udfCollectorMemoryBudgetInMB)
              .buildLayerMemoryAssigner()
              .bindInputLayerColumnIndexWithExpression()
              .buildResultColumnPointReaders()
              .getOutputReaders();
    } finally {
      UDFManagementService.getInstance().releaseLock();
    }
  }

  protected YieldableState iterateAllColumnsToNextValid() throws Exception {
    for (int i = 0, n = shouldIterateReadersToNextValid.length; i < n; ++i) {
      if (shouldIterateReadersToNextValid[i]) {
        final YieldableState yieldableState = iterateReaderToNextValid(i);
        if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
          return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
        }
        shouldIterateReadersToNextValid[i] = false;
      }
    }
    return YieldableState.YIELDABLE;
  }

  @SuppressWarnings("squid:S135")
  protected YieldableState iterateReaderToNextValid(int index) throws Exception {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
    // If keepNull is false, we must iterate the reader until a non-null row is returned.
    while (true) {
      if (outputColumns[index] == null) {
        YieldableState state = transformers[index].yield();
        if (state != YieldableState.YIELDABLE) {
          return state;
        }

        Column[] columns = transformers[index].current();
        TsBlock block = new TsBlock(columns[1], columns[0]);
        outputColumns[index] = block;
        currentIndexes[index] = 0;
      }

      Column outputValueColumn = outputColumns[index].getColumn(0);
      while (outputValueColumn.isNull(currentIndexes[index]) && !keepNull) {
        currentIndexes[index]++;
        if (currentIndexes[index] == outputValueColumn.getPositionCount()) {
          transformers[index].consumedAll();
          outputColumns[index] = null;
          currentIndexes[index] = 0;
          break;
        }
      }

      if (outputColumns[index] != null) {
        Column outputTimeColumn = outputColumns[index].getTimeColumn();
        long time = outputTimeColumn.getLong(currentIndexes[index]);
        timeHeap.add(time);
        return YieldableState.YIELDABLE;
      }
    }
  }

  @SuppressWarnings("squid:S112")
  @Override
  public final boolean hasNext() throws Exception {
    if (!timeHeap.isEmpty()) {
      return true;
    }
    try {
      if (iterateAllColumnsToNextValid() == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return true;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return !timeHeap.isEmpty();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public TsBlock next() throws Exception {

    try {
      YieldableState yieldableState = iterateAllColumnsToNextValid();
      if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return null;
      }

      final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
      prepareTsBlockBuilder(tsBlockBuilder);
      final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
      final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
      final int columnCount = columnBuilders.length;

      int rowCount = 0;
      while (!timeHeap.isEmpty()) {
        final long currentTime = timeHeap.pollFirst();

        // time
        timeBuilder.writeLong(currentTime);

        // values
        for (int i = 0; i < columnCount; ++i) {
          yieldableState = collectDataPoint(columnBuilders[i], currentTime, i);
          if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            for (int j = 0; j <= i; ++j) {
              shouldIterateReadersToNextValid[j] = false;
            }
            timeHeap.add(currentTime);

            tsBlockBuilder.declarePositions(rowCount);
            return tsBlockBuilder.build();
          }
        }

        prepareEachColumn(columnCount);

        ++rowCount;

        yieldableState = iterateAllColumnsToNextValid();
        if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
          tsBlockBuilder.declarePositions(rowCount);
          return tsBlockBuilder.build();
        }

        inputLayer.updateRowRecordListEvictionUpperBound();
      }

      tsBlockBuilder.declarePositions(rowCount);
      return tsBlockBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void prepareTsBlockBuilder(TsBlockBuilder tsBlockBuilder) {
    if (outputDataTypes == null) {
      outputDataTypes = new ArrayList<>();
      for (LayerReader reader : transformers) {
        outputDataTypes.add(reader.getDataTypes()[0]);
      }
    }
    tsBlockBuilder.buildValueColumnBuilders(outputDataTypes);
  }

  private void prepareEachColumn(int columnCount) {
    for (int i = 0; i < columnCount; ++i) {
      if (shouldIterateReadersToNextValid[i]) {
        currentIndexes[i]++;
        if (currentIndexes[i] == outputColumns[i].getColumn(0).getPositionCount()) {
          transformers[i].consumedAll();
          outputColumns[i] = null;
          currentIndexes[i] = 0;
        }
      }
    }
  }

  protected YieldableState collectDataPoint(ColumnBuilder writer, long currentTime, int index)
      throws Exception {
    YieldableState state = transformers[index].yield();
    if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
      writer.appendNull();
      return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
    }
    if (state != YieldableState.YIELDABLE) {
      return state;
    }

    Column timeColumn = outputColumns[index].getTimeColumn();
    Column valueColumn = outputColumns[index].getColumn(0);
    int currentIndex = currentIndexes[index];
    if (timeColumn.getLong(currentIndex) != currentTime) {
      writer.appendNull();
      return YieldableState.YIELDABLE;
    }

    if (valueColumn.isNull(currentIndex)) {
      writer.appendNull();
    } else {
      TSDataType type = transformers[index].getDataTypes()[0];
      switch (type) {
        case INT32:
        case DATE:
          writer.writeInt(valueColumn.getInt(currentIndex));
          break;
        case INT64:
        case TIMESTAMP:
          writer.writeLong(valueColumn.getLong(currentIndex));
          break;
        case FLOAT:
          writer.writeFloat(valueColumn.getFloat(currentIndex));
          break;
        case DOUBLE:
          writer.writeDouble(valueColumn.getDouble(currentIndex));
          break;
        case BOOLEAN:
          writer.writeBoolean(valueColumn.getBoolean(currentIndex));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          writer.writeBinary(valueColumn.getBinary(currentIndex));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", type));
      }
    }

    shouldIterateReadersToNextValid[index] = true;

    return YieldableState.YIELDABLE;
  }

  @Override
  public void close() throws Exception {
    udtfContext.finalizeUDFExecutors(udtfQueryId);
    inputOperator.close();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    // call hasNext first, or data of inputOperator could be missing
    boolean flag = !hasNextWithTimer();
    return timeHeap.isEmpty() && (flag || inputOperator.isFinished());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // here we use maximum estimated memory usage
    return (long)
        (udfCollectorMemoryBudgetInMB
            + udfTransformerMemoryBudgetInMB
            + inputOperator.calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    return (long) (1 + transformers.length)
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(shouldIterateReadersToNextValid)
        + RamUsageEstimator.sizeOf(udtfQueryId);
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    // Collector may cache points, here we use maximum usage
    return (long)
        (inputOperator.calculateRetainedSizeAfterCallingNext() + udfCollectorMemoryBudgetInMB);
  }

  @TestOnly
  public LayerReader[] getTransformers() {
    return transformers;
  }
}
