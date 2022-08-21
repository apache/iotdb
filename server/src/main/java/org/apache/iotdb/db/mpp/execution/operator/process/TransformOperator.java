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

import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.Codegen;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.CodegenContext;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.CodegenImpl;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.builder.EvaluationDAGBuilder;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.input.TsBlockInputDataSet;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TransformOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformOperator.class);

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
  protected LayerPointReader[] transformers;
  protected List<TSDataType> outputDataTypes;

  protected TimeSelector timeHeap;
  protected boolean[] shouldIterateReadersToNextValid;
  protected TimeSelector transformerTimeHeap;
  protected List<Boolean> codeGeneratedSuccess;
  protected LayerPointReader timeReader;
  protected LayerPointReader[] dataReaders;
  protected boolean codegenExpressionsHaveCache;
  protected Object[] cacheValues;
  protected Codegen generatedEvaluate;

  public TransformOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      boolean keepNull,
      ZoneId zoneId,
      TypeProvider typeProvider,
      boolean isAscending)
      throws QueryProcessException, IOException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.keepNull = keepNull;

    initInputLayer(inputDataTypes);
    initUdtfContext(outputExpressions, zoneId);
    initTransformers(inputLocations, outputExpressions, typeProvider);
    initGeneratedEvaluate(inputLocations, outputExpressions, typeProvider, inputDataTypes);
    timeHeap = new TimeSelector(transformers.length << 1, isAscending);
    transformerTimeHeap = new TimeSelector(transformers.length << 1, isAscending);
    shouldIterateReadersToNextValid = new boolean[outputExpressions.length];
    Arrays.fill(shouldIterateReadersToNextValid, true);
  }

  private void initGeneratedEvaluate(
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      TypeProvider typeProvider,
      List<TSDataType> inputDataTypes) {
    generatedEvaluate = new CodegenImpl(typeProvider, udtfContext, new CodegenContext());
    List<String> paths = new ArrayList<>(inputLocations.keySet());

    generatedEvaluate.setInputs(paths, inputDataTypes);

    for (Expression expression : outputExpressions) {
      generatedEvaluate.addExpression(expression);
    }
    codeGeneratedSuccess = generatedEvaluate.isGenerated();
    try {
      generatedEvaluate.generateScriptEvaluator();
    } catch (Exception exception) {
      // code generate fail, all marked as false
      codeGeneratedSuccess = Collections.nCopies(codeGeneratedSuccess.size(), false);
    }
  }

  private void initInputLayer(List<TSDataType> inputDataTypes) throws QueryProcessException {
    inputLayer =
        new QueryDataSetInputLayer(
            operatorContext.getOperatorId(),
            udfReaderMemoryBudgetInMB,
            new TsBlockInputDataSet(inputOperator, inputDataTypes));

    timeReader = inputLayer.constructTimePointReader();
    dataReaders = new LayerPointReader[inputDataTypes.size()];
    for (int i = 0; i < inputDataTypes.size(); ++i) {
      dataReaders[i] = inputLayer.constructValuePointReader(i);
    }
  }

  private void initUdtfContext(Expression[] outputExpressions, ZoneId zoneId) {
    udtfContext = new UDTFContext(zoneId);
    udtfContext.constructUdfExecutors(outputExpressions);
  }

  protected void initTransformers(
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      TypeProvider typeProvider)
      throws QueryProcessException, IOException {
    UDFRegistrationService.getInstance().acquireRegistrationLock();
    try {
      // This statement must be surrounded by the registration lock.
      UDFClassLoaderManager.getInstance().initializeUDFQuery(operatorContext.getOperatorId());
      // UDF executors will be initialized at the same time
      transformers =
          new EvaluationDAGBuilder(
                  operatorContext.getOperatorId(),
                  inputLayer,
                  inputLocations,
                  outputExpressions,
                  typeProvider,
                  udtfContext,
                  udfTransformerMemoryBudgetInMB + udfCollectorMemoryBudgetInMB)
              .buildLayerMemoryAssigner()
              .bindInputLayerColumnIndexWithExpression()
              .buildResultColumnPointReaders()
              .getOutputPointReaders();
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  protected Object[] collectRow() throws IOException {
    Object[] row = new Object[dataReaders.length];
    for (int i = 0; i < dataReaders.length; ++i) {
      Object value;
      if (dataReaders[i].isCurrentNull()) {
        value = null;
      } else {
        switch (dataReaders[i].getDataType()) {
          case INT32:
            value = dataReaders[i].currentInt();
            break;
          case INT64:
            value = dataReaders[i].currentLong();
            break;
          case FLOAT:
            value = dataReaders[i].currentFloat();
            break;
          case DOUBLE:
            value = dataReaders[i].currentDouble();
            break;
          case BOOLEAN:
            value = dataReaders[i].currentBoolean();
            break;
            //          case TEXT:
            //            value = dataReaders[i].currentBinary();
            //            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported while codegen.", dataReaders[i].getDataType()));
        }
      }
      row[i] = value;
    }
    return row;
  }

  protected YieldableState iterateAllColumnsToNextValid()
      throws QueryProcessException, IOException, InvocationTargetException {
    YieldableState yieldableState = iterateTransformerNextValid();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }
    return iterateCodeGenExpressionsToNextValid();
  }

  protected YieldableState iterateTransformerNextValid() throws QueryProcessException, IOException {
    for (int i = 0, n = shouldIterateReadersToNextValid.length; i < n; ++i) {
      if (shouldIterateReadersToNextValid[i] && !codeGeneratedSuccess.get(i)) {
        final YieldableState yieldableState = iterateReaderToNextValid(transformers[i]);
        if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
          return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
        }
        shouldIterateReadersToNextValid[i] = false;
      }
    }
    return YieldableState.YIELDABLE;
  }

  protected YieldableState iterateCodeGenExpressionsToNextValid()
      throws QueryProcessException, IOException, InvocationTargetException {
    if (!codegenExpressionsHaveCache) {
      YieldableState yieldableState = timeReader.yield();
      if (yieldableState != YieldableState.YIELDABLE) {
        return yieldableState;
      } else {
        for (LayerPointReader dataReader : dataReaders) {
          if (dataReader.yield() == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
            return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
          }
        }
        long currentTime = timeReader.currentLong();
        timeHeap.add(currentTime);
        cacheValues = generatedEvaluate.accept(collectRow(), currentTime);
      }
      codegenExpressionsHaveCache = true;
    }
    return YieldableState.YIELDABLE;
  }

  protected YieldableState iterateReaderToNextValid(LayerPointReader reader)
      throws QueryProcessException, IOException {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
    // If keepNull is false, we must iterate the reader until a non-null row is returned.
    YieldableState yieldableState;
    while ((yieldableState = reader.yield()) == YieldableState.YIELDABLE) {
      if (reader.isCurrentNull() && !keepNull) {
        reader.readyForNext();
        continue;
      }
      transformerTimeHeap.add(reader.currentTime());
      timeHeap.add(reader.currentTime());
      break;
    }
    return yieldableState;
  }

  @Override
  public final boolean hasNext() {
    if (!timeHeap.isEmpty()) {
      return true;
    }
    try {
      if (iterateAllColumnsToNextValid() == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("TransformOperator#hasNext()", e);
      throw new RuntimeException(e);
    }
    return !timeHeap.isEmpty();
  }

  @Override
  public TsBlock next() {

    try {
      YieldableState yieldableState = iterateAllColumnsToNextValid();
      if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return null;
      }

      final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
      if (outputDataTypes == null) {
        outputDataTypes = new ArrayList<>();
        for (LayerPointReader reader : transformers) {
          outputDataTypes.add(reader.getDataType());
        }
      }
      tsBlockBuilder.buildValueColumnBuilders(outputDataTypes);
      final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
      final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
      final int columnCount = columnBuilders.length;

      int rowCount = 0;
      while (!timeHeap.isEmpty()) {

        final long currentTime = timeHeap.pollFirst();

        // infer are there anything need to be written
        boolean somethingNeedToWrite;
        boolean causedByCodegen;
        boolean causedByTransformer;

        if (transformerTimeHeap.isEmpty()) {
          causedByTransformer = false;
        } else {
          long minTransformerTime = transformerTimeHeap.first();
          if (currentTime == minTransformerTime) {
            transformerTimeHeap.pollFirst();
            causedByTransformer = true;
          } else {
            causedByTransformer = false;
          }
        }

        causedByCodegen = !isCodegenExpressionsCacheAllNull();
        somethingNeedToWrite = causedByCodegen || causedByTransformer;

        if (somethingNeedToWrite) {
          // time
          timeBuilder.writeLong(currentTime);

          // values
          for (int i = 0; i < columnCount; ++i) {
            if (codeGeneratedSuccess.get(i)) {
              collectGeneratedData(columnBuilders[i], i, outputDataTypes.get(i));
            } else {
              yieldableState = collectDataPoint(transformers[i], columnBuilders[i], currentTime, i);
              if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
                for (int j = 0; j <= i; ++j) {
                  shouldIterateReadersToNextValid[j] = false;
                }
                timeHeap.add(currentTime);
                if (causedByTransformer) {
                  transformerTimeHeap.add(currentTime);
                }

                tsBlockBuilder.declarePositions(rowCount);
                return tsBlockBuilder.build();
              }
            }
          }
          ++rowCount;
        }

        codegenExpressionReadyForNext();

        for (int i = 0; i < columnCount; ++i) {
          if (shouldIterateReadersToNextValid[i] && !codeGeneratedSuccess.get(i)) {
            transformers[i].readyForNext();
          }
        }

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
      LOGGER.error("TransformOperator#next()", e);
      throw new RuntimeException(e);
    }
  }

  private boolean isCodegenExpressionsCacheAllNull() {
    if (Objects.isNull(cacheValues)) {
      return true;
    }
    for (Object o : cacheValues) {
      if (!Objects.isNull(o)) {
        return false;
      }
    }
    return true;
  }

  protected void codegenExpressionReadyForNext() {
    codegenExpressionsHaveCache = false;
    cacheValues = null;
    timeReader.readyForNext();
    for (LayerPointReader layerPointReader : dataReaders) {
      layerPointReader.readyForNext();
    }
  }

  protected boolean collectGeneratedData(
      ColumnBuilder writer, int dataIndex, TSDataType tsDataType) {
    if (Objects.isNull(cacheValues[dataIndex])) {
      writer.appendNull();
      return false;
    }
    switch (tsDataType) {
      case INT32:
        writer.writeInt((int) cacheValues[dataIndex]);
        return true;
      case INT64:
        writer.writeLong((long) cacheValues[dataIndex]);
        return true;
      case FLOAT:
        writer.writeFloat((float) cacheValues[dataIndex]);
        return true;
      case DOUBLE:
        writer.writeDouble((double) cacheValues[dataIndex]);
        return true;
      case BOOLEAN:
        writer.writeBoolean((boolean) cacheValues[dataIndex]);
        return true;
      case TEXT:
        writer.writeBinary((Binary) cacheValues[dataIndex]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", tsDataType));
    }
    return false;
  }

  protected boolean collectReaderAppendIsNull(LayerPointReader reader, long currentTime)
      throws QueryProcessException, IOException {
    final YieldableState yieldableState = reader.yield();

    if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
      return true;
    }

    if (yieldableState != YieldableState.YIELDABLE) {
      return false;
    }

    if (reader.currentTime() != currentTime) {
      return true;
    }

    if (reader.isCurrentNull()) {
      return true;
    } else {
      return false;
    }
  }

  protected YieldableState collectDataPoint(
      LayerPointReader reader, ColumnBuilder writer, long currentTime, int readerIndex)
      throws QueryProcessException, IOException {
    final YieldableState yieldableState = reader.yield();
    if (yieldableState == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
      writer.appendNull();
      return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
    }
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }

    if (reader.currentTime() != currentTime) {
      writer.appendNull();
      return YieldableState.YIELDABLE;
    }

    if (reader.isCurrentNull()) {
      writer.appendNull();
    } else {
      TSDataType type = reader.getDataType();
      switch (type) {
        case INT32:
          writer.writeInt(reader.currentInt());
          break;
        case INT64:
          writer.writeLong(reader.currentLong());
          break;
        case FLOAT:
          writer.writeFloat(reader.currentFloat());
          break;
        case DOUBLE:
          writer.writeDouble(reader.currentDouble());
          break;
        case BOOLEAN:
          writer.writeBoolean(reader.currentBoolean());
          break;
        case TEXT:
          writer.writeBinary(reader.currentBinary());
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", type));
      }
    }

    shouldIterateReadersToNextValid[readerIndex] = true;

    return YieldableState.YIELDABLE;
  }

  @Override
  public void close() throws Exception {
    udtfContext.finalizeUDFExecutors(operatorContext.getOperatorId());
    inputOperator.close();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public boolean isFinished() {
    // call hasNext first, or data of inputOperator could be missing
    boolean flag = !hasNext();
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
  public long calculateRetainedSizeAfterCallingNext() {
    // Collector may cache points, here we use maximum usage
    return (long)
        (inputOperator.calculateRetainedSizeAfterCallingNext() + udfCollectorMemoryBudgetInMB);
  }
}
