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
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.builder.EvaluationDAGBuilder;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.input.TsBlockInputDataSet;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransformOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformOperator.class);

  // TODO: make it configurable
  protected static final int FETCH_SIZE = 10000;

  protected final float udfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  protected final float udfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  protected final float udfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  protected final OperatorContext operatorContext;
  protected final Operator inputOperator;
  protected final boolean keepNull;

  protected boolean isFirstIteration;

  protected QueryDataSetInputLayer inputLayer;
  protected UDTFContext udtfContext;
  protected LayerPointReader[] transformers;
  protected TimeSelector timeHeap;
  protected List<TSDataType> outputDataTypes;

  public TransformOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      boolean keepNull,
      ZoneId zoneId,
      TypeProvider typeProvider)
      throws QueryProcessException, IOException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.keepNull = keepNull;

    isFirstIteration = true;

    initInputLayer(inputDataTypes);
    initUdtfContext(outputExpressions, zoneId);
    initTransformers(inputLocations, outputExpressions, typeProvider);
  }

  private void initInputLayer(List<TSDataType> inputDataTypes) throws QueryProcessException {
    inputLayer =
        new QueryDataSetInputLayer(
            operatorContext.getOperatorId(),
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

  protected void readyForFirstIteration() throws QueryProcessException, IOException {
    timeHeap = new TimeSelector(transformers.length << 1, true);
    for (LayerPointReader reader : transformers) {
      iterateReaderToNextValid(reader);
    }
  }

  protected void iterateReaderToNextValid(LayerPointReader reader)
      throws QueryProcessException, IOException {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
    // If keepNull is false, we must iterate the reader until a non-null row is returned.
    while (reader.next()) {
      if (reader.isCurrentNull() && !keepNull) {
        reader.readyForNext();
        continue;
      }
      timeHeap.add(reader.currentTime());
      break;
    }
  }

  @Override
  public final boolean hasNext() {
    if (isFirstIteration) {
      try {
        readyForFirstIteration();
      } catch (Exception e) {
        LOGGER.error("TransformOperator#hasNext()", e);
        throw new RuntimeException(e);
      }
      isFirstIteration = false;
    }

    return !timeHeap.isEmpty();
  }

  @Override
  public TsBlock next() {
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

    try {
      int rowCount = 0;
      while (rowCount < FETCH_SIZE && !timeHeap.isEmpty()) {
        final long currentTime = timeHeap.pollFirst();

        // time
        timeBuilder.writeLong(currentTime);

        // values
        for (int i = 0; i < columnCount; ++i) {
          collectDataPointAndIterateToNextValid(transformers[i], columnBuilders[i], currentTime);
        }

        ++rowCount;

        inputLayer.updateRowRecordListEvictionUpperBound();
      }

      tsBlockBuilder.declarePositions(rowCount);
    } catch (Exception e) {
      LOGGER.error("TransformOperator#next()", e);
      throw new RuntimeException(e);
    }

    return tsBlockBuilder.build();
  }

  protected void collectDataPointAndIterateToNextValid(
      LayerPointReader reader, ColumnBuilder writer, long currentTime)
      throws QueryProcessException, IOException {
    if (!reader.next() || reader.currentTime() != currentTime) {
      writer.appendNull();
      return;
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

    reader.readyForNext();
    iterateReaderToNextValid(reader);
  }

  @Override
  public void close() throws Exception {
    udtfContext.finalizeUDFExecutors(operatorContext.getOperatorId());

    inputOperator.close();
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
