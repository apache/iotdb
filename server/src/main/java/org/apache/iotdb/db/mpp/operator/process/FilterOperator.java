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

package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.dataset.IUDFInputDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.layer.EvaluationDAGBuilder;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.TsBlockInputDataSet;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterOperator implements ProcessOperator {

  private static final int FETCH_SIZE = 10000;
  private final float udfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  private final float udfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  private final float udfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  private final boolean keepNull;
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final List<TSDataType> inputDataTypes;
  private final Expression filterExpression;
  private final Expression[] outputExpressions;
  private final UDTFContext udtfContext;
  private LayerPointReader[] transformers;

  private IUDFInputDataSet inputDataset;
  private LayerPointReader filter;
  private TimeSelector timeHeap;

  public FilterOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Expression filterExpression,
      Expression[] outputExpressions,
      UDTFContext udtfContext,
      boolean keepNull)
      throws QueryProcessException, IOException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = inputDataTypes;
    this.filterExpression = filterExpression;
    this.udtfContext = udtfContext;
    this.outputExpressions = outputExpressions;
    this.keepNull = keepNull;

    initInputDataset(this.inputDataTypes);
    initExpressions();
    initTimeHeap();
  }

  private void initExpressions() throws QueryProcessException, IOException {
    UDFRegistrationService.getInstance().acquireRegistrationLock();
    // This statement must be surrounded by the registration lock.
    UDFClassLoaderManager.getInstance().initializeUDFQuery(operatorContext.getOperatorId());
    try {
      // UDF executors will be initialized at the same time
      Expression[] expressions;
      if (filter != null) {
        expressions = new Expression[outputExpressions.length + 1];
        expressions[0] = filterExpression;
        for (int i = 0; i < outputExpressions.length; ++i) {
          expressions[1 + i] = outputExpressions[i];
        }
      } else {
        expressions = outputExpressions;
      }

      for (int i = 0; i < outputExpressions.length; ++i) {
        expressions[1 + i] = outputExpressions[i];
      }
      transformers =
          new EvaluationDAGBuilder(
                  operatorContext.getOperatorId(),
                  new RawQueryInputLayer(
                      operatorContext.getOperatorId(), udfReaderMemoryBudgetInMB, inputDataset),
                  expressions,
                  udtfContext,
                  udfTransformerMemoryBudgetInMB + udfCollectorMemoryBudgetInMB)
              .buildLayerMemoryAssigner()
              .buildResultColumnPointReaders()
              .getOutputPointReaders();
      filter = filterExpression == null ? null : transformers[0];
      if (filter != null && filter.getDataType() != TSDataType.BOOLEAN) {
        throw new UnSupportedDataTypeException(
            String.format(
                "Data type of filter should be BOOLEAN, but %s is received.",
                filter.getDataType()));
      }
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  private void initInputDataset(List<TSDataType> inputDataTypes) {
    inputDataset = new TsBlockInputDataSet(inputOperator, inputDataTypes);
  }

  private void initTimeHeap() throws QueryProcessException, IOException {
    timeHeap = new TimeSelector(transformers.length + 1 << 1, true);
    if (filter != null) {
      iterateReaderToNextValid(filter);
    }
    for (LayerPointReader reader : transformers) {
      iterateReaderToNextValid(reader);
    }
  }

  private void iterateReaderToNextValid(LayerPointReader reader)
      throws QueryProcessException, IOException {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
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
  public TsBlock next() {
    final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();
    List<TSDataType> outputDataType = new ArrayList<>();
    // if filter is not null, transformers[0] is filter,
    // need not output
    int offset = filter == null ? 0 : 1;
    for (int i = offset; i < transformers.length; ++i) {
      outputDataType.add(transformers[i].getDataType());
    }
    tsBlockBuilder.buildValueColumnBuilders(outputDataType);

    final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    final int columnCount = columnBuilders.length;

    int rowCount = 0;
    try {
      while (rowCount < FETCH_SIZE && !timeHeap.isEmpty()) {

        long minTime = filter == null ? timeHeap.pollFirst() : filter.currentTime();

        if (filter == null || filter.currentBoolean()) {
          timeBuilder.writeLong(minTime);

          for (int i = 0; i < columnCount; ++i) {
            LayerPointReader reader = transformers[i + 1];

            if (!reader.next() || reader.currentTime() != minTime || reader.isCurrentNull()) {
              columnBuilders[i].appendNull();
              continue;
            }

            TSDataType type = reader.getDataType();
            switch (type) {
              case INT32:
                columnBuilders[i].writeInt(reader.currentInt());
                break;
              case INT64:
                columnBuilders[i].writeLong(reader.currentLong());
                break;
              case FLOAT:
                columnBuilders[i].writeFloat(reader.currentFloat());
                break;
              case DOUBLE:
                columnBuilders[i].writeDouble(reader.currentDouble());
                break;
              case BOOLEAN:
                columnBuilders[i].writeBoolean(reader.currentBoolean());
                break;
              case TEXT:
                columnBuilders[i].writeBinary(reader.currentBinary());
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }

            reader.readyForNext();

            iterateReaderToNextValid(reader);
          }

          ++rowCount;
        }

        if (filter != null) {
          filter.readyForNext();
          iterateReaderToNextValid(filter);
        }
      }
    } catch (Exception e) {
      // TODO: throw here?
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public boolean hasNext() {
    return !timeHeap.isEmpty();
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
