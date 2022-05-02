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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterOperator extends TransformOperator {

  private LayerPointReader filterPointReader;

  public FilterOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Expression filterExpression,
      Expression[] outputExpressions,
      UDTFContext udtfContext)
      throws QueryProcessException, IOException {
    super(
        operatorContext,
        inputOperator,
        inputDataTypes,
        bindExpressions(filterExpression, outputExpressions),
        udtfContext,
        false);
  }

  private static Expression[] bindExpressions(
      Expression filterExpression, Expression[] outputExpressions) {
    Expression[] expressions = new Expression[outputExpressions.length + 1];
    System.arraycopy(outputExpressions, 0, expressions, 0, outputExpressions.length);
    expressions[expressions.length - 1] = filterExpression;
    return expressions;
  }

  @Override
  protected void initTransformers() throws QueryProcessException, IOException {
    super.initTransformers();

    filterPointReader = transformers[transformers.length - 1];
    if (filterPointReader.getDataType() != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException(
          String.format(
              "Data type of the filter expression should be BOOLEAN, but %s is received.",
              filterPointReader.getDataType()));
    }
  }

  @Override
  protected void initLayerPointReaders() throws QueryProcessException, IOException {
    iterateFilterReaderToNextValid();
  }

  private void iterateFilterReaderToNextValid() throws QueryProcessException, IOException {
    while (filterPointReader.next()
        && (filterPointReader.isCurrentNull() || !filterPointReader.currentBoolean())) {
      filterPointReader.readyForNext();
    }
  }

  @Override
  public TsBlock next() {
    final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();

    final int outputColumnCount = transformers.length - 1;

    if (outputDataTypes == null) {
      outputDataTypes = new ArrayList<>();
      for (int i = 0; i < outputColumnCount; ++i) {
        outputDataTypes.add(transformers[i].getDataType());
      }
    }
    tsBlockBuilder.buildValueColumnBuilders(outputDataTypes);

    final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    try {
      int rowCount = 0;
      while (rowCount < FETCH_SIZE && filterPointReader.next()) {
        final long currentTime = filterPointReader.currentTime();

        boolean hasAtLeastOneValid = false;
        for (int i = 0; i < outputColumnCount; ++i) {
          if (currentTime == iterateValueReadersToNextValid(transformers[i], currentTime)) {
            hasAtLeastOneValid = true;
          }
        }

        if (hasAtLeastOneValid) {
          timeBuilder.writeLong(currentTime);
          for (int i = 0; i < outputColumnCount; ++i) {
            collectDataPoint(transformers[i], columnBuilders[i], currentTime);
          }
          ++rowCount;
        }

        iterateFilterReaderToNextValid();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return tsBlockBuilder.build();
  }

  private long iterateValueReadersToNextValid(LayerPointReader reader, long currentTime)
      throws QueryProcessException, IOException {
    while (reader.next() && (reader.isCurrentNull() || reader.currentTime() < currentTime)) {
      reader.readyForNext();
    }
    return reader.currentTime();
  }

  @Override
  public boolean hasNext() {
    try {
      return filterPointReader.next();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
