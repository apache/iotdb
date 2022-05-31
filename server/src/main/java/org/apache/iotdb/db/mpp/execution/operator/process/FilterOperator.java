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
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterOperator extends TransformOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterOperator.class);

  private LayerPointReader filterPointReader;

  public FilterOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Map<String, List<InputLocation>> inputLocations,
      Expression filterExpression,
      Expression[] outputExpressions,
      boolean keepNull,
      ZoneId zoneId,
      TypeProvider typeProvider)
      throws QueryProcessException, IOException {
    super(
        operatorContext,
        inputOperator,
        inputDataTypes,
        inputLocations,
        bindExpressions(filterExpression, outputExpressions),
        keepNull,
        zoneId,
        typeProvider);
  }

  private static Expression[] bindExpressions(
      Expression filterExpression, Expression[] outputExpressions) {
    Expression[] expressions = new Expression[outputExpressions.length + 1];
    System.arraycopy(outputExpressions, 0, expressions, 0, outputExpressions.length);
    expressions[expressions.length - 1] = filterExpression;
    return expressions;
  }

  @Override
  protected void initTransformers(
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      TypeProvider typeProvider)
      throws QueryProcessException, IOException {
    super.initTransformers(inputLocations, outputExpressions, typeProvider);

    filterPointReader = transformers[transformers.length - 1];
    if (filterPointReader.getDataType() != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException(
          String.format(
              "Data type of the filter expression should be BOOLEAN, but %s is received.",
              filterPointReader.getDataType()));
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

      while (rowCount < FETCH_SIZE && !timeHeap.isEmpty()) {
        final long currentTime = timeHeap.pollFirst();

        if (filterPointReader.next() && filterPointReader.currentTime() == currentTime) {
          if (!filterPointReader.isCurrentNull() && filterPointReader.currentBoolean()) {
            // time
            timeBuilder.writeLong(currentTime);

            // values
            for (int i = 0; i < outputColumnCount; ++i) {
              collectDataPointAndIterateToNextValid(
                  transformers[i], columnBuilders[i], currentTime);
            }

            ++rowCount;
          } else {
            // values
            for (int i = 0; i < outputColumnCount; ++i) {
              skipDataPointAndIterateToNextValid(transformers[i], currentTime);
            }
          }

          filterPointReader.readyForNext();
          iterateReaderToNextValid(filterPointReader);
        } else {
          // values
          for (int i = 0; i < outputColumnCount; ++i) {
            skipDataPointAndIterateToNextValid(transformers[i], currentTime);
          }
        }

        inputLayer.updateRowRecordListEvictionUpperBound();
      }

      tsBlockBuilder.declarePositions(rowCount);
    } catch (Exception e) {
      LOGGER.error("FilterOperator#next()", e);
      throw new RuntimeException(e);
    }

    return tsBlockBuilder.build();
  }

  private void skipDataPointAndIterateToNextValid(LayerPointReader reader, long currentTime)
      throws IOException, QueryProcessException {
    if (!reader.next() || reader.currentTime() != currentTime) {
      return;
    }

    reader.readyForNext();
    iterateReaderToNextValid(reader);
  }
}
