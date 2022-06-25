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
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
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
      TypeProvider typeProvider,
      boolean isAscending)
      throws QueryProcessException, IOException {
    super(
        operatorContext,
        inputOperator,
        inputDataTypes,
        inputLocations,
        bindExpressions(filterExpression, outputExpressions),
        keepNull,
        zoneId,
        typeProvider,
        isAscending);
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

    try {
      YieldableState yieldableState = iterateAllColumnsToNextValid();
      if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
        return null;
      }

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

      int rowCount = 0;
      while (!timeHeap.isEmpty()) {
        final long currentTime = timeHeap.pollFirst();

        yieldableState = filterPointReader.yield();
        if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
          timeHeap.add(currentTime);
          tsBlockBuilder.declarePositions(rowCount);
          return tsBlockBuilder.build();
        }

        if (yieldableState == YieldableState.YIELDABLE
            && filterPointReader.currentTime() == currentTime) {

          boolean isReaderContinueNull = true;
          for (int i = 0; isReaderContinueNull && i < outputColumnCount; ++i) {
            isReaderContinueNull = collectReaderAppendIsNull(transformers[i], currentTime);
          } // After the loop, isReaderContinueNull is true means all values of readers are null

          if (!filterPointReader.isCurrentNull()
              && filterPointReader.currentBoolean()
              && !isReaderContinueNull) {
            // time
            timeBuilder.writeLong(currentTime);

            // values
            for (int i = 0; i < outputColumnCount; ++i) {
              yieldableState = collectDataPoint(transformers[i], columnBuilders[i], currentTime, i);
              if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
                for (int j = 0; j <= i; ++j) {
                  shouldIterateReadersToNextValid[j] = false;
                }
                timeHeap.add(currentTime);

                tsBlockBuilder.declarePositions(rowCount);
                return tsBlockBuilder.build();
              }
            }
            shouldIterateReadersToNextValid[outputColumnCount] = true;

            for (int i = 0; i <= outputColumnCount; ++i) {
              if (shouldIterateReadersToNextValid[i]) {
                transformers[i].readyForNext();
              }
            }

            ++rowCount;
          } else {
            // values
            for (int i = 0; i < outputColumnCount; ++i) {
              yieldableState = skipDataPoint(transformers[i], currentTime, i);
              if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
                for (int j = 0; j <= i; ++j) {
                  shouldIterateReadersToNextValid[j] = false;
                }
                timeHeap.add(currentTime);

                tsBlockBuilder.declarePositions(rowCount);
                return tsBlockBuilder.build();
              }
            }
            shouldIterateReadersToNextValid[outputColumnCount] = true;

            for (int i = 0; i <= outputColumnCount; ++i) {
              if (shouldIterateReadersToNextValid[i]) {
                transformers[i].readyForNext();
              }
            }
          }
        } else {
          // values
          for (int i = 0; i < outputColumnCount; ++i) {
            yieldableState = skipDataPoint(transformers[i], currentTime, i);
            if (yieldableState == YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA) {
              for (int j = 0; j <= i; ++j) {
                shouldIterateReadersToNextValid[j] = false;
              }
              timeHeap.add(currentTime);

              tsBlockBuilder.declarePositions(rowCount);
              return tsBlockBuilder.build();
            }
          }

          for (int i = 0; i < outputColumnCount; ++i) {
            if (shouldIterateReadersToNextValid[i]) {
              transformers[i].readyForNext();
            }
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
      LOGGER.error("FilterOperator#next()", e);
      throw new RuntimeException(e);
    }
  }

  private YieldableState skipDataPoint(LayerPointReader reader, long currentTime, int readerIndex)
      throws IOException, QueryProcessException {
    final YieldableState yieldableState = reader.yield();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }

    if (reader.currentTime() == currentTime) {
      shouldIterateReadersToNextValid[readerIndex] = true;
    }

    return YieldableState.YIELDABLE;
  }
}
