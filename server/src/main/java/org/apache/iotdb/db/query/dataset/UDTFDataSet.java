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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.InputLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticAdditionTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticDivisionTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticModuloTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticMultiplicationTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticNegationTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticSubtractionTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.RawQueryPointTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowWindowTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class UDTFDataSet extends QueryDataSet {

  protected static final float UDF_READER_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  protected static final float UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  protected static final float UDF_COLLECTOR_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  protected final long queryId;
  protected final UDTFPlan udtfPlan;
  protected final InputLayer inputLayer;

  protected LayerPointReader[] transformers;

  /** execute with value filters */
  protected UDTFDataSet(
      QueryContext queryContext,
      UDTFPlan udtfPlan,
      List<PartialPath> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes,
      TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries,
      List<Boolean> cached)
      throws QueryProcessException, IOException {
    super(new ArrayList<>(deduplicatedPaths), deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    inputLayer =
        new InputLayer(
            queryId,
            UDF_READER_MEMORY_BUDGET_IN_MB,
            deduplicatedPaths,
            deduplicatedDataTypes,
            timestampGenerator,
            readersOfSelectedSeries,
            cached);
    udtfPlan.initializeUdfExecutors(queryId, UDF_COLLECTOR_MEMORY_BUDGET_IN_MB);
    initTransformers();
  }

  /** execute without value filters */
  protected UDTFDataSet(
      QueryContext queryContext,
      UDTFPlan udtfPlan,
      List<PartialPath> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes,
      List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException, IOException, InterruptedException {
    super(new ArrayList<>(deduplicatedPaths), deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    inputLayer =
        new InputLayer(
            queryId,
            UDF_READER_MEMORY_BUDGET_IN_MB,
            deduplicatedPaths,
            deduplicatedDataTypes,
            readersOfSelectedSeries);
    udtfPlan.initializeUdfExecutors(queryId, UDF_COLLECTOR_MEMORY_BUDGET_IN_MB);
    initTransformers();
  }

  protected void initTransformers() throws QueryProcessException, IOException {
    final float memoryBudgetForSingleWindowTransformer =
        calculateMemoryBudgetForSingleWindowTransformer();
    final int size = udtfPlan.getPathToIndex().size();
    transformers = new Transformer[size];
    for (int i = 0; i < size; ++i) {
      if (udtfPlan.isUdfColumn(i)) {
        constructUdfTransformer(i, memoryBudgetForSingleWindowTransformer);
      } else if (udtfPlan.isArithmeticColumn(i)) {
        constructArithmeticTransformer(i);
      } else {
        constructRawQueryTransformer(i);
      }
    }
  }

  @SuppressWarnings("squid:S3518") // "Math.max(windowTransformerCount, 1)" can't be zero
  private float calculateMemoryBudgetForSingleWindowTransformer() {
    int size = udtfPlan.getPathToIndex().size();
    int windowTransformerCount = 0;
    for (int i = 0; i < size; ++i) {
      if (udtfPlan.isUdfColumn(i)) {
        switch (udtfPlan
            .getExecutorByDataSetOutputColumnIndex(i)
            .getConfigurations()
            .getAccessStrategy()
            .getAccessStrategyType()) {
          case SLIDING_SIZE_WINDOW:
          case SLIDING_TIME_WINDOW:
            ++windowTransformerCount;
            break;
          default:
            break;
        }
      }
    }
    return UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB / Math.max(windowTransformerCount, 1);
  }

  private void constructUdfTransformer(
      int columnIndex, float memoryBudgetForSingleWindowTransformer)
      throws QueryProcessException, IOException {
    UDTFExecutor executor = udtfPlan.getExecutorByDataSetOutputColumnIndex(columnIndex);
    int[] readerIndexes = calculateUdfReaderIndexes(executor);
    AccessStrategy accessStrategy = executor.getConfigurations().getAccessStrategy();
    switch (accessStrategy.getAccessStrategyType()) {
      case ROW_BY_ROW:
        transformers[columnIndex] =
            new UDFQueryRowTransformer(inputLayer.constructRowReader(readerIndexes), executor);
        break;
      case SLIDING_SIZE_WINDOW:
      case SLIDING_TIME_WINDOW:
        transformers[columnIndex] =
            new UDFQueryRowWindowTransformer(
                inputLayer.constructRowWindowReader(
                    readerIndexes, accessStrategy, memoryBudgetForSingleWindowTransformer),
                executor);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported transformer access strategy");
    }
  }

  private int[] calculateUdfReaderIndexes(UDTFExecutor executor) {
    List<PartialPath> paths = executor.getExpression().getPaths();
    int[] readerIndexes = new int[paths.size()];
    for (int i = 0; i < readerIndexes.length; ++i) {
      readerIndexes[i] = udtfPlan.getReaderIndex(paths.get(i).getFullPath());
    }
    return readerIndexes;
  }

  private void constructArithmeticTransformer(int columnIndex) {
    Expression expression =
        udtfPlan.getResultColumnByDatasetOutputIndex(columnIndex).getExpression();

    // unary expression
    if (expression instanceof NegationExpression) {
      transformers[columnIndex] =
          new ArithmeticNegationTransformer(
              constructPointReaderBySeriesName(
                  ((NegationExpression) expression).getExpression().getExpressionString()));
      return;
    }

    // binary expression
    BinaryExpression binaryExpression = (BinaryExpression) expression;
    if (binaryExpression instanceof AdditionExpression) {
      transformers[columnIndex] =
          new ArithmeticAdditionTransformer(
              constructPointReaderBySeriesName(
                  binaryExpression.getLeftExpression().getExpressionString()),
              constructPointReaderBySeriesName(
                  binaryExpression.getRightExpression().getExpressionString()));
    } else if (binaryExpression instanceof SubtractionExpression) {
      transformers[columnIndex] =
          new ArithmeticSubtractionTransformer(
              constructPointReaderBySeriesName(
                  binaryExpression.getLeftExpression().getExpressionString()),
              constructPointReaderBySeriesName(
                  binaryExpression.getRightExpression().getExpressionString()));
    } else if (binaryExpression instanceof MultiplicationExpression) {
      transformers[columnIndex] =
          new ArithmeticMultiplicationTransformer(
              constructPointReaderBySeriesName(
                  binaryExpression.getLeftExpression().getExpressionString()),
              constructPointReaderBySeriesName(
                  binaryExpression.getRightExpression().getExpressionString()));
    } else if (binaryExpression instanceof DivisionExpression) {
      transformers[columnIndex] =
          new ArithmeticDivisionTransformer(
              constructPointReaderBySeriesName(
                  binaryExpression.getLeftExpression().getExpressionString()),
              constructPointReaderBySeriesName(
                  binaryExpression.getRightExpression().getExpressionString()));
    } else if (binaryExpression instanceof ModuloExpression) {
      transformers[columnIndex] =
          new ArithmeticModuloTransformer(
              constructPointReaderBySeriesName(
                  binaryExpression.getLeftExpression().getExpressionString()),
              constructPointReaderBySeriesName(
                  binaryExpression.getRightExpression().getExpressionString()));
    } else {
      throw new UnsupportedOperationException(binaryExpression.getExpressionString());
    }
  }

  private void constructRawQueryTransformer(int columnIndex) {
    transformers[columnIndex] =
        new RawQueryPointTransformer(
            constructPointReaderBySeriesName(
                udtfPlan.getRawQueryColumnNameByDatasetOutputColumnIndex(columnIndex)));
  }

  private LayerPointReader constructPointReaderBySeriesName(String seriesName) {
    return inputLayer.constructPointReader(udtfPlan.getReaderIndex(seriesName));
  }

  public void finalizeUDFs(long queryId) {
    udtfPlan.finalizeUDFExecutors(queryId);
  }
}
