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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.input.InputLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.RawQueryPointTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowWindowTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public abstract class UDTFDataSet extends QueryDataSet {

  protected static final float UDF_READER_MEMORY_BUDGET_IN_MB = IoTDBDescriptor.getInstance()
      .getConfig().getUdfReaderMemoryBudgetInMB();
  protected static final float UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB = IoTDBDescriptor.getInstance()
      .getConfig().getUdfTransformerMemoryBudgetInMB();
  protected static final float UDF_COLLECTOR_MEMORY_BUDGET_IN_MB = IoTDBDescriptor.getInstance()
      .getConfig().getUdfCollectorMemoryBudgetInMB();

  protected final long queryId;
  protected final UDTFPlan udtfPlan;
  protected final InputLayer inputLayer;

  protected LayerPointReader[] transformers;

  /**
   * execute with value filters
   */
  protected UDTFDataSet(QueryContext queryContext, UDTFPlan udtfPlan,
      List<PartialPath> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      TimeGenerator timestampGenerator, List<IReaderByTimestamp> readersOfSelectedSeries,
      List<Boolean> cached) throws QueryProcessException, IOException {
    super(new ArrayList<>(deduplicatedPaths), deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    inputLayer = new InputLayer(queryId, UDF_READER_MEMORY_BUDGET_IN_MB, deduplicatedPaths,
        deduplicatedDataTypes, timestampGenerator, readersOfSelectedSeries, cached);
    udtfPlan.initializeUdfExecutors(queryId, UDF_COLLECTOR_MEMORY_BUDGET_IN_MB);
    initTransformers(UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB);
  }

  /**
   * execute without value filters
   */
  protected UDTFDataSet(QueryContext queryContext, UDTFPlan udtfPlan,
      List<PartialPath> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException, IOException, InterruptedException {
    super(new ArrayList<>(deduplicatedPaths), deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    inputLayer = new InputLayer(queryId, UDF_READER_MEMORY_BUDGET_IN_MB, deduplicatedPaths,
        deduplicatedDataTypes, readersOfSelectedSeries);
    udtfPlan.initializeUdfExecutors(queryId, UDF_COLLECTOR_MEMORY_BUDGET_IN_MB);
    initTransformers(UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB);
  }

  @SuppressWarnings("squid:S3518") // "Math.max(windowTransformerCount, 1)" can't be zero
  protected void initTransformers(float memoryBudgetInMB)
      throws QueryProcessException, IOException {
    int size = udtfPlan.getPathToIndex().size();
    transformers = new Transformer[size];

    int windowTransformerCount = 0;
    for (int i = 0; i < size; ++i) {
      if (udtfPlan.isUdfColumn(i)) {
        AccessStrategy accessStrategy = udtfPlan.getExecutorByDataSetOutputColumnIndex(i)
            .getConfigurations().getAccessStrategy();
        switch (accessStrategy.getAccessStrategyType()) {
          case SLIDING_SIZE_WINDOW:
          case SLIDING_TIME_WINDOW:
            ++windowTransformerCount;
            break;
          default:
            break;
        }
      }
    }
    memoryBudgetInMB /= Math.max(windowTransformerCount, 1);

    for (int i = 0; i < size; ++i) {
      if (udtfPlan.isUdfColumn(i)) {
        UDTFExecutor executor = udtfPlan.getExecutorByDataSetOutputColumnIndex(i);
        int[] readerIndexes = calculateReaderIndexes(executor);
        AccessStrategy accessStrategy = executor.getConfigurations().getAccessStrategy();
        switch (accessStrategy.getAccessStrategyType()) {
          case ROW_BY_ROW:
            transformers[i] = new UDFQueryRowTransformer(
                inputLayer.constructRowReader(readerIndexes), executor);
            break;
          case SLIDING_SIZE_WINDOW:
          case SLIDING_TIME_WINDOW:
            transformers[i] = new UDFQueryRowWindowTransformer(inputLayer
                .constructRowWindowReader(readerIndexes, accessStrategy, memoryBudgetInMB),
                executor);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported transformer access strategy");
        }
      } else {
        transformers[i] = new RawQueryPointTransformer(inputLayer.constructPointReader(
            udtfPlan.getReaderIndex(udtfPlan.getRawQueryColumnNameByDatasetOutputColumnIndex(i))));
      }
    }
  }

  private int[] calculateReaderIndexes(UDTFExecutor executor) {
    List<PartialPath> paths = executor.getContext().getPaths();
    int[] readerIndexes = new int[paths.size()];
    for (int i = 0; i < readerIndexes.length; ++i) {
      readerIndexes[i] = udtfPlan.getReaderIndex(paths.get(i).getFullPath());
    }
    return readerIndexes;
  }

  public void finalizeUDFs(long queryId) {
    udtfPlan.finalizeUDFExecutors(queryId);
  }
}
