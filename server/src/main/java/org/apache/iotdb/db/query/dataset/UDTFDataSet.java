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
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.input.InputLayer;
import org.apache.iotdb.db.query.udf.core.input.InputLayerWithValueFilter;
import org.apache.iotdb.db.query.udf.core.input.InputLayerWithoutValueFilter;
import org.apache.iotdb.db.query.udf.core.transformer.RawQueryPointTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.UDFQueryRowWindowTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public abstract class UDTFDataSet extends QueryDataSet {

  protected final long queryId;
  protected final UDTFPlan udtfPlan;
  protected final InputLayer inputLayer;

  protected Transformer[] transformers;

  // execute with value filters
  public UDTFDataSet(QueryContext queryContext, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, TimeGenerator timestampGenerator,
      List<IReaderByTimestamp> readersOfSelectedSeries, List<Boolean> cached)
      throws QueryProcessException, IOException {
    super(deduplicatedPaths, deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    udtfPlan.initializeUdfExecutor(queryId);
    inputLayer = new InputLayerWithValueFilter(queryId, deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries, cached);
    initTransformers();
  }

  // execute without value filters
  public UDTFDataSet(QueryContext queryContext, UDTFPlan udtfPlan, List<Path> deduplicatedPaths,
      List<TSDataType> deduplicatedDataTypes, List<ManagedSeriesReader> readersOfSelectedSeries)
      throws QueryProcessException, IOException, InterruptedException {
    super(deduplicatedPaths, deduplicatedDataTypes);
    queryId = queryContext.getQueryId();
    this.udtfPlan = udtfPlan;
    udtfPlan.initializeUdfExecutor(queryId);
    inputLayer = new InputLayerWithoutValueFilter(queryId, deduplicatedPaths, deduplicatedDataTypes,
        readersOfSelectedSeries);
    initTransformers();
  }

  protected void initTransformers() throws QueryProcessException, IOException {
    int size = udtfPlan.getPathToIndex().size();
    transformers = new Transformer[size];
    for (int i = 0; i < size; ++i) {
      if (udtfPlan.isUdfColumn(i)) {
        UDTFExecutor executor = udtfPlan.getExecutorByDataSetOutputColumnIndex(i);
        int[] readerIndexes = calculateReaderIndexes(executor);
        AccessStrategy accessStrategy = executor.getConfigurations().getAccessStrategy();
        switch (accessStrategy.getAccessStrategyType()) {
          case ONE_BY_ONE:
            transformers[i] = new UDFQueryRowTransformer(
                inputLayer.constructRowReader(readerIndexes), executor);
            break;
          case TUMBLING_WINDOW:
          case SLIDING_TIME_WINDOW:
            transformers[i] = new UDFQueryRowWindowTransformer(
                inputLayer.constructRowWindowReader(readerIndexes, accessStrategy), executor);
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
    List<Path> paths = executor.getContext().getPaths();
    int[] readerIndexes = new int[paths.size()];
    for (int i = 0; i < readerIndexes.length; ++i) {
      readerIndexes[i] = udtfPlan.getReaderIndex(paths.get(i).getFullPath());
    }
    return readerIndexes;
  }

  public void finalizeUDFs() {
    udtfPlan.finalizeUDFExecutors();
  }
}
