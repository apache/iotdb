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
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.udf.core.layer.DAGBuilder;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
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
  protected final RawQueryInputLayer rawQueryInputLayer;

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
    rawQueryInputLayer =
        new RawQueryInputLayer(
            queryId,
            UDF_READER_MEMORY_BUDGET_IN_MB,
            deduplicatedPaths,
            deduplicatedDataTypes,
            timestampGenerator,
            readersOfSelectedSeries,
            cached);

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
    rawQueryInputLayer =
        new RawQueryInputLayer(
            queryId,
            UDF_READER_MEMORY_BUDGET_IN_MB,
            deduplicatedPaths,
            deduplicatedDataTypes,
            readersOfSelectedSeries);

    initTransformers();
  }

  protected void initTransformers() throws QueryProcessException, IOException {
    UDFRegistrationService.getInstance().acquireRegistrationLock();
    // This statement must be surrounded by the registration lock.
    UDFClassLoaderManager.getInstance().initializeUDFQuery(queryId);
    try {
      // UDF executors will be initialized at the same time
      transformers =
          new DAGBuilder(
                  queryId,
                  udtfPlan,
                  rawQueryInputLayer,
                  UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB + UDF_COLLECTOR_MEMORY_BUDGET_IN_MB)
              .buildLayerMemoryAssigner()
              .buildResultColumnPointReaders()
              .setDataSetResultColumnDataTypes()
              .getResultColumnPointReaders();
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  public void finalizeUDFs(long queryId) {
    udtfPlan.finalizeUDFExecutors(queryId);
  }
}
