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

package org.apache.iotdb.db.query.executor;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.UDTFAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.UDTFNonAlignDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class UDTFQueryExecutor extends RawDataQueryExecutor {

  protected final UDTFPlan udtfPlan;

  public UDTFQueryExecutor(UDTFPlan udtfPlan) {
    super(udtfPlan);
    this.udtfPlan = udtfPlan;
  }

  public QueryDataSet executeWithoutValueFilterAlignByTime(QueryContext context,
      RawDataQueryPlan queryPlan)
      throws StorageEngineException, QueryProcessException, IOException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context, queryPlan);
    return new UDTFAlignByTimeDataSet(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes,
        readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterAlignByTime(QueryContext context,
      RawDataQueryPlan queryPlan)
      throws StorageEngineException, QueryProcessException, IOException {
    TimeGenerator timestampGenerator = getTimeGenerator(optimizedExpression, context, queryPlan);
    List<Boolean> cached = markFilterdPaths(optimizedExpression, deduplicatedPaths,
        timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries = initSeriesReaderByTimestamp(context,
        queryPlan, cached);
    return new UDTFAlignByTimeDataSet(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries, cached);
  }

  public QueryDataSet executeWithoutValueFilterNonAlign(QueryContext context,
      RawDataQueryPlan queryPlan) throws QueryProcessException, StorageEngineException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context, queryPlan);
    return new UDTFNonAlignDataSet(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes,
        readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterNonAlign(QueryContext context,
      RawDataQueryPlan queryPlan)
      throws QueryProcessException, StorageEngineException, IOException {
    TimeGenerator timestampGenerator = getTimeGenerator(optimizedExpression, context, queryPlan);
    List<Boolean> cached = markFilterdPaths(optimizedExpression, deduplicatedPaths,
        timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries = initSeriesReaderByTimestamp(context,
        queryPlan, cached);
    return new UDTFNonAlignDataSet(context, udtfPlan, deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries, cached);
  }
}
