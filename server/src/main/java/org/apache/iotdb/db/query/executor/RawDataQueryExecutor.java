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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB query executor.
 */
public class RawDataQueryExecutor {

  protected List<Path> deduplicatedPaths;
  protected List<TSDataType> deduplicatedDataTypes;
  protected IExpression optimizedExpression;

  public RawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    this.deduplicatedPaths = queryPlan.getDeduplicatedPaths();
    this.deduplicatedDataTypes = queryPlan.getDeduplicatedDataTypes();
    this.optimizedExpression = queryPlan.getExpression();
  }

  /**
   * without filter or with global time filter.
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
          throws StorageEngineException {

    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    try {
      return new RawQueryDataSetWithoutValueFilter(deduplicatedPaths, deduplicatedDataTypes,
          readersOfSelectedSeries);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineException(e.getMessage());
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  public QueryDataSet executeNonAlign(QueryContext context) throws StorageEngineException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new NonAlignEngineDataSet(deduplicatedPaths, deduplicatedDataTypes,
        readersOfSelectedSeries);
  }

  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException {
    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context, timeFilter);
      timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

      ManagedSeriesReader reader = new SeriesRawDataBatchReader(path, dataType, context,
          queryDataSource, timeFilter, null, null);
      readersOfSelectedSeries.add(reader);
    }
    return readersOfSelectedSeries;
  }

  /**
   * executeWithValueFilter query.
   *
   * @return QueryDataSet object
   * @throws StorageEngineException StorageEngineException
   */
  public QueryDataSet executeWithValueFilter(QueryContext context) throws StorageEngineException {

    TimeGenerator timestampGenerator = getTimeGenerator(
        optimizedExpression, context);

    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      IReaderByTimestamp seriesReaderByTimestamp = getReaderByTimestamp(path,
          deduplicatedDataTypes.get(i), context);
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }
    return new RawQueryDataSetWithValueFilter(deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries);
  }

  protected IReaderByTimestamp getReaderByTimestamp(Path path, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException {
    return new SeriesReaderByTimestamp(path,
        dataType, context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null), null);
  }

  protected TimeGenerator getTimeGenerator(IExpression expression,
      QueryContext context) throws StorageEngineException {
    return new ServerTimeGenerator(expression, context);
  }
}
