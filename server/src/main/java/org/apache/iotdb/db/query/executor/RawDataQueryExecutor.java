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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
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
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

/** IoTDB query executor. */
public class RawDataQueryExecutor {

  protected RawDataQueryPlan queryPlan;

  public RawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  /** without filter or with global time filter. */
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, false);
    if (dataSet != null) {
      return dataSet;
    }
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);

    try {
      return new RawQueryDataSetWithoutValueFilter(
          context.getQueryId(),
          queryPlan.getDeduplicatedPaths(),
          queryPlan.getDeduplicatedDataTypes(),
          readersOfSelectedSeries,
          queryPlan.isAscending());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineException(e.getMessage());
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  public final QueryDataSet executeNonAlign(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, false);
    if (dataSet != null) {
      return dataSet;
    }
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new NonAlignEngineDataSet(
        context.getQueryId(),
        queryPlan.getDeduplicatedPaths(),
        queryPlan.getDeduplicatedDataTypes(),
        readersOfSelectedSeries);
  }

  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    Filter timeFilter = null;
    if (queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    try {
      for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
        PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
        TSDataType dataType = queryPlan.getDeduplicatedDataTypes().get(i);

        QueryDataSource queryDataSource =
            QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
        timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

        ManagedSeriesReader reader =
            new SeriesRawDataBatchReader(
                path,
                queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                dataType,
                context,
                queryDataSource,
                timeFilter,
                null,
                null,
                queryPlan.isAscending());
        readersOfSelectedSeries.add(reader);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
    return readersOfSelectedSeries;
  }

  /**
   * executeWithValueFilter query.
   *
   * @return QueryDataSet object
   * @throws StorageEngineException StorageEngineException
   */
  public final QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, QueryProcessException {
    QueryDataSet dataSet = needRedirect(context, true);
    if (dataSet != null) {
      return dataSet;
    }

    TimeGenerator timestampGenerator = getTimeGenerator(context, queryPlan);
    List<Boolean> cached =
        markFilterdPaths(
            queryPlan.getExpression(),
            new ArrayList<>(queryPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    List<IReaderByTimestamp> readersOfSelectedSeries =
        initSeriesReaderByTimestamp(context, queryPlan, cached);
    return new RawQueryDataSetWithValueFilter(
        queryPlan.getDeduplicatedPaths(),
        queryPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        readersOfSelectedSeries,
        cached,
        queryPlan.isAscending());
  }

  protected List<IReaderByTimestamp> initSeriesReaderByTimestamp(
      QueryContext context, RawDataQueryPlan queryPlan, List<Boolean> cached)
      throws QueryProcessException, StorageEngineException {
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    try {
      for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
        if (cached.get(i)) {
          readersOfSelectedSeries.add(null);
          continue;
        }
        PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTimestamp(
                path,
                queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                queryPlan.getDeduplicatedDataTypes().get(i),
                context);
        readersOfSelectedSeries.add(seriesReaderByTimestamp);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
    return readersOfSelectedSeries;
  }

  protected IReaderByTimestamp getReaderByTimestamp(
      PartialPath path, Set<String> allSensors, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        allSensors,
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        queryPlan.isAscending());
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  /**
   * Check whether need to redirect query to other node.
   *
   * @param context query context
   * @param hasValueFilter if has value filter, we need to check timegenerator
   * @return dummyDataSet to avoid more cost, if null, no need
   */
  protected QueryDataSet needRedirect(QueryContext context, boolean hasValueFilter)
      throws StorageEngineException, QueryProcessException {
    return null;
  }
}
