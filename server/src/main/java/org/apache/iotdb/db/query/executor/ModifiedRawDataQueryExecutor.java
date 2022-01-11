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
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.iterator.ServerSeriesIterator;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.iterator.CalcTimeSeries;
import org.apache.iotdb.tsfile.read.query.iterator.EqualJoin;
import org.apache.iotdb.tsfile.read.query.iterator.ProjectTimeSeries;
import org.apache.iotdb.tsfile.read.query.iterator.RelInput;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeries;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeriesQueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** IoTDB query executor. */
public class ModifiedRawDataQueryExecutor {

  protected RawDataQueryPlan queryPlan;

  public ModifiedRawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  private static final Logger logger = LoggerFactory.getLogger(ModifiedRawDataQueryExecutor.class);

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
          context.getQueryId(), queryPlan, readersOfSelectedSeries);
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
    List<VirtualStorageGroupProcessor> list = null;
//        StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    try {
      List<PartialPath> paths = queryPlan.getDeduplicatedPaths();
      for (PartialPath path : paths) {
        TSDataType dataType = path.getSeriesType();

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
    } catch (Exception e) {
      logger.error("Meet error when init series reader ", e);
      throw new QueryProcessException("Meet error when init series reader.", e);
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
    if (!(queryPlan.getExpression() instanceof SingleSeriesExpression)) {
      throw new UnsupportedOperationException("Currently not supported");
    }

    // How do we now upfront where each entry will be placed
    // If we e.g. get
    // 2*s1, s1 where s2 == xxx
    //  ^    ^        ^
    //  2    1        0
    // Or in this more complex case
    // 2*abs(s1), s1 where s2 == xxx
    // ^    ^     ^        ^
    // 3    2     1        0
    // Where we would need two Calc stages with the current implementation
    SingleSeriesExpression expression = (SingleSeriesExpression) queryPlan.getExpression();
    EqualJoin iterator = new EqualJoin(
        new ServerSeriesIterator(context, queryPlan, queryPlan.getPaths().get(0).transformToExactPath(), null),
        new ServerSeriesIterator(context, queryPlan, ((PartialPath) expression.getSeriesPath()), expression.getFilter()),
        queryPlan.isAscending()
    );
    TimeSeries calc = new CalcTimeSeries(iterator, Arrays.asList(
        new CalcTimeSeries.NoopExpression(new RelInput.RelInputImpl(0, TSDataType.TEXT)),
        new CalcTimeSeries.DoubleExpression(new RelInput.RelInputImpl(0, TSDataType.INT32))
    ));
    TimeSeries calc2 = new CalcTimeSeries(calc, Arrays.asList(
        new CalcTimeSeries.DoubleExpression(new RelInput.RelInputImpl(2, TSDataType.INT32))
    ));
    ProjectTimeSeries projection = new ProjectTimeSeries(calc2, new int[]{0});
    return new TimeSeriesQueryDataSet(projection);
  }

  protected List<IReaderByTimestamp> initSeriesReaderByTimestamp(
      QueryContext context, RawDataQueryPlan queryPlan, List<Boolean> cached)
      throws QueryProcessException, StorageEngineException {
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    List<VirtualStorageGroupProcessor> list = null;
//        StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
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
