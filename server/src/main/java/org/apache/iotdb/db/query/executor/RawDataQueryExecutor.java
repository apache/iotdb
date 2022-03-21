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
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
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
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

/** IoTDB query executor. */
public class RawDataQueryExecutor {

  protected RawDataQueryPlan queryPlan;

  public RawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  private static final Logger logger = LoggerFactory.getLogger(RawDataQueryExecutor.class);

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
    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(queryPlan.getDeduplicatedPaths());
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    try {
      List<PartialPath> paths = queryPlan.getDeduplicatedPaths();
      for (PartialPath path : paths) {
        TSDataType dataType = path.getSeriesType();

        QueryDataSource queryDataSource =
            QueryResourceManager.getInstance()
                .getQueryDataSource(path, context, timeFilter, queryPlan.isAscending());
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
      logger.error("Meet error when init series reader  ", e);
      throw new QueryProcessException("Meet error when init series reader .", e);
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

    // transfer to MeasurementPath to AlignedPath if it's under an aligned entity
    queryPlan.setDeduplicatedPaths(
        queryPlan.getDeduplicatedPaths().stream()
            .map(p -> ((MeasurementPath) p).transformToExactPath())
            .collect(Collectors.toList()));

    TimeGenerator timestampGenerator = getTimeGenerator(context, queryPlan);
    List<Boolean> cached =
        markFilterdPaths(
            queryPlan.getExpression(),
            new ArrayList<>(queryPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    Pair<List<IReaderByTimestamp>, List<List<Integer>>> pair =
        initSeriesReaderByTimestamp(context, queryPlan, cached, timestampGenerator.getTimeFilter());

    return new RawQueryDataSetWithValueFilter(
        queryPlan.getDeduplicatedPaths(),
        queryPlan.getDeduplicatedDataTypes(),
        timestampGenerator,
        pair.left,
        pair.right,
        cached,
        queryPlan.isAscending());
  }

  /**
   * init IReaderByTimestamp for each not cached PartialPath, if it's already been cached, the
   * corresponding IReaderByTimestamp will be null group these not cached PartialPath to one
   * AlignedPath if they belong to same aligned device
   *
   * @return List<IReaderByTimestamp> if it's already been cached, the corresponding
   *     IReaderByTimestamp will be null List<List<Integer>> IReaderByTimestamp's corresponding
   *     index list to the result RowRecord.
   */
  protected Pair<List<IReaderByTimestamp>, List<List<Integer>>> initSeriesReaderByTimestamp(
      QueryContext context, RawDataQueryPlan queryPlan, List<Boolean> cached, Filter timeFilter)
      throws QueryProcessException, StorageEngineException {
    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();

    List<PartialPath> pathList = new ArrayList<>();
    List<PartialPath> notCachedPathList = new ArrayList<>();

    // reader index -> deduplicated path index
    List<List<Integer>> readerToIndexList = new ArrayList<>();
    // fullPath -> reader index
    Map<String, Integer> fullPathToReaderIndexMap = new HashMap<>();
    List<PartialPath> deduplicatedPaths = queryPlan.getDeduplicatedPaths();
    int index = 0;
    for (int i = 0; i < cached.size(); i++) {
      if (cached.get(i)) {
        pathList.add(deduplicatedPaths.get(i));
        readerToIndexList.add(Collections.singletonList(i));
        cached.set(index++, Boolean.TRUE);
      } else {
        notCachedPathList.add(deduplicatedPaths.get(i));
        // For aligned Path, it's deviceID; for nonAligned path, it's full path
        String fullPath = deduplicatedPaths.get(i).getFullPath();
        Integer readerIndex = fullPathToReaderIndexMap.get(fullPath);

        // it's another sub sensor in aligned device, we just add it to the previous AlignedPath
        if (readerIndex != null) {
          AlignedPath anotherSubSensor = (AlignedPath) deduplicatedPaths.get(i);
          ((AlignedPath) pathList.get(readerIndex)).mergeAlignedPath(anotherSubSensor);
          readerToIndexList.get(readerIndex).add(i);
        } else {
          pathList.add(deduplicatedPaths.get(i));
          fullPathToReaderIndexMap.put(fullPath, index);
          List<Integer> indexList = new ArrayList<>();
          indexList.add(i);
          readerToIndexList.add(indexList);
          cached.set(index++, Boolean.FALSE);
        }
      }
    }

    queryPlan.setDeduplicatedPaths(pathList);
    int previousSize = cached.size();
    if (previousSize > pathList.size()) {
      cached.subList(pathList.size(), previousSize).clear();
    }

    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(notCachedPathList);
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }
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
    return new Pair<>(readersOfSelectedSeries, readerToIndexList);
  }

  protected IReaderByTimestamp getReaderByTimestamp(
      PartialPath path, Set<String> allSensors, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        allSensors,
        dataType,
        context,
        QueryResourceManager.getInstance()
            .getQueryDataSource(path, context, null, queryPlan.isAscending()),
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
