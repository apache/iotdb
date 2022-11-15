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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.externalsort.serialize.IExternalSortFileDeserializer;
import org.apache.iotdb.db.service.TemporaryQueryDataFileService;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * QueryResourceManager manages resource (file streams) used by each query job, and assign Ids to
 * the jobs. During the life cycle of a query, the following methods must be called in strict order:
 *
 * <p>1. assignQueryId - get an Id for the new query.
 *
 * <p>2. getQueryDataSource - open files for the job or reuse existing readers.
 *
 * <p>3. endQueryForGivenJob - release the resource used by this job.
 */
public class QueryResourceManager {

  private final AtomicLong queryIdAtom = new AtomicLong();
  private final QueryFileManager filePathsManager;

  /**
   * Record temporary files used for external sorting.
   *
   * <p>Key: query job id. Value: temporary file list used for external sorting.
   */
  private final Map<Long, List<IExternalSortFileDeserializer>> externalSortFileMap;

  /**
   * Record QueryDataSource used in queries
   *
   * <p>Key: query job id. Value: QueryDataSource corresponding to each data region.
   */
  private final Map<Long, Map<String, QueryDataSource>> cachedQueryDataSourcesMap;

  private QueryResourceManager() {
    filePathsManager = new QueryFileManager();
    externalSortFileMap = new ConcurrentHashMap<>();
    cachedQueryDataSourcesMap = new ConcurrentHashMap<>();
  }

  public static QueryResourceManager getInstance() {
    return QueryTokenManagerHelper.INSTANCE;
  }

  /** Register a new query. When a query request is created firstly, this method must be invoked. */
  public long assignQueryId() {
    return queryIdAtom.incrementAndGet();
  }

  /**
   * Register a query id for compaction. The name of the compaction thread is
   * 'pool-x-IoTDB-Compaction-xx', xx in which is usually an integer from 0 to
   * MAXCOMPACTION_THREAD_NUM. We use the following rules to define query id for compaction: <br>
   * queryId = xx + Long.MIN_VALUE
   */
  public long assignCompactionQueryId() {
    long threadNum = Long.parseLong((Thread.currentThread().getName().split("-"))[4]);
    long queryId = Long.MIN_VALUE + threadNum;
    filePathsManager.addQueryId(queryId);
    return queryId;
  }

  /**
   * register temporary file generated by external sort for resource release.
   *
   * @param queryId query job id
   * @param deserializer deserializer of temporary file in external sort.
   */
  public void registerTempExternalSortFile(
      long queryId, IExternalSortFileDeserializer deserializer) {
    externalSortFileMap.computeIfAbsent(queryId, x -> new ArrayList<>()).add(deserializer);
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the
   * QueryDataSource needed for this query and put them in the cachedQueryDataSourcesMap.
   *
   * @param processorToSeriesMap Key: processor of the data region. Value: selected series under the
   *     data region
   */
  public void initQueryDataSourceCache(
      Map<DataRegion, List<PartialPath>> processorToSeriesMap,
      QueryContext context,
      Filter timeFilter)
      throws QueryProcessException {
    for (Map.Entry<DataRegion, List<PartialPath>> entry : processorToSeriesMap.entrySet()) {
      DataRegion processor = entry.getKey();
      List<PartialPath> pathList =
          entry.getValue().stream().map(IDTable::translateQueryPath).collect(Collectors.toList());

      // when all the selected series are under the same device, the QueryDataSource will be
      // filtered according to timeIndex
      Set<String> selectedDeviceIdSet =
          pathList.stream().map(PartialPath::getDevice).collect(Collectors.toSet());

      long queryId = context.getQueryId();
      String storageGroupPath = processor.getStorageGroupPath();

      QueryDataSource cachedQueryDataSource =
          processor.query(
              pathList,
              selectedDeviceIdSet.size() == 1 ? selectedDeviceIdSet.iterator().next() : null,
              context,
              filePathsManager,
              timeFilter);
      cachedQueryDataSourcesMap
          .computeIfAbsent(queryId, k -> new HashMap<>())
          .put(storageGroupPath, cachedQueryDataSource);
    }
  }

  /**
   * @param selectedPath MeasurementPath or AlignedPath, even if it contains only one sub sensor of
   *     an aligned device, it should be AlignedPath instead of MeasurementPath
   */
  public QueryDataSource getQueryDataSource(
      PartialPath selectedPath, QueryContext context, Filter timeFilter, boolean ascending)
      throws StorageEngineException, QueryProcessException {

    long queryId = context.getQueryId();
    String storageGroupPath = StorageEngine.getInstance().getStorageGroupPath(selectedPath);
    String deviceId = selectedPath.getDevice();

    // get cached QueryDataSource
    QueryDataSource cachedQueryDataSource;
    if (cachedQueryDataSourcesMap.containsKey(queryId)
        && cachedQueryDataSourcesMap.get(queryId).containsKey(storageGroupPath)) {
      cachedQueryDataSource = cachedQueryDataSourcesMap.get(queryId).get(storageGroupPath);
    } else {
      // QueryDataSource is never cached in cluster mode
      DataRegion processor = StorageEngine.getInstance().getProcessor(selectedPath.getDevicePath());
      PartialPath translatedPath = IDTable.translateQueryPath(selectedPath);
      cachedQueryDataSource =
          processor.query(
              Collections.singletonList(translatedPath),
              translatedPath.getDevice(),
              context,
              filePathsManager,
              timeFilter);
    }

    // construct QueryDataSource for selectedPath
    QueryDataSource queryDataSource =
        new QueryDataSource(
            cachedQueryDataSource.getSeqResources(), cachedQueryDataSource.getUnseqResources());

    queryDataSource.setDataTTL(cachedQueryDataSource.getDataTTL());

    // calculate the read order of unseqResources
    QueryUtils.fillOrderIndexes(queryDataSource, deviceId, ascending);

    return queryDataSource;
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All
   * query tokens created by this jdbc request must be cleared.
   */
  // Suppress high Cognitive Complexity warning
  public void endQuery(long queryId) throws StorageEngineException {
    // close file stream of external sort files, and delete
    if (externalSortFileMap.get(queryId) != null) {
      for (IExternalSortFileDeserializer deserializer : externalSortFileMap.get(queryId)) {
        try {
          deserializer.close();
        } catch (IOException e) {
          throw new StorageEngineException(e);
        }
      }
      externalSortFileMap.remove(queryId);
    }

    // remove usage of opened file paths of current thread
    filePathsManager.removeUsedFilesForQuery(queryId);

    // close and delete UDF temp files
    TemporaryQueryDataFileService.getInstance().deregister(queryId);

    // remove cached QueryDataSource
    cachedQueryDataSourcesMap.remove(queryId);
  }

  public void writeQueryFileInfo() {
    filePathsManager.writeQueryFileInfo();
  }

  public QueryFileManager getQueryFileManager() {
    return filePathsManager;
  }

  private static class QueryTokenManagerHelper {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();

    private QueryTokenManagerHelper() {}
  }
}
