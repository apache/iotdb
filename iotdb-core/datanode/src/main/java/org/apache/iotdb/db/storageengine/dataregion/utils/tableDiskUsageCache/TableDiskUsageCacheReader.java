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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.TableDiskUsageStatisticUtil;

import org.apache.tsfile.utils.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class TableDiskUsageCacheReader implements Closeable {

  private final DataRegion dataRegion;
  private final int regionId;
  private final Map<Long, TimePartitionTableSizeQueryContext> timePartitionQueryContexts;
  private CompletableFuture<TsFileTableSizeCacheReader> future;
  private TsFileTableSizeCacheReader cacheFileReader;

  private final Iterator<Map.Entry<Long, TimePartitionTableSizeQueryContext>> timePartitionIterator;

  private final boolean currentDatabaseOnlyHasOneTable;
  private final Optional<FragmentInstanceContext> context;
  private TableDiskUsageStatisticUtil tableDiskUsageStatisticUtil;

  private final List<Pair<TsFileID, Long>> tsFilesToQueryInCache = new ArrayList<>();
  private Iterator<Pair<TsFileID, Long>> tsFilesToQueryInCacheIterator = null;

  public TableDiskUsageCacheReader(
      DataRegion dataRegion,
      Map<Long, TimePartitionTableSizeQueryContext> resultMap,
      boolean databaseHasOnlyOneTable,
      Optional<FragmentInstanceContext> context) {
    this.dataRegion = dataRegion;
    this.regionId = Integer.parseInt(dataRegion.getDataRegionIdString());
    this.timePartitionQueryContexts = resultMap;
    this.currentDatabaseOnlyHasOneTable = databaseHasOnlyOneTable;
    this.context = context;
    this.timePartitionIterator = timePartitionQueryContexts.entrySet().iterator();
  }

  public boolean prepareCachedTsFileIDKeys(long startTime, long maxRunTime) throws Exception {
    if (this.cacheFileReader == null) {
      this.future =
          this.future == null
              ? TableDiskUsageCache.getInstance().startRead(dataRegion.getDatabaseName(), regionId)
              : future;
      do {
        try {
          if (future.isDone()) {
            this.cacheFileReader = future.get();
            this.cacheFileReader.openKeyFile();
            break;
          } else {
            Thread.sleep(1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      } while (System.nanoTime() - startTime < maxRunTime);
    }
    if (this.cacheFileReader == null) {
      return false;
    }
    return cacheFileReader.readFromKeyFile(timePartitionQueryContexts, startTime, maxRunTime);
  }

  public boolean calculateNextFile() {
    while (true) {
      if (tableDiskUsageStatisticUtil != null && tableDiskUsageStatisticUtil.hasNextFile()) {
        tableDiskUsageStatisticUtil.calculateNextFile();
        return true;
      }
      if (timePartitionIterator.hasNext()) {
        Map.Entry<Long, TimePartitionTableSizeQueryContext> currentTimePartitionEntry =
            timePartitionIterator.next();
        long timePartition = currentTimePartitionEntry.getKey();
        tableDiskUsageStatisticUtil =
            new TableDiskUsageStatisticUtil(
                dataRegion,
                timePartition,
                currentTimePartitionEntry.getValue(),
                currentDatabaseOnlyHasOneTable,
                tsFilesToQueryInCache,
                context);
      } else {
        return false;
      }
    }
  }

  public boolean readCacheValueFilesAndUpdateResultMap(long startTime, long maxRunTime)
      throws IOException {
    if (this.tsFilesToQueryInCacheIterator == null) {
      this.tsFilesToQueryInCache.sort(Comparator.comparingLong(Pair::getRight));
      this.tsFilesToQueryInCacheIterator = tsFilesToQueryInCache.iterator();
      this.cacheFileReader.openValueFile();
    }
    return cacheFileReader.readFromValueFile(
        tsFilesToQueryInCacheIterator, timePartitionQueryContexts, startTime, maxRunTime);
  }

  public DataRegion getDataRegion() {
    return dataRegion;
  }

  @Override
  public void close() throws IOException {
    if (future != null) {
      TableDiskUsageCache.getInstance().endRead(dataRegion.getDatabaseName(), regionId);
      future = null;
    }
    if (cacheFileReader != null) {
      cacheFileReader.closeCurrentFile();
      cacheFileReader = null;
    }
  }
}
