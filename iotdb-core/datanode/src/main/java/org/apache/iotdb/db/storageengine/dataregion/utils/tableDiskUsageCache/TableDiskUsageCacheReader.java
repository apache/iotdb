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

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.TableDiskUsageStatisticUtil;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.object.IObjectTableSizeCacheReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableSizeCacheReader;

import org.apache.tsfile.utils.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TableDiskUsageCacheReader implements Closeable {

  private final DataRegion dataRegion;
  private final DataRegionTableSizeQueryContext dataRegionContext;

  private CompletableFuture<Pair<TsFileTableSizeCacheReader, IObjectTableSizeCacheReader>>
      prepareReaderFuture;
  private TsFileTableSizeCacheReader tsFileTableSizeCacheReader;
  private IObjectTableSizeCacheReader objectTableSizeCacheReader;

  private boolean objectFileSizeLoaded = false;
  private boolean tsFileIdKeysPrepared = false;
  private boolean allTsFileResourceChecked = false;

  private final Iterator<Map.Entry<Long, TimePartitionTableSizeQueryContext>> timePartitionIterator;

  private final boolean currentDatabaseOnlyHasOneTable;
  private TableDiskUsageStatisticUtil tableDiskUsageStatisticUtil;

  private final List<Pair<TsFileID, Long>> tsFilesToQueryInCache = new ArrayList<>();
  private Iterator<Pair<TsFileID, Long>> tsFilesToQueryInCacheIterator = null;

  public TableDiskUsageCacheReader(
      DataRegion dataRegion,
      DataRegionTableSizeQueryContext dataRegionContext,
      boolean databaseHasOnlyOneTable) {
    this.dataRegion = dataRegion;
    this.dataRegionContext = dataRegionContext;
    this.currentDatabaseOnlyHasOneTable = databaseHasOnlyOneTable;
    if (dataRegionContext.isNeedAllData()) {
      dataRegionContext.addAllTimePartitionsInTsFileManager(dataRegion.getTsFileManager());
    }
    this.timePartitionIterator =
        dataRegionContext.getTimePartitionTableSizeQueryContextMap().entrySet().iterator();
    dataRegionContext.reserveMemoryForResultMap();
  }

  public boolean prepareCacheReader(long startTime, long maxRunTime) throws Exception {
    if (this.tsFileTableSizeCacheReader == null) {
      this.prepareReaderFuture =
          this.prepareReaderFuture == null
              ? TableDiskUsageCache.getInstance().startRead(dataRegion, true, true)
              : this.prepareReaderFuture;
      do {
        if (prepareReaderFuture.isDone()) {
          Pair<TsFileTableSizeCacheReader, IObjectTableSizeCacheReader> readerPair =
              prepareReaderFuture.get();
          this.tsFileTableSizeCacheReader = readerPair.left;
          this.tsFileTableSizeCacheReader.openKeyFile();
          this.objectTableSizeCacheReader = readerPair.right;
          break;
        } else {
          Thread.sleep(1);
        }
      } while (System.nanoTime() - startTime < maxRunTime);
    }
    return this.tsFileTableSizeCacheReader != null;
  }

  public boolean loadObjectFileTableSizeCache(long startTime, long maxRunTime) throws Exception {
    if (objectFileSizeLoaded) {
      return true;
    }
    if (objectTableSizeCacheReader.loadObjectFileTableSize(
        dataRegionContext, startTime, maxRunTime)) {
      closeObjectFileTableSizeCacheReader();
      objectFileSizeLoaded = true;
      return true;
    }
    return false;
  }

  public boolean prepareCachedTsFileIDKeys(long startTime, long maxRunTime) throws Exception {
    if (tsFileIdKeysPrepared) {
      return true;
    }
    if (tsFileTableSizeCacheReader.readFromKeyFile(dataRegionContext, startTime, maxRunTime)) {
      dataRegionContext.reserveMemoryForTsFileIDs();
      tsFileIdKeysPrepared = true;
      return true;
    }
    return false;
  }

  public boolean checkAllFilesInTsFileManager(long start, long maxRunTime) {
    if (allTsFileResourceChecked) {
      return true;
    }
    do {
      if (!calculateNextFile()) {
        allTsFileResourceChecked = true;
        break;
      }
    } while (System.nanoTime() - start < maxRunTime);
    return allTsFileResourceChecked;
  }

  private boolean calculateNextFile() {
    while (true) {
      if (tableDiskUsageStatisticUtil != null && tableDiskUsageStatisticUtil.hasNextFile()) {
        tableDiskUsageStatisticUtil.calculateNextFile();
        return true;
      }
      if (timePartitionIterator.hasNext()) {
        Map.Entry<Long, TimePartitionTableSizeQueryContext> currentTimePartitionEntry =
            timePartitionIterator.next();
        long timePartition = currentTimePartitionEntry.getKey();
        closeTableDiskUsageStatisticUtil();
        tableDiskUsageStatisticUtil =
            new TableDiskUsageStatisticUtil(
                dataRegion,
                timePartition,
                currentTimePartitionEntry.getValue(),
                dataRegionContext.isNeedAllData(),
                currentDatabaseOnlyHasOneTable,
                tsFilesToQueryInCache,
                dataRegionContext.getFragmentInstanceContext());
      } else {
        closeTableDiskUsageStatisticUtil();
        return false;
      }
    }
  }

  public boolean readCacheValueFilesAndUpdateResultMap(long startTime, long maxRunTime)
      throws IOException {
    if (this.tsFilesToQueryInCacheIterator == null) {
      this.tsFilesToQueryInCache.sort(Comparator.comparingLong(Pair::getRight));
      this.tsFilesToQueryInCacheIterator = tsFilesToQueryInCache.iterator();
      this.tsFileTableSizeCacheReader.openValueFile();
    }
    return tsFileTableSizeCacheReader.readFromValueFile(
        tsFilesToQueryInCacheIterator, dataRegionContext, startTime, maxRunTime);
  }

  public DataRegion getDataRegion() {
    return dataRegion;
  }

  @Override
  public void close() throws IOException {
    closeTableDiskUsageStatisticUtil();
    closeTsFileTableSizeCacheReader();
    closeObjectFileTableSizeCacheReader();
    if (prepareReaderFuture != null) {
      TableDiskUsageCache.getInstance().endRead(dataRegion);
      prepareReaderFuture = null;
    }
    dataRegionContext.releaseMemory();
  }

  private void closeTableDiskUsageStatisticUtil() {
    if (tableDiskUsageStatisticUtil != null) {
      tableDiskUsageStatisticUtil.close();
      tableDiskUsageStatisticUtil = null;
    }
  }

  private void closeTsFileTableSizeCacheReader() {
    if (tsFileTableSizeCacheReader != null) {
      tsFileTableSizeCacheReader.closeCurrentFile();
    }
  }

  private void closeObjectFileTableSizeCacheReader() {
    if (objectTableSizeCacheReader != null) {
      objectTableSizeCacheReader.close();
      objectTableSizeCacheReader = null;
    }
  }
}
