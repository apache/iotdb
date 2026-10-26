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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex;

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.TableDiskUsageStatisticUtil;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object.IObjectTableSizeIndexReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.tsfile.TsFileTableSizeIndexReader;

import org.apache.tsfile.utils.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TableDiskUsageIndexReader implements Closeable {

  private final DataRegion dataRegion;
  private final DataRegionTableSizeQueryContext dataRegionContext;

  private CompletableFuture<Pair<TsFileTableSizeIndexReader, IObjectTableSizeIndexReader>>
      prepareReaderFuture;
  private TsFileTableSizeIndexReader tsFileTableSizeIndexReader;
  private IObjectTableSizeIndexReader objectTableSizeIndexReader;

  private boolean objectFileSizeLoaded = false;
  private boolean tsFileIdKeysPrepared = false;
  private boolean allTsFileResourceChecked = false;

  private final Iterator<Map.Entry<Long, TimePartitionTableSizeQueryContext>> timePartitionIterator;

  private final boolean currentDatabaseOnlyHasOneTable;
  private TableDiskUsageStatisticUtil tableDiskUsageStatisticUtil;

  private final List<Pair<TsFileID, Long>> tsFilesToQueryInIndex = new ArrayList<>();
  private Iterator<Pair<TsFileID, Long>> iteratorOfTsFilesToQueryInIndex = null;

  public TableDiskUsageIndexReader(
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

  public boolean prepareIndexReader(long startTime, long maxRunTime) throws Exception {
    if (this.tsFileTableSizeIndexReader == null) {
      this.prepareReaderFuture =
          this.prepareReaderFuture == null
              ? TableDiskUsageIndex.getInstance().startRead(dataRegion, true, true)
              : this.prepareReaderFuture;
      // Ensure we perform at least one check on the future even if the remaining
      // runtime is already exhausted.
      long waitTime = Math.max(0, maxRunTime - (System.nanoTime() - startTime));
      try {
        Pair<TsFileTableSizeIndexReader, IObjectTableSizeIndexReader> readerPair =
            prepareReaderFuture.get(waitTime, TimeUnit.NANOSECONDS);
        this.tsFileTableSizeIndexReader = readerPair.left;
        this.tsFileTableSizeIndexReader.openKeyFile();
        this.objectTableSizeIndexReader = readerPair.right;
      } catch (TimeoutException timeoutException) {
        return false;
      }
    }
    return true;
  }

  public boolean loadObjectFileTableSizeIndex(long startTime, long maxRunTime) throws Exception {
    if (objectFileSizeLoaded) {
      return true;
    }
    if (objectTableSizeIndexReader.loadObjectFileTableSize(
        dataRegionContext, startTime, maxRunTime)) {
      closeObjectFileTableSizeIndexReader();
      objectFileSizeLoaded = true;
      return true;
    }
    return false;
  }

  public boolean prepareIndexTsFileIDKeys(long startTime, long maxRunTime) throws Exception {
    if (tsFileIdKeysPrepared) {
      return true;
    }
    if (tsFileTableSizeIndexReader.readFromKeyFile(dataRegionContext, startTime, maxRunTime)) {
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
                tsFilesToQueryInIndex,
                dataRegionContext.getFragmentInstanceContext());
      } else {
        closeTableDiskUsageStatisticUtil();
        return false;
      }
    }
  }

  public boolean readIndexValueFilesAndUpdateResultMap(long startTime, long maxRunTime)
      throws IOException {
    if (this.iteratorOfTsFilesToQueryInIndex == null) {
      this.tsFilesToQueryInIndex.sort(Comparator.comparingLong(Pair::getRight));
      this.iteratorOfTsFilesToQueryInIndex = tsFilesToQueryInIndex.iterator();
      this.tsFileTableSizeIndexReader.openValueFile();
    }
    return tsFileTableSizeIndexReader.readFromValueFile(
        iteratorOfTsFilesToQueryInIndex, dataRegionContext, startTime, maxRunTime);
  }

  public DataRegion getDataRegion() {
    return dataRegion;
  }

  @Override
  public void close() throws IOException {
    closeTableDiskUsageStatisticUtil();
    closeTsFileTableSizeIndexReader();
    closeObjectFileTableSizeIndexReader();
    if (prepareReaderFuture != null) {
      TableDiskUsageIndex.getInstance().endRead(dataRegion);
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

  private void closeTsFileTableSizeIndexReader() {
    if (tsFileTableSizeIndexReader != null) {
      tsFileTableSizeIndexReader.closeCurrentFile();
    }
  }

  private void closeObjectFileTableSizeIndexReader() {
    if (objectTableSizeIndexReader != null) {
      objectTableSizeIndexReader.close();
      objectTableSizeIndexReader = null;
    }
  }
}
