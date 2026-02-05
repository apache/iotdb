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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.object.EmptyObjectTableSizeCacheReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.object.IObjectTableSizeCacheReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableDiskUsageCacheWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile.TsFileTableSizeCacheReader;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TableDiskUsageCache {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TableDiskUsageCache.class);
  protected final BlockingQueue<Operation> queue = new LinkedBlockingQueue<>(1000);
  // regionId -> writer mapping
  protected final Map<Integer, DataRegionTableSizeCacheWriter> writerMap = new HashMap<>();
  protected ScheduledExecutorService scheduledExecutorService;
  private int processedOperationCountSinceLastPeriodicCheck = 0;
  protected volatile boolean failedToRecover = false;
  private volatile boolean stop = false;

  protected TableDiskUsageCache() {
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.FILE_TIME_INDEX_RECORD.getName());
    scheduledExecutorService.submit(this::run);
  }

  protected void run() {
    try {
      while (!stop) {
        try {
          for (DataRegionTableSizeCacheWriter writer : writerMap.values()) {
            syncTsFileTableSizeCacheIfNecessary(writer);
            persistPendingObjectDeltasIfNecessary(writer);
          }
          Operation operation = queue.poll(1, TimeUnit.SECONDS);
          if (operation != null) {
            operation.apply(this);
            processedOperationCountSinceLastPeriodicCheck++;
          }
          if (operation == null || processedOperationCountSinceLastPeriodicCheck % 1000 == 0) {
            performPeriodicMaintenance();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          LOGGER.error("Meet exception when apply TableDiskUsageCache operation.", e);
        }
      }
    } finally {
      writerMap.values().forEach(DataRegionTableSizeCacheWriter::close);
    }
  }

  private void performPeriodicMaintenance() {
    checkAndMayCloseIdleWriter();
    compactIfNecessary(TimeUnit.SECONDS.toMillis(1));
    processedOperationCountSinceLastPeriodicCheck = 0;
  }

  /**
   * Any unrecoverable error in a single writer will mark the whole TableDiskUsageCache as failed
   * and disable further operations.
   */
  protected void failedToRecover(Exception e) {
    failedToRecover = true;
    LOGGER.error("Failed to recover TableDiskUsageCache", e);
  }

  protected void syncTsFileTableSizeCacheIfNecessary(DataRegionTableSizeCacheWriter writer) {
    try {
      writer.tsFileCacheWriter.syncIfNecessary();
    } catch (IOException e) {
      LOGGER.warn("Failed to sync tsfile table size cache.", e);
    }
  }

  // Hook for subclasses to persist pending object table size deltas. No-op by default.
  protected void persistPendingObjectDeltasIfNecessary(DataRegionTableSizeCacheWriter writer) {}

  protected void compactIfNecessary(long maxRunTime) {
    if (!StorageEngine.getInstance().isReadyForReadAndWrite()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    for (DataRegionTableSizeCacheWriter writer : writerMap.values()) {
      if (System.currentTimeMillis() - startTime > maxRunTime) {
        break;
      }
      if (writer.getActiveReaderNum() > 0) {
        continue;
      }
      writer.compactIfNecessary();
    }
  }

  protected void checkAndMayCloseIdleWriter() {
    for (DataRegionTableSizeCacheWriter writer : writerMap.values()) {
      writer.closeIfIdle();
    }
  }

  public void write(String database, TsFileID tsFileID, Map<String, Long> tableSizeMap) {
    if (tableSizeMap == null || tableSizeMap.isEmpty()) {
      // tree model
      return;
    }
    addOperationToQueue(new WriteOperation(database, tsFileID, tableSizeMap));
  }

  public void write(String database, TsFileID originTsFileID, TsFileID newTsFileID) {
    addOperationToQueue(new ReplaceTsFileOperation(database, originTsFileID, newTsFileID));
  }

  public void writeObjectDelta(
      String database, int regionId, long timePartition, String table, long size, int num) {
    throw new UnsupportedOperationException("writeObjectDelta");
  }

  public CompletableFuture<Pair<TsFileTableSizeCacheReader, IObjectTableSizeCacheReader>> startRead(
      DataRegion dataRegion, boolean readTsFileCache, boolean readObjectFileCache) {
    StartReadOperation operation =
        new StartReadOperation(dataRegion, readTsFileCache, readObjectFileCache);
    if (!addOperationToQueue(operation)) {
      operation.future.complete(
          new Pair<>(
              new TsFileTableSizeCacheReader(
                  0, null, 0, null, dataRegion.getDataRegionId().getId()),
              new EmptyObjectTableSizeCacheReader()));
    }
    return operation.future;
  }

  public void endRead(DataRegion dataRegion) {
    EndReadOperation operation = new EndReadOperation(dataRegion);
    addOperationToQueue(operation);
  }

  public void registerRegion(DataRegion region) {
    RegisterRegionOperation operation = new RegisterRegionOperation(region);
    if (!region.isTableModel()) {
      return;
    }
    addOperationToQueue(operation);
  }

  public void remove(String database, int regionId) {
    RemoveRegionOperation operation = new RemoveRegionOperation(database, regionId);
    if (!addOperationToQueue(operation)) {
      return;
    }
    try {
      operation.future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOGGER.error("Meet exception when remove TableDiskUsageCache.", e);
    }
  }

  protected boolean addOperationToQueue(Operation operation) {
    if (failedToRecover || stop) {
      return false;
    }
    try {
      queue.put(operation);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  public int getQueueSize() {
    return queue.size();
  }

  public void close() {
    if (scheduledExecutorService == null) {
      return;
    }
    try {
      stop = true;
      scheduledExecutorService.shutdown();
      scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
      writerMap.values().forEach(DataRegionTableSizeCacheWriter::close);
      writerMap.clear();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @TestOnly
  public void ensureRunning() {
    stop = false;
    failedToRecover = false;
    if (scheduledExecutorService.isTerminated()) {
      scheduledExecutorService =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.FILE_TIME_INDEX_RECORD.getName());
      scheduledExecutorService.submit(this::run);
    }
  }

  protected DataRegionTableSizeCacheWriter createWriter(
      String database, int regionId, DataRegion region) {
    return new DataRegionTableSizeCacheWriter(database, regionId, region);
  }

  protected TsFileTableSizeCacheReader createTsFileCacheReader(
      DataRegionTableSizeCacheWriter dataRegionWriter, int regionId) {
    TsFileTableDiskUsageCacheWriter tsFileCacheWriter = dataRegionWriter.tsFileCacheWriter;
    return new TsFileTableSizeCacheReader(
        tsFileCacheWriter.keyFileLength(),
        tsFileCacheWriter.getKeyFile(),
        tsFileCacheWriter.valueFileLength(),
        tsFileCacheWriter.getValueFile(),
        regionId);
  }

  protected IObjectTableSizeCacheReader createObjectFileCacheReader(
      DataRegionTableSizeCacheWriter dataRegionWriter, int regionId) {
    return new EmptyObjectTableSizeCacheReader();
  }

  protected abstract static class Operation {
    protected final String database;
    protected final int regionId;

    protected Operation(String database, int regionId) {
      this.database = database;
      this.regionId = regionId;
    }

    public int getRegionId() {
      return regionId;
    }

    public String getDatabase() {
      return database;
    }

    public abstract void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException;
  }

  protected static class StartReadOperation extends Operation {
    protected final DataRegion region;
    protected final boolean readTsFileCache;
    protected final boolean readObjectFileCache;
    public CompletableFuture<Pair<TsFileTableSizeCacheReader, IObjectTableSizeCacheReader>> future =
        new CompletableFuture<>();

    public StartReadOperation(
        DataRegion dataRegion, boolean readTsFileCache, boolean readObjectFileCache) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId().getId());
      this.region = dataRegion;
      this.readTsFileCache = readTsFileCache;
      this.readObjectFileCache = readObjectFileCache;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      DataRegionTableSizeCacheWriter writer = tableDiskUsageCache.writerMap.get(regionId);
      try {
        if (writer == null || writer.getRemovedFuture() != null) {
          // region is removing or removed
          future.complete(
              new Pair<>(
                  new TsFileTableSizeCacheReader(0, null, 0, null, regionId),
                  new EmptyObjectTableSizeCacheReader()));
          return;
        }
        writer.increaseActiveReaderNum();
        // Flush buffered writes to ensure readers observe a consistent snapshot
        writer.flush();
        TsFileTableSizeCacheReader tsFileTableSizeCacheReader =
            readTsFileCache ? tableDiskUsageCache.createTsFileCacheReader(writer, regionId) : null;
        IObjectTableSizeCacheReader objectTableSizeCacheReader =
            readObjectFileCache
                ? tableDiskUsageCache.createObjectFileCacheReader(writer, regionId)
                : null;
        future.complete(new Pair<>(tsFileTableSizeCacheReader, objectTableSizeCacheReader));
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    }
  }

  private static class EndReadOperation extends Operation {
    protected final DataRegion region;

    public EndReadOperation(DataRegion dataRegion) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId().getId());
      this.region = dataRegion;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      tableDiskUsageCache.writerMap.computeIfPresent(
          regionId,
          (k, writer) -> {
            if (writer.dataRegion != region) {
              return writer;
            }
            writer.decreaseActiveReaderNum();
            // Complete pending remove when the last reader exits
            if (writer.getRemovedFuture() != null) {
              writer.close();
              writer.getRemovedFuture().complete(null);
              writer.setRemovedFuture(null);
              return null;
            }
            return writer;
          });
    }
  }

  private static class WriteOperation extends Operation {

    private final TsFileID tsFileID;
    private final Map<String, Long> tableSizeMap;

    protected WriteOperation(String database, TsFileID tsFileID, Map<String, Long> tableSizeMap) {
      super(database, tsFileID.regionId);
      this.tsFileID = tsFileID;
      this.tableSizeMap = tableSizeMap;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      DataRegionTableSizeCacheWriter dataRegionTableSizeCacheWriter =
          tableDiskUsageCache.writerMap.get(regionId);
      if (dataRegionTableSizeCacheWriter != null) {
        dataRegionTableSizeCacheWriter.tsFileCacheWriter.write(tsFileID, tableSizeMap);
      }
    }
  }

  private static class ReplaceTsFileOperation extends Operation {
    private final TsFileID originTsFileID;
    private final TsFileID newTsFileID;

    public ReplaceTsFileOperation(String database, TsFileID originTsFileID, TsFileID newTsFileID) {
      super(database, originTsFileID.regionId);
      this.originTsFileID = originTsFileID;
      this.newTsFileID = newTsFileID;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      DataRegionTableSizeCacheWriter writer = tableDiskUsageCache.writerMap.get(regionId);
      if (writer != null) {
        writer.tsFileCacheWriter.write(originTsFileID, newTsFileID);
      }
    }
  }

  protected static class RegisterRegionOperation extends Operation {

    protected final DataRegion dataRegion;
    protected final CompletableFuture<Void> future = new CompletableFuture<>();

    public RegisterRegionOperation(DataRegion dataRegion) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId().getId());
      this.dataRegion = dataRegion;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) {
      tableDiskUsageCache.writerMap.computeIfAbsent(
          regionId, regionId -> tableDiskUsageCache.createWriter(database, regionId, dataRegion));
      future.complete(null);
    }

    public CompletableFuture<Void> getFuture() {
      return future;
    }
  }

  private static class RemoveRegionOperation extends Operation {

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    private RemoveRegionOperation(String database, int regionId) {
      super(database, regionId);
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) {
      tableDiskUsageCache.writerMap.computeIfPresent(
          regionId,
          (k, writer) -> {
            if (writer.getActiveReaderNum() > 0) {
              // If there are active readers, defer removal until all readers finish
              writer.setRemovedFuture(future);
              return writer;
            }
            writer.close();
            future.complete(null);
            return null;
          });
    }
  }

  public static TableDiskUsageCache getInstance() {
    return TableDiskUsageCache.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final TableDiskUsageCache INSTANCE = loadInstance();

    private static TableDiskUsageCache loadInstance() {
      ServiceLoader<TableDiskUsageCacheProvider> loader =
          ServiceLoader.load(TableDiskUsageCacheProvider.class);
      for (TableDiskUsageCacheProvider provider : loader) {
        return provider.create();
      }
      return new DefaultTableDiskUsageCacheProvider().create();
    }
  }

  protected static class DataRegionTableSizeCacheWriter {
    protected final DataRegion dataRegion;
    protected final TsFileTableDiskUsageCacheWriter tsFileCacheWriter;
    protected int activeReaderNum = 0;
    protected CompletableFuture<Void> removedFuture;

    protected DataRegionTableSizeCacheWriter(String database, int regionId, DataRegion dataRegion) {
      this.dataRegion = dataRegion;
      this.tsFileCacheWriter = new TsFileTableDiskUsageCacheWriter(database, regionId);
    }

    public void increaseActiveReaderNum() {
      activeReaderNum++;
    }

    public void decreaseActiveReaderNum() {
      if (activeReaderNum > 0) {
        activeReaderNum--;
      }
    }

    public int getActiveReaderNum() {
      return activeReaderNum;
    }

    public void compactIfNecessary() {
      if (tsFileCacheWriter.needCompact()) {
        tsFileCacheWriter.compact();
      }
    }

    public void closeIfIdle() {
      tsFileCacheWriter.closeIfIdle();
    }

    public void flush() throws IOException {
      tsFileCacheWriter.flush();
    }

    public void setRemovedFuture(CompletableFuture<Void> removedFuture) {
      this.removedFuture = removedFuture;
    }

    public CompletableFuture<Void> getRemovedFuture() {
      return removedFuture;
    }

    public void close() {
      tsFileCacheWriter.close();
    }
  }
}
