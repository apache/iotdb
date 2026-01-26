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
  protected final BlockingQueue<Operation> queue = new LinkedBlockingQueue<>();
  protected final Map<Integer, DataRegionTableSizeCacheWriter> writerMap = new HashMap<>();
  protected final ScheduledExecutorService scheduledExecutorService;
  private int counter = 0;
  protected volatile boolean failedToRecover = false;

  protected TableDiskUsageCache() {
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.FILE_TIME_INDEX_RECORD.getName());
    scheduledExecutorService.submit(this::run);
  }

  protected void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          checkAndMaySyncObjectDeltaToFile();
          Operation operation = queue.poll(1, TimeUnit.SECONDS);
          if (operation != null) {
            operation.apply(this);
            counter++;
          }
          if (operation == null || counter % 1000 == 0) {
            timedCheck();
          }
        } catch (InterruptedException e) {
          return;
        } catch (Exception e) {
          LOGGER.error("Meet exception when apply TableDiskUsageCache operation.", e);
        }
      }
    } finally {
      writerMap.values().forEach(DataRegionTableSizeCacheWriter::close);
    }
  }

  private void timedCheck() {
    checkAndMayCloseIdleWriter();
    checkAndMayCompact(TimeUnit.SECONDS.toMillis(1));
    counter = 0;
  }

  /**
   * Any unrecoverable error in a single writer will mark the whole TableDiskUsageCache as failed
   * and disable further operations.
   */
  protected void failedToRecover(Exception e) {
    failedToRecover = true;
    LOGGER.error("Failed to recover TableDiskUsageCache", e);
  }

  protected void checkAndMaySyncObjectDeltaToFile() {}

  protected void checkAndMayCompact(long maxRunTime) {
    long startTime = System.currentTimeMillis();
    for (DataRegionTableSizeCacheWriter writer : writerMap.values()) {
      if (System.currentTimeMillis() - startTime > maxRunTime) {
        break;
      }
      if (writer.getActiveReaderNum() > 0) {
        continue;
      }
      if (writer.tsFileCacheWriter.needCompact()) {
        writer.tsFileCacheWriter.compact();
      }
    }
  }

  protected void checkAndMayCloseIdleWriter() {
    for (DataRegionTableSizeCacheWriter writer : writerMap.values()) {
      writer.closeIfIdle();
    }
  }

  public void write(String database, TsFileID tsFileID, Map<String, Long> tableSizeMap) {
    if (tableSizeMap == null || tableSizeMap.isEmpty()) {
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
      String database, int regionId, boolean readTsFileCache, boolean readObjectFileCache) {
    StartReadOperation operation =
        new StartReadOperation(database, regionId, readTsFileCache, readObjectFileCache);
    addOperationToQueue(operation);
    return operation.future;
  }

  public void endRead(String database, int regionId) {
    EndReadOperation operation = new EndReadOperation(database, regionId);
    addOperationToQueue(operation);
  }

  public void registerRegion(String database, int regionId) {
    RegisterRegionOperation operation = new RegisterRegionOperation(database, regionId);
    addOperationToQueue(operation);
  }

  public void remove(String database, int regionId) {
    RemoveRegionOperation operation = new RemoveRegionOperation(database, regionId);
    addOperationToQueue(operation);
    try {
      operation.future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception ignored) {
    }
  }

  protected void addOperationToQueue(Operation operation) {
    if (failedToRecover) {
      return;
    }
    queue.add(operation);
  }

  public void close() {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    writerMap.values().forEach(DataRegionTableSizeCacheWriter::close);
  }

  protected DataRegionTableSizeCacheWriter createWriter(String database, int regionId) {
    return new DataRegionTableSizeCacheWriter(database, regionId);
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
    protected final boolean readTsFileCache;
    protected final boolean readObjectFileCache;
    public CompletableFuture<Pair<TsFileTableSizeCacheReader, IObjectTableSizeCacheReader>> future =
        new CompletableFuture<>();

    public StartReadOperation(
        String database, int regionId, boolean readTsFileCache, boolean readObjectFileCache) {
      super(database, regionId);
      this.readTsFileCache = readTsFileCache;
      this.readObjectFileCache = readObjectFileCache;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      try {
        DataRegionTableSizeCacheWriter writer =
            tableDiskUsageCache.writerMap.computeIfAbsent(
                regionId, k -> tableDiskUsageCache.createWriter(database, regionId));
        // It is safe to always increase activeReaderNum here. Before a DataRegion is removed, it is
        // first marked as deleted, and all table size queries will skip DataRegions that are
        // already marked deleted.
        // Under this guarantee, waiting for activeReaderNum to reach zero will not be blocked by
        // newly created readers, and the region can be safely removed.
        writer.increaseActiveReaderNum();
        if (writer.getRemovedFuture() != null) {
          // region is removed
          future.complete(
              new Pair<>(
                  new TsFileTableSizeCacheReader(
                      0,
                      writer.tsFileCacheWriter.getKeyFile(),
                      0,
                      writer.tsFileCacheWriter.getValueFile(),
                      regionId),
                  new EmptyObjectTableSizeCacheReader()));
          return;
        }
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
    public EndReadOperation(String database, int regionId) {
      super(database, regionId);
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      tableDiskUsageCache.writerMap.computeIfPresent(
          regionId,
          (k, writer) -> {
            writer.decreaseActiveReaderNum();
            if (writer.getRemovedFuture() != null) {
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
      tableDiskUsageCache
          .writerMap
          .computeIfAbsent(
              regionId, k -> tableDiskUsageCache.createWriter(database, tsFileID.regionId))
          .tsFileCacheWriter
          .write(tsFileID, tableSizeMap);
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

    protected final CompletableFuture<Void> future = new CompletableFuture<>();

    public RegisterRegionOperation(String database, int regionId) {
      super(database, regionId);
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) {
      tableDiskUsageCache.writerMap.computeIfAbsent(
          regionId, regionId -> tableDiskUsageCache.createWriter(database, regionId));
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
            writer.close();
            if (writer.getActiveReaderNum() > 0) {
              writer.setRemovedFuture(future);
              return writer;
            }
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
    protected final TsFileTableDiskUsageCacheWriter tsFileCacheWriter;
    protected int activeReaderNum = 0;
    protected CompletableFuture<Void> removedFuture;

    protected DataRegionTableSizeCacheWriter(String database, int regionId) {
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
