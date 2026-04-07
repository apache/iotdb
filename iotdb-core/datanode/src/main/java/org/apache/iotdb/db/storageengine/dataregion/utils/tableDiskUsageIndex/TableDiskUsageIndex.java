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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object.EmptyObjectTableSizeIndexReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.object.IObjectTableSizeIndexReader;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.tsfile.TsFileTableDiskUsageIndexWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.tsfile.TsFileTableSizeIndexReader;

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

public class TableDiskUsageIndex {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TableDiskUsageIndex.class);
  protected final BlockingQueue<Operation> queue = new LinkedBlockingQueue<>(1000);
  // regionId -> writer mapping
  protected final Map<Integer, DataRegionTableSizeIndexWriter> writerMap = new HashMap<>();
  protected ScheduledExecutorService scheduledExecutorService;
  private int processedOperationCountSinceLastPeriodicCheck = 0;
  protected volatile boolean failedToRecover = false;
  private volatile boolean stop = false;

  protected TableDiskUsageIndex() {
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.TABLE_SIZE_INDEX_RECORD.getName());
    scheduledExecutorService.submit(this::run);
  }

  protected void run() {
    try {
      while (!stop) {
        try {
          for (DataRegionTableSizeIndexWriter writer : writerMap.values()) {
            syncTsFileTableSizeIndexIfNecessary(writer);
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
          // Ignore unexpected interruptions to guarantee the worker thread continues running.
          // This worker relies on the `stop` flag to terminate gracefully. Therefore, if an
          // interruption occurs while TableDiskUsageIndex is still active, we log the event and
          // keep processing subsequent operations.
          if (!stop) {
            LOGGER.warn(
                "TableDiskUsageIndex worker thread was interrupted unexpectedly while waiting for operations.",
                e);
          }
        } catch (Exception e) {
          LOGGER.error("Meet exception when apply TableDiskUsageIndex operation.", e);
        }
      }
    } finally {
      writerMap.values().forEach(DataRegionTableSizeIndexWriter::close);
    }
  }

  private void performPeriodicMaintenance() {
    checkAndMayCloseIdleWriter();
    compactIfNecessary(TimeUnit.SECONDS.toMillis(1));
    processedOperationCountSinceLastPeriodicCheck = 0;
  }

  /**
   * Any unrecoverable error in a single writer will mark the whole TableDiskUsageIndex as failed
   * and disable further operations.
   */
  protected void failedToRecover(Exception e) {
    failedToRecover = true;
    LOGGER.error("Failed to recover TableDiskUsageIndex", e);
  }

  protected void syncTsFileTableSizeIndexIfNecessary(DataRegionTableSizeIndexWriter writer) {
    try {
      writer.tsFileIndexWriter.syncIfNecessary();
    } catch (IOException e) {
      LOGGER.warn("Failed to sync tsfile table size index.", e);
    }
  }

  // Hook for subclasses to persist pending object table size deltas. No-op by default.
  protected void persistPendingObjectDeltasIfNecessary(DataRegionTableSizeIndexWriter writer) {}

  protected void compactIfNecessary(long maxRunTime) {
    if (!StorageEngine.getInstance().isReadyForReadAndWrite()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    for (DataRegionTableSizeIndexWriter writer : writerMap.values()) {
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
    for (DataRegionTableSizeIndexWriter writer : writerMap.values()) {
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

  public CompletableFuture<Pair<TsFileTableSizeIndexReader, IObjectTableSizeIndexReader>> startRead(
      DataRegion dataRegion, boolean readTsFileIndex, boolean readObjectFileIndex) {
    StartReadOperation operation =
        new StartReadOperation(dataRegion, readTsFileIndex, readObjectFileIndex);
    if (!addOperationToQueue(operation)) {
      operation.future.complete(
          new Pair<>(
              new TsFileTableSizeIndexReader(0, null, 0, null, dataRegion.getDataRegionId()),
              new EmptyObjectTableSizeIndexReader()));
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
      LOGGER.error("Meet exception when remove TableDiskUsageIndex.", e);
    }
  }

  protected boolean addOperationToQueue(Operation operation) {
    if (failedToRecover) {
      return false;
    }
    if (stop) {
      LOGGER.warn(
          "Skip adding operation {} to queue because TableDiskUsageIndex has been stopped.",
          operation);
      return false;
    }
    try {
      queue.put(operation);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while adding operation {} to queue.", operation, e);
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
      writerMap.values().forEach(DataRegionTableSizeIndexWriter::close);
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

  protected DataRegionTableSizeIndexWriter createWriter(
      String database, int regionId, DataRegion region) {
    return new DataRegionTableSizeIndexWriter(database, regionId, region);
  }

  protected TsFileTableSizeIndexReader createTsFileIndexReader(
      DataRegionTableSizeIndexWriter dataRegionWriter, int regionId) {
    TsFileTableDiskUsageIndexWriter tsFileIndexWriter = dataRegionWriter.tsFileIndexWriter;
    return new TsFileTableSizeIndexReader(
        tsFileIndexWriter.keyFileLength(),
        tsFileIndexWriter.getKeyFile(),
        tsFileIndexWriter.valueFileLength(),
        tsFileIndexWriter.getValueFile(),
        regionId);
  }

  protected IObjectTableSizeIndexReader createObjectFileIndexReader(
      DataRegionTableSizeIndexWriter dataRegionWriter, int regionId) {
    return new EmptyObjectTableSizeIndexReader();
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

    public abstract void apply(TableDiskUsageIndex tableDiskUsageIndex) throws IOException;

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  protected static class StartReadOperation extends Operation {
    protected final DataRegion region;
    protected final boolean readTsFileIndex;
    protected final boolean readObjectFileIndex;
    private final CompletableFuture<Pair<TsFileTableSizeIndexReader, IObjectTableSizeIndexReader>>
        future = new CompletableFuture<>();

    public StartReadOperation(
        DataRegion dataRegion, boolean readTsFileIndex, boolean readObjectFileIndex) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId());
      this.region = dataRegion;
      this.readTsFileIndex = readTsFileIndex;
      this.readObjectFileIndex = readObjectFileIndex;
    }

    @Override
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) throws IOException {
      DataRegionTableSizeIndexWriter writer = tableDiskUsageIndex.writerMap.get(regionId);
      try {
        if (writer == null || writer.getRemovedFuture() != null) {
          // region is removing or removed
          future.complete(
              new Pair<>(
                  new TsFileTableSizeIndexReader(0, null, 0, null, regionId),
                  new EmptyObjectTableSizeIndexReader()));
          return;
        }
        writer.increaseActiveReaderNum();
        // Flush buffered writes to ensure readers observe a consistent snapshot
        writer.flush();
        TsFileTableSizeIndexReader tsFileTableSizeIndexReader =
            readTsFileIndex ? tableDiskUsageIndex.createTsFileIndexReader(writer, regionId) : null;
        IObjectTableSizeIndexReader objectTableSizeIndexReader =
            readObjectFileIndex
                ? tableDiskUsageIndex.createObjectFileIndexReader(writer, regionId)
                : null;
        future.complete(new Pair<>(tsFileTableSizeIndexReader, objectTableSizeIndexReader));
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    }
  }

  private static class EndReadOperation extends Operation {
    protected final DataRegion region;

    public EndReadOperation(DataRegion dataRegion) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId());
      this.region = dataRegion;
    }

    @Override
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) throws IOException {
      tableDiskUsageIndex.writerMap.computeIfPresent(
          regionId,
          (k, writer) -> {
            if (writer.dataRegion != region) {
              return writer;
            }
            writer.decreaseActiveReaderNum();
            // Complete pending remove when the last reader exits
            if (writer.getActiveReaderNum() == 0 && writer.getRemovedFuture() != null) {
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
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) throws IOException {
      DataRegionTableSizeIndexWriter dataRegionTableSizeIndexWriter =
          tableDiskUsageIndex.writerMap.get(regionId);
      if (dataRegionTableSizeIndexWriter != null) {
        dataRegionTableSizeIndexWriter.tsFileIndexWriter.write(tsFileID, tableSizeMap);
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
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) throws IOException {
      DataRegionTableSizeIndexWriter writer = tableDiskUsageIndex.writerMap.get(regionId);
      if (writer != null) {
        writer.tsFileIndexWriter.write(originTsFileID, newTsFileID);
      }
    }
  }

  protected static class RegisterRegionOperation extends Operation {

    protected final DataRegion dataRegion;
    protected final CompletableFuture<Void> future = new CompletableFuture<>();

    public RegisterRegionOperation(DataRegion dataRegion) {
      super(dataRegion.getDatabaseName(), dataRegion.getDataRegionId());
      this.dataRegion = dataRegion;
    }

    @Override
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) {
      tableDiskUsageIndex.writerMap.computeIfAbsent(
          regionId, regionId -> tableDiskUsageIndex.createWriter(database, regionId, dataRegion));
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
    public void apply(TableDiskUsageIndex tableDiskUsageIndex) {
      tableDiskUsageIndex.writerMap.computeIfPresent(
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

  public static TableDiskUsageIndex getInstance() {
    return TableDiskUsageIndex.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final TableDiskUsageIndex INSTANCE = loadInstance();

    private static TableDiskUsageIndex loadInstance() {
      ServiceLoader<TableDiskUsageIndexProvider> loader =
          ServiceLoader.load(TableDiskUsageIndexProvider.class);
      for (TableDiskUsageIndexProvider provider : loader) {
        return provider.create();
      }
      return new DefaultTableDiskUsageIndexProvider().create();
    }
  }

  protected static class DataRegionTableSizeIndexWriter {
    protected final DataRegion dataRegion;
    protected final TsFileTableDiskUsageIndexWriter tsFileIndexWriter;
    protected int activeReaderNum = 0;
    protected CompletableFuture<Void> removedFuture;

    protected DataRegionTableSizeIndexWriter(String database, int regionId, DataRegion dataRegion) {
      this.dataRegion = dataRegion;
      this.tsFileIndexWriter = new TsFileTableDiskUsageIndexWriter(database, regionId);
    }

    public void increaseActiveReaderNum() {
      activeReaderNum++;
    }

    public void decreaseActiveReaderNum() {
      if (activeReaderNum > 0) {
        activeReaderNum--;
      } else {
        LOGGER.warn(
            "Attempt to decrease activeReaderNum when it is already 0. This may indicate an incorrect reader lifecycle management.");
      }
    }

    public int getActiveReaderNum() {
      return activeReaderNum;
    }

    public void compactIfNecessary() {
      if (tsFileIndexWriter.needCompact()) {
        tsFileIndexWriter.compact();
      }
    }

    public void closeIfIdle() {
      tsFileIndexWriter.closeIfIdle();
    }

    public void flush() throws IOException {
      tsFileIndexWriter.flush();
    }

    public void setRemovedFuture(CompletableFuture<Void> removedFuture) {
      this.removedFuture = removedFuture;
    }

    public CompletableFuture<Void> getRemovedFuture() {
      return removedFuture;
    }

    public void close() {
      tsFileIndexWriter.close();
    }
  }
}
