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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TableDiskUsageCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDiskUsageCache.class);
  private final BlockingQueue<Operation> queue = new LinkedBlockingQueue<>();
  private final Map<Integer, TableDiskUsageCacheWriter> writerMap = new HashMap<>();
  private final ScheduledExecutorService scheduledExecutorService;

  private TableDiskUsageCache() {
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.FILE_TIME_INDEX_RECORD.getName());
    scheduledExecutorService.submit(this::run);
  }

  private void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Operation operation = queue.poll(10, TimeUnit.MILLISECONDS);
          if (operation == null) {
            checkAndMayCloseIdleWriter();
            checkAndMayCompact(TimeUnit.SECONDS.toMillis(1));
            continue;
          }
          operation.apply(this);
        } catch (InterruptedException e) {
          return;
        } catch (Exception e) {
          LOGGER.error("Meet exception when apply TableDiskUsageCache.", e);
        }
      }
    } finally {
      writerMap.values().forEach(TableDiskUsageCacheWriter::close);
    }
  }

  private void checkAndMayCompact(long maxRunTime) {
    long startTime = System.currentTimeMillis();
    for (TableDiskUsageCacheWriter writer : writerMap.values()) {
      if (System.currentTimeMillis() - startTime > maxRunTime) {
        break;
      }
      if (writer.needCompact()) {
        writer.compact();
      }
    }
  }

  private void checkAndMayCloseIdleWriter() {
    for (TableDiskUsageCacheWriter writer : writerMap.values()) {
      writer.closeIfIdle();
    }
  }

  public void write(String database, TsFileID tsFileID, Map<String, Long> tableSizeMap) {
    if (tableSizeMap == null || tableSizeMap.isEmpty()) {
      return;
    }
    queue.add(new WriteOperation(database, tsFileID, tableSizeMap));
  }

  public void write(String database, TsFileID originTsFileID, TsFileID newTsFileID) {
    queue.add(new ReplaceOperation(database, originTsFileID, newTsFileID));
  }

  public CompletableFuture<TsFileTableSizeCacheReader> startRead(String database, int regionId) {
    StartReadOperation operation = new StartReadOperation(database, regionId);
    queue.add(operation);
    return operation.future;
  }

  public void endRead(String database, int regionId) {
    EndReadOperation operation = new EndReadOperation(database, regionId);
    queue.add(operation);
  }

  public void remove(String database, int regionId) {
    RemoveRegionOperation operation = new RemoveRegionOperation(database, regionId);
    queue.add(operation);
    try {
      operation.future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception ignored) {
    }
  }

  public abstract static class Operation {
    protected final String database;
    protected final int regionId;

    protected Operation(String database, int regionId) {
      this.database = database;
      this.regionId = regionId;
    }

    public abstract void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException;
  }

  private static class StartReadOperation extends Operation {
    public CompletableFuture<TsFileTableSizeCacheReader> future = new CompletableFuture<>();

    public StartReadOperation(String database, int regionId) {
      super(database, regionId);
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      try {
        TableDiskUsageCacheWriter writer =
            tableDiskUsageCache.writerMap.computeIfAbsent(
                regionId, k -> new TableDiskUsageCacheWriter(database, regionId));
        writer.flush();
        writer.increaseActiveReaderNum();
        future.complete(
            new TsFileTableSizeCacheReader(
                writer.keyFileLength(),
                writer.getKeyFile(),
                writer.valueFileLength(),
                writer.getValueFile(),
                regionId));
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
      TableDiskUsageCacheWriter writer =
          tableDiskUsageCache.writerMap.computeIfAbsent(
              regionId, k -> new TableDiskUsageCacheWriter(database, regionId));
      writer.decreaseActiveReaderNum();
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
          .computeIfAbsent(regionId, k -> new TableDiskUsageCacheWriter(database, regionId))
          .write(tsFileID, tableSizeMap);
    }
  }

  private static class ReplaceOperation extends Operation {
    private final TsFileID originTsFileID;
    private final TsFileID newTsFileID;

    public ReplaceOperation(String database, TsFileID originTsFileID, TsFileID newTsFileID) {
      super(database, originTsFileID.regionId);
      this.originTsFileID = originTsFileID;
      this.newTsFileID = newTsFileID;
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      TableDiskUsageCacheWriter writer = tableDiskUsageCache.writerMap.get(regionId);
      if (writer != null) {
        writer.write(originTsFileID, newTsFileID);
      }
    }
  }

  private static class RemoveRegionOperation extends Operation {

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    private RemoveRegionOperation(String database, int regionId) {
      super(database, regionId);
    }

    @Override
    public void apply(TableDiskUsageCache tableDiskUsageCache) throws IOException {
      TableDiskUsageCacheWriter writer = tableDiskUsageCache.writerMap.remove(regionId);
      if (writer != null) {
        writer.close();
      }
      future.complete(null);
    }
  }

  public static TableDiskUsageCache getInstance() {
    return TableDiskUsageCache.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final TableDiskUsageCache INSTANCE = new TableDiskUsageCache();
  }
}
