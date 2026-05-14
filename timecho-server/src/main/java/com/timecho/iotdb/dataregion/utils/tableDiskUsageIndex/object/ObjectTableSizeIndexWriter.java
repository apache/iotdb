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

package com.timecho.iotdb.dataregion.utils.tableDiskUsageIndex.object;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.AbstractTableSizeIndexWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageIndex.DataRegionTableSizeQueryContext;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ObjectTableSizeIndexWriter extends AbstractTableSizeIndexWriter {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(ObjectTableSizeIndexWriter.class);
  public static final String OBJECT_SIZE_INDEX_FILE_PREFIX = "TableObjectSizeFile_";
  public static final byte OBJECT_FILE_NUM_DELTA_TYPE = 1;
  public static final byte OBJECT_FILE_SIZE_DELTA_TYPE = 2;
  public static final byte OBJECT_FILE_SNAPSHOT_END_TYPE = 3;

  private final String database;
  private int currentFileVersion = 0;
  private ObjectFileTableSizeIndexFileWriter writer;

  private final Map<Long, Map<String, Long>> pendingObjectSizeDeltasByTimePartition;
  private int pendingObjectFileCountDelta;
  // The timestamp since when object size deltas have been accumulated but not yet persisted to
  // the object table size index file.
  private long pendingObjectDeltaBatchStartTime;

  public ObjectTableSizeIndexWriter(String database, int regionId) {
    super(database, regionId);
    this.database = database;
    recoverFromDir(true);

    pendingObjectSizeDeltasByTimePartition = new HashMap<>();
    pendingObjectFileCountDelta = 0;
    pendingObjectDeltaBatchStartTime = 0;
  }

  private void recoverFromDir(boolean needSelfCheck) {
    dir.mkdirs();
    File[] files = dir.listFiles();
    currentFileVersion = 0;
    File previousFile = null;
    if (files != null) {
      for (File file : files) {
        String fileName = file.getName();
        if (!fileName.startsWith(OBJECT_SIZE_INDEX_FILE_PREFIX)) {
          continue;
        }
        if (fileName.endsWith(TEMP_INDEX_FILE_SUBFIX)) {
          deleteFile(file);
        }
        int version;
        try {
          version = Integer.parseInt(fileName.substring(OBJECT_SIZE_INDEX_FILE_PREFIX.length()));
        } catch (NumberFormatException e) {
          continue;
        }
        if (version < currentFileVersion) {
          deleteFile(file);
        } else {
          if (previousFile != null) {
            deleteFile(previousFile);
          }
          previousFile = file;
          currentFileVersion = version;
        }
      }
    }
    if (previousFile == null) {
      try {
        previousFile = recoverByScanAllObjectFiles();
      } catch (IOException e) {
        failedToRecover(e);
        return;
      }
    }
    try {
      this.writer =
          new ObjectFileTableSizeIndexFileWriter(database, regionId, previousFile, needSelfCheck);
    } catch (IOException e) {
      failedToRecover(e);
    }
  }

  private File recoverByScanAllObjectFiles() throws IOException {
    ObjectFileTableSizeIndexFileWriter tempWriter = null;
    DataRegionTableSizeQueryContext context = scanAllObjectFiles();
    try {
      tempWriter =
          new ObjectFileTableSizeIndexFileWriter(database, regionId, generateFile(0, true), false);
      tempWriter.writeResultFromQuery(context);
    } finally {
      if (tempWriter != null) {
        tempWriter.close();
      }
    }
    File target = generateFile(0, false);
    tempWriter.getFile().renameTo(target);
    return target;
  }

  private DataRegionTableSizeQueryContext scanAllObjectFiles() throws IOException {
    boolean restrictObjectLimit =
        CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit();
    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    for (String dir : TierManager.getInstance().getAllObjectFileFolders()) {
      File dataRegionObjectDir = new File(dir, regionId + "");
      if (!dataRegionObjectDir.isDirectory()) {
        continue;
      }
      File[] tableDirs = dataRegionObjectDir.listFiles();
      if (tableDirs == null) {
        continue;
      }
      for (File tableDir : tableDirs) {
        if (!tableDir.isDirectory()) {
          continue;
        }
        final String tableName;

        if (!restrictObjectLimit) {
          try {
            tableName =
                new String(
                    BaseEncoding.base32().omitPadding().decode(tableDir.getName()),
                    StandardCharsets.UTF_8);
          } catch (IllegalArgumentException ignored) {
            continue;
          }
        } else {
          tableName = tableDir.getName();
        }
        Files.walkFileTree(
            tableDir.toPath(),
            new FileVisitor<Path>() {

              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String fileName = file.getFileName().toString();
                if (!fileName.endsWith(".bin")) {
                  return FileVisitResult.CONTINUE;
                }
                long timestamp;
                try {
                  timestamp = Long.parseLong(fileName.substring(0, fileName.length() - 4));
                } catch (NumberFormatException e) {
                  return FileVisitResult.CONTINUE;
                }
                long timePartition = TimePartitionUtils.getTimePartitionId(timestamp);
                context.updateObjectFileNum(1);
                context.updateResult(tableName, attrs.size(), timePartition);
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                return FileVisitResult.CONTINUE;
              }
            });
      }
    }
    return context;
  }

  private File generateFile(int version, boolean isTempFile) {
    return new File(
        dir
            + File.separator
            + OBJECT_SIZE_INDEX_FILE_PREFIX
            + version
            + (isTempFile ? TEMP_INDEX_FILE_SUBFIX : ""));
  }

  protected void deleteFile(File file) {
    try {
      Files.delete(file.toPath());
    } catch (IOException ignored) {
    }
  }

  public void write(String table, long timePartition, long size, int num) {
    pendingObjectSizeDeltasByTimePartition
        .computeIfAbsent(timePartition, k -> new HashMap<>())
        .compute(table, (k, v) -> (v == null ? 0 : v) + size);
    pendingObjectFileCountDelta += num;
    if (pendingObjectDeltaBatchStartTime == 0) {
      pendingObjectDeltaBatchStartTime = System.currentTimeMillis();
    }
    markWritten();
  }

  public File getFile() {
    return writer.getFile();
  }

  @Override
  public boolean needCompact() {
    if (System.currentTimeMillis() - previousCompactionTimestamp <= TimeUnit.MINUTES.toMillis(2)) {
      return false;
    }
    long deltaSize = writer.length() - writer.getSnapshotSize();
    return deltaSize > 1024 * 1024;
  }

  @Override
  public void compact() {
    previousCompactionTimestamp = System.currentTimeMillis();
    close();
    File originFile = writer.getFile();
    ObjectTableSizeIndexReader reader =
        new ObjectTableSizeIndexReader(originFile, originFile.length());
    DataRegionTableSizeQueryContext context = new DataRegionTableSizeQueryContext(true);
    try {
      reader.loadObjectFileTableSize(context, System.nanoTime(), Long.MAX_VALUE);
    } catch (IOException e) {
      logger.error("Failed to execute compaction for obejct table size index file", e);
      return;
    } finally {
      reader.close();
    }
    ObjectFileTableSizeIndexFileWriter targetFileWriter = null;
    try {
      targetFileWriter =
          new ObjectFileTableSizeIndexFileWriter(
              database, regionId, generateFile(currentFileVersion + 1, true), false);
      targetFileWriter.writeResultFromQuery(context);
      targetFileWriter.close();

      Files.move(
          targetFileWriter.getFile().toPath(),
          generateFile(currentFileVersion + 1, false).toPath());
    } catch (IOException e) {
      logger.error("Failed to execute compaction for object table size index file", e);
    } finally {
      if (targetFileWriter != null) {
        targetFileWriter.close();
      }
      recoverFromDir(false);
    }
  }

  public void syncPendingObjectDeltasToDiskIfNecessary() {
    if (pendingObjectSizeDeltasByTimePartition.isEmpty()
        || System.currentTimeMillis() - pendingObjectDeltaBatchStartTime
            < config.getWalAsyncModeFsyncDelayInMs()) {
      return;
    }
    persistPendingObjectDeltas();
    sync();
  }

  public void persistPendingObjectDeltas() {
    try {
      for (Map.Entry<Long, Map<String, Long>> timePartitionDeltaEntry :
          pendingObjectSizeDeltasByTimePartition.entrySet()) {
        writer.write(timePartitionDeltaEntry.getKey(), timePartitionDeltaEntry.getValue());
      }
      if (pendingObjectFileCountDelta != 0) {
        writer.write(pendingObjectFileCountDelta);
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    pendingObjectFileCountDelta = 0;
    pendingObjectDeltaBatchStartTime = 0;
    pendingObjectSizeDeltasByTimePartition.clear();
  }

  @Override
  public void flush() {
    try {
      writer.flush();
    } catch (IOException e) {
      logger.warn("Failed to sync object table size index file {}", getFile(), e);
    }
  }

  @Override
  public void sync() {
    try {
      writer.sync();
    } catch (IOException e) {
      logger.warn("Failed to sync object table size index file {}", getFile(), e);
    }
  }

  @Override
  public void close() {
    persistPendingObjectDeltas();
    sync();
    writer.close();
  }
}
