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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class TableDiskUsageCacheWriter {
  private static final Logger logger = LoggerFactory.getLogger(TableDiskUsageCacheWriter.class);
  private static final String TSFILE_CACHE_KEY_FILENAME_PREFIX = "TableSizeKeyFile_";
  private static final String TSFILE_CACHE_VALUE_FILENAME_PREFIX = "TableSizeValueFile_";
  public static final int KEY_FILE_OFFSET_RECORD_LENGTH = 5 * Long.BYTES + 1;
  public static final int KEY_FILE_REDIRECT_RECORD_LENGTH = 7 * Long.BYTES + 1;
  private static final String TEMP_CACHE_FILE_SUBFIX = ".tmp";
  public static final byte KEY_FILE_RECORD_TYPE_OFFSET = 1;
  public static final byte KEY_FILE_RECORD_TYPE_REDIRECT = 2;

  private final int regionId;
  private int activeReaderNum = 0;
  private long previousCompactionTimestamp = System.currentTimeMillis();
  private long lastWriteTimestamp = System.currentTimeMillis();
  private int currentTsFileIndexFileVersion = 0;
  private final File dir;
  private TsFileTableSizeCacheWriter tsFileTableSizeCacheWriter;

  public TableDiskUsageCacheWriter(String database, int regionId) {
    this.regionId = regionId;
    this.dir = StorageEngine.getDataRegionSystemDir(database, regionId + "");
    recoverTsFileTableSizeIndexFile(true);
  }

  private void recoverTsFileTableSizeIndexFile(boolean needRecover) {
    dir.mkdirs();
    File[] files = dir.listFiles();
    currentTsFileIndexFileVersion = 0;
    List<File> keyFiles = new ArrayList<>();
    List<File> valueFiles = new ArrayList<>();
    if (files != null) {
      for (File file : files) {
        String fileName = file.getName();
        boolean isKeyFile = fileName.startsWith(TSFILE_CACHE_KEY_FILENAME_PREFIX);
        boolean isValueFile = !isKeyFile && fileName.startsWith(TSFILE_CACHE_VALUE_FILENAME_PREFIX);
        boolean isTempFile = fileName.endsWith(TEMP_CACHE_FILE_SUBFIX);
        if (!isKeyFile) {
          if (isValueFile && !isTempFile) {
            valueFiles.add(file);
          }
          continue;
        }
        if (isTempFile) {
          try {
            Files.delete(file.toPath());
          } catch (IOException ignored) {
          }
        }
        int version;
        try {
          version = Integer.parseInt(fileName.substring(TSFILE_CACHE_KEY_FILENAME_PREFIX.length()));
        } catch (NumberFormatException ignored) {
          continue;
        }
        File valueFile =
            new File(dir + File.separator + TSFILE_CACHE_VALUE_FILENAME_PREFIX + version);
        // may have a valid value index file
        if (!valueFile.exists()) {
          File tempValueFile = new File(valueFile.getPath() + TEMP_CACHE_FILE_SUBFIX);
          if (tempValueFile.exists()) {
            tempValueFile.renameTo(valueFile);
            valueFiles.add(valueFile);
          } else {
            // lost value file
            try {
              Files.delete(file.toPath());
            } catch (IOException ignored) {
            }
            continue;
          }
        }
        currentTsFileIndexFileVersion = Math.max(currentTsFileIndexFileVersion, version);
        keyFiles.add(file);
      }
      if (keyFiles.size() > 1) {
        deleteOldVersionFiles(
            currentTsFileIndexFileVersion, TSFILE_CACHE_KEY_FILENAME_PREFIX, keyFiles);
      }
      if (valueFiles.size() > 1) {
        deleteOldVersionFiles(
            currentTsFileIndexFileVersion, TSFILE_CACHE_VALUE_FILENAME_PREFIX, valueFiles);
      }
    }
    File currentKeyIndexFile = generateKeyFile(currentTsFileIndexFileVersion, false);
    File currentValueIndexFile = generateValueFile(currentTsFileIndexFileVersion, false);
    try {
      this.tsFileTableSizeCacheWriter =
          new TsFileTableSizeCacheWriter(
              regionId, currentKeyIndexFile, currentValueIndexFile, needRecover);
    } catch (IOException ignored) {
    }
  }

  private void deleteOldVersionFiles(int maxVersion, String prefix, List<File> files) {
    for (File file : files) {
      try {
        int version = Integer.parseInt(file.getName().substring(prefix.length()));
        if (version != maxVersion) {
          Files.deleteIfExists(file.toPath());
        }
      } catch (Exception e) {
      }
    }
  }

  public void write(TsFileID tsFileID, Map<String, Long> tableSizeMap) throws IOException {
    tsFileTableSizeCacheWriter.write(tsFileID, tableSizeMap);
  }

  public void write(TsFileID originTsFileID, TsFileID newTsFileID) throws IOException {
    tsFileTableSizeCacheWriter.write(originTsFileID, newTsFileID);
  }

  public void closeIfIdle() {
    if (System.currentTimeMillis() - lastWriteTimestamp >= TimeUnit.MINUTES.toMillis(1)) {
      close();
    }
  }

  public boolean needCompact() {
    if (activeReaderNum > 0) {
      return false;
    }
    if (System.currentTimeMillis() - previousCompactionTimestamp <= TimeUnit.MINUTES.toMillis(2)) {
      return false;
    }
    DataRegion dataRegion = StorageEngine.getInstance().getDataRegion(new DataRegionId(regionId));
    if (dataRegion == null || dataRegion.isDeleted()) {
      return false;
    }
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    int fileNum = tsFileManager.size(true) + tsFileManager.size(false);
    int estimatedEntryNumInCacheFile = (int) (keyFileLength() / KEY_FILE_OFFSET_RECORD_LENGTH);
    int delta = estimatedEntryNumInCacheFile - fileNum;
    return delta > 0.2 * estimatedEntryNumInCacheFile || delta >= 1000;
  }

  public void compact() {
    previousCompactionTimestamp = System.currentTimeMillis();
    this.tsFileTableSizeCacheWriter.close();
    TsFileTableSizeCacheReader cacheFileReader =
        new TsFileTableSizeCacheReader(
            tsFileTableSizeCacheWriter.getKeyFile().length(),
            tsFileTableSizeCacheWriter.getKeyFile(),
            tsFileTableSizeCacheWriter.getValueFile().length(),
            tsFileTableSizeCacheWriter.getValueFile(),
            regionId);
    Map<Long, TimePartitionTableSizeQueryContext> contextMap = new HashMap<>();
    try {
      cacheFileReader.openKeyFile();
      while (cacheFileReader.hasNextEntryInKeyFile()) {
        TsFileTableSizeCacheReader.KeyFileEntry keyFileEntry =
            cacheFileReader.readOneEntryFromKeyFile();
        TimePartitionTableSizeQueryContext context =
            contextMap.computeIfAbsent(
                keyFileEntry.getTimePartitionId(),
                k -> new TimePartitionTableSizeQueryContext(Collections.emptyMap()));
        if (keyFileEntry.originTsFileID == null) {
          context.addCachedTsFileIDAndOffsetInValueFile(keyFileEntry.tsFileID, keyFileEntry.offset);
        } else {
          context.replaceCachedTsFileID(keyFileEntry.originTsFileID, keyFileEntry.tsFileID);
        }
      }
    } catch (IOException e) {
      return;
    } finally {
      cacheFileReader.closeCurrentFile();
    }

    List<Pair<TsFileID, Long>> validFilesOrderByOffset = new ArrayList<>();
    DataRegion dataRegion = StorageEngine.getInstance().getDataRegion(new DataRegionId(regionId));
    if (dataRegion == null || dataRegion.isDeleted()) {
      return;
    }
    TsFileManager tsFileManager = dataRegion.getTsFileManager();
    for (Long timePartition : tsFileManager.getTimePartitions()) {
      TimePartitionTableSizeQueryContext context = contextMap.get(timePartition);
      if (context == null) {
        continue;
      }
      Pair<List<TsFileResource>, List<TsFileResource>> resources =
          tsFileManager.getTsFileListSnapshot(timePartition);
      Stream.concat(resources.left.stream(), resources.right.stream())
          .forEach(
              resource -> {
                Long offset = context.getCachedTsFileIdOffset(resource.getTsFileID());
                if (offset != null) {
                  validFilesOrderByOffset.add(new Pair<>(resource.getTsFileID(), offset));
                }
              });
    }
    validFilesOrderByOffset.sort(Comparator.comparingLong(Pair::getRight));

    TsFileTableSizeCacheWriter targetFileWriter = null;
    try {
      targetFileWriter =
          new TsFileTableSizeCacheWriter(
              regionId,
              generateKeyFile(currentTsFileIndexFileVersion + 1, true),
              generateValueFile(currentTsFileIndexFileVersion + 1, true));
      cacheFileReader.openValueFile();
      for (Pair<TsFileID, Long> pair : validFilesOrderByOffset) {
        TsFileID tsFileID = pair.getLeft();
        long offset = pair.getRight();
        Map<String, Long> tableSizeMap = cacheFileReader.readOneEntryFromValueFile(offset, true);
        targetFileWriter.write(tsFileID, tableSizeMap);
      }
      targetFileWriter.close();

      // replace
      File targetKeyFile = generateKeyFile(currentTsFileIndexFileVersion + 1, false);
      File targetValueFile = generateValueFile(currentTsFileIndexFileVersion + 1, false);
      targetFileWriter.getKeyFile().renameTo(targetKeyFile);
      targetFileWriter.getValueFile().renameTo(targetValueFile);
      this.tsFileTableSizeCacheWriter.close();
    } catch (Exception e) {
      logger.error("Failed to execute compaction for tsfile table size cache file", e);
    } finally {
      if (tsFileTableSizeCacheWriter != null) {
        tsFileTableSizeCacheWriter.close();
      }
      if (targetFileWriter != null) {
        targetFileWriter.close();
      }
      cacheFileReader.closeCurrentFile();
      this.recoverTsFileTableSizeIndexFile(false);
    }
  }

  private File generateKeyFile(int version, boolean isTempFile) {
    return new File(
        dir
            + File.separator
            + TSFILE_CACHE_KEY_FILENAME_PREFIX
            + version
            + (isTempFile ? TEMP_CACHE_FILE_SUBFIX : ""));
  }

  private File generateValueFile(int version, boolean isTempFile) {
    return new File(
        dir
            + File.separator
            + TSFILE_CACHE_VALUE_FILENAME_PREFIX
            + version
            + (isTempFile ? TEMP_CACHE_FILE_SUBFIX : ""));
  }

  public void flush() throws IOException {
    tsFileTableSizeCacheWriter.flush();
  }

  public File getKeyFile() {
    return tsFileTableSizeCacheWriter.getKeyFile();
  }

  public File getValueFile() {
    return tsFileTableSizeCacheWriter.getValueFile();
  }

  public long keyFileLength() {
    return tsFileTableSizeCacheWriter.keyFileLength();
  }

  public long valueFileLength() {
    return tsFileTableSizeCacheWriter.valueFileLength();
  }

  public void sync() throws IOException {
    tsFileTableSizeCacheWriter.sync();
  }

  public void increaseActiveReaderNum() {
    activeReaderNum++;
  }

  public void decreaseActiveReaderNum() {
    activeReaderNum--;
  }

  public int getActiveReaderNum() {
    return activeReaderNum;
  }

  public void removeFiles() {}

  public void close() {
    this.tsFileTableSizeCacheWriter.close();
  }
}
