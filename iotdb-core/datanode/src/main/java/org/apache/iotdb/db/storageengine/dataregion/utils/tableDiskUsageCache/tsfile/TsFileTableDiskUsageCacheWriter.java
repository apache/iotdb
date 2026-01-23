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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.tsfile;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.AbstractTableSizeCacheWriter;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TimePartitionTableSizeQueryContext;

import org.apache.tsfile.utils.Pair;

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

public class TsFileTableDiskUsageCacheWriter extends AbstractTableSizeCacheWriter {
  private static final String TSFILE_CACHE_KEY_FILENAME_PREFIX = "TableSizeKeyFile_";
  private static final String TSFILE_CACHE_VALUE_FILENAME_PREFIX = "TableSizeValueFile_";
  public static final int KEY_FILE_OFFSET_RECORD_LENGTH = 5 * Long.BYTES + 1;
  public static final int KEY_FILE_REDIRECT_RECORD_LENGTH = 7 * Long.BYTES + 1;
  public static final byte KEY_FILE_RECORD_TYPE_OFFSET = 1;
  public static final byte KEY_FILE_RECORD_TYPE_REDIRECT = 2;

  private TsFileTableSizeIndexFileWriter tsFileTableSizeIndexFileWriter;

  public TsFileTableDiskUsageCacheWriter(String database, int regionId) {
    super(database, regionId);
    recoverTsFileTableSizeIndexFile(true);
  }

  private void recoverTsFileTableSizeIndexFile(boolean needRecover) {
    dir.mkdirs();
    File[] files = dir.listFiles();
    currentIndexFileVersion = 0;
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
        currentIndexFileVersion = Math.max(currentIndexFileVersion, version);
        keyFiles.add(file);
      }
      if (keyFiles.size() > 1) {
        deleteOldVersionFiles(currentIndexFileVersion, TSFILE_CACHE_KEY_FILENAME_PREFIX, keyFiles);
      }
      if (valueFiles.size() > 1) {
        deleteOldVersionFiles(
            currentIndexFileVersion, TSFILE_CACHE_VALUE_FILENAME_PREFIX, valueFiles);
      }
    }
    File currentKeyIndexFile = generateKeyFile(currentIndexFileVersion, false);
    File currentValueIndexFile = generateValueFile(currentIndexFileVersion, false);
    try {
      this.tsFileTableSizeIndexFileWriter =
          new TsFileTableSizeIndexFileWriter(
              regionId, currentKeyIndexFile, currentValueIndexFile, needRecover);
    } catch (IOException e) {
      failedToRecover(e);
    }
  }

  public void write(TsFileID tsFileID, Map<String, Long> tableSizeMap) throws IOException {
    tsFileTableSizeIndexFileWriter.write(tsFileID, tableSizeMap);
    markWritten();
  }

  public void write(TsFileID originTsFileID, TsFileID newTsFileID) throws IOException {
    tsFileTableSizeIndexFileWriter.write(originTsFileID, newTsFileID);
    markWritten();
  }

  @Override
  public boolean needCompact() {
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
    return delta >= 1000;
  }

  @Override
  public void compact() {
    previousCompactionTimestamp = System.currentTimeMillis();
    this.tsFileTableSizeIndexFileWriter.close();
    TsFileTableSizeCacheReader cacheFileReader =
        new TsFileTableSizeCacheReader(
            tsFileTableSizeIndexFileWriter.getKeyFile().length(),
            tsFileTableSizeIndexFileWriter.getKeyFile(),
            tsFileTableSizeIndexFileWriter.getValueFile().length(),
            tsFileTableSizeIndexFileWriter.getValueFile(),
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
      logger.error("Failed to read key file during compaction", e);
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

    TsFileTableSizeIndexFileWriter targetFileWriter = null;
    try {
      targetFileWriter =
          new TsFileTableSizeIndexFileWriter(
              regionId,
              generateKeyFile(currentIndexFileVersion + 1, true),
              generateValueFile(currentIndexFileVersion + 1, true));
      cacheFileReader.openValueFile();
      for (Pair<TsFileID, Long> pair : validFilesOrderByOffset) {
        TsFileID tsFileID = pair.getLeft();
        long offset = pair.getRight();
        Map<String, Long> tableSizeMap = cacheFileReader.readOneEntryFromValueFile(offset, true);
        targetFileWriter.write(tsFileID, tableSizeMap);
      }
      targetFileWriter.close();

      // replace
      File targetKeyFile = generateKeyFile(currentIndexFileVersion + 1, false);
      File targetValueFile = generateValueFile(currentIndexFileVersion + 1, false);
      Files.move(targetFileWriter.getKeyFile().toPath(), targetKeyFile.toPath());
      Files.move(targetFileWriter.getValueFile().toPath(), targetValueFile.toPath());
    } catch (Exception e) {
      logger.error("Failed to execute compaction for tsfile table size cache file", e);
    } finally {
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

  @Override
  public void flush() throws IOException {
    tsFileTableSizeIndexFileWriter.flush();
  }

  public File getKeyFile() {
    return tsFileTableSizeIndexFileWriter.getKeyFile();
  }

  public File getValueFile() {
    return tsFileTableSizeIndexFileWriter.getValueFile();
  }

  public long keyFileLength() {
    return tsFileTableSizeIndexFileWriter.keyFileLength();
  }

  public long valueFileLength() {
    return tsFileTableSizeIndexFileWriter.valueFileLength();
  }

  @Override
  public void sync() throws IOException {
    tsFileTableSizeIndexFileWriter.sync();
  }

  @Override
  public void close() {
    this.tsFileTableSizeIndexFileWriter.close();
  }
}
