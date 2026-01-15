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

import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableDiskUsageCacheWriter {
  private static final String TSFILE_CACHE_KEY_FILENAME_PREFIX = "TableSizeKeyFile_";
  private static final String TSFILE_CACHE_VALUE_FILENAME_PREFIX = "TableSizeValueFile_";
  public static final int KEY_FILE_OFFSET_RECORD_LENGTH = 5 * Long.BYTES;
  public static final int KEY_FILE_REDIRECT_RECORD_LENGTH = 7 * Long.BYTES;
  private static final String TEMP_CACHE_FILE_SUBFIX = ".tmp";
  public static final byte KEY_FILE_RECORD_TYPE_OFFSET = 1;
  public static final byte KEY_FILE_RECORD_TYPE_REDIRECT = 2;

  private final int regionId;
  private int activeReaderNum = 0;
  private int currentTsFileIndexFileVersion = 0;
  private final File dir;
  private File currentKeyIndexFile;
  private File currentValueIndexFile;
  private FileOutputStream keyFileOutputStream;
  private FileOutputStream valueFileOutputStream;
  private BufferedOutputStream keyBufferedOutputStream;
  private BufferedOutputStream valueBufferedOutputStream;
  private long keyFileSize;
  private long valueFileSize;

  public TableDiskUsageCacheWriter(String database, int regionId) {
    this.regionId = regionId;
    this.dir = StorageEngine.getDataRegionSystemDir(database, regionId + "");
    recoverTsFileTableSizeIndexFile();
  }

  private void recoverTsFileTableSizeIndexFile() {
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
          currentTsFileIndexFileVersion = Math.max(currentTsFileIndexFileVersion, version);
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
    currentKeyIndexFile =
        keyFiles.isEmpty()
            ? new File(
                dir
                    + File.separator
                    + TSFILE_CACHE_KEY_FILENAME_PREFIX
                    + currentTsFileIndexFileVersion)
            : keyFiles.get(0);
    currentValueIndexFile =
        valueFiles.isEmpty()
            ? new File(
                dir
                    + File.separator
                    + TSFILE_CACHE_VALUE_FILENAME_PREFIX
                    + currentTsFileIndexFileVersion)
            : valueFiles.get(0);
    try {
      cacheFileSelfCheck();
    } catch (IOException ignored) {
    }
  }

  private void cacheFileSelfCheck() throws IOException {
    currentKeyIndexFile.createNewFile();
    currentValueIndexFile.createNewFile();
    TsFileTableSizeCacheReader cacheFileReader =
        new TsFileTableSizeCacheReader(
            currentKeyIndexFile.length(),
            currentKeyIndexFile,
            currentValueIndexFile.length(),
            currentValueIndexFile,
            regionId);
    Pair<Long, Long> truncateSize = cacheFileReader.selfCheck();
    if (truncateSize.left != currentKeyIndexFile.length()) {
      try (FileChannel channel =
          FileChannel.open(currentKeyIndexFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(truncateSize.left);
      }
    }
    if (truncateSize.right != currentValueIndexFile.length()) {
      try (FileChannel channel =
          FileChannel.open(currentValueIndexFile.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(truncateSize.right);
      }
    }
    this.keyFileSize = truncateSize.left;
    this.valueFileSize = truncateSize.right;
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
    if (keyFileOutputStream == null) {
      keyFileOutputStream = new FileOutputStream(currentKeyIndexFile, true);
      keyFileSize = currentKeyIndexFile.length();
      keyBufferedOutputStream = new BufferedOutputStream(keyFileOutputStream);
    }
    if (valueFileOutputStream == null) {
      valueFileOutputStream = new FileOutputStream(currentValueIndexFile, true);
      valueFileSize = currentValueIndexFile.length();
      valueBufferedOutputStream = new BufferedOutputStream(valueFileOutputStream);
    }

    long valueOffset = valueFileSize;
    valueFileSize +=
        ReadWriteForEncodingUtils.writeVarInt(tableSizeMap.size(), valueBufferedOutputStream);
    for (Map.Entry<String, Long> entry : tableSizeMap.entrySet()) {
      valueFileSize += ReadWriteIOUtils.writeVar(entry.getKey(), valueBufferedOutputStream);
      valueFileSize += ReadWriteIOUtils.write(entry.getValue(), valueBufferedOutputStream);
    }
    keyFileSize += ReadWriteIOUtils.write(KEY_FILE_RECORD_TYPE_OFFSET, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.timePartitionId, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(tsFileID.compactionVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(valueOffset, keyBufferedOutputStream);
  }

  public void write(TsFileID originTsFileID, TsFileID newTsFileID) throws IOException {
    if (keyFileOutputStream == null) {
      keyFileOutputStream = new FileOutputStream(currentKeyIndexFile, true);
      keyFileSize = currentKeyIndexFile.length();
      keyBufferedOutputStream = new BufferedOutputStream(keyFileOutputStream);
    }
    keyFileSize += ReadWriteIOUtils.write(KEY_FILE_RECORD_TYPE_REDIRECT, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.timePartitionId, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(newTsFileID.compactionVersion, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(originTsFileID.timestamp, keyBufferedOutputStream);
    keyFileSize += ReadWriteIOUtils.write(originTsFileID.fileVersion, keyBufferedOutputStream);
    keyFileSize +=
        ReadWriteIOUtils.write(originTsFileID.compactionVersion, keyBufferedOutputStream);
  }

  public void compact() {}

  public void flush() throws IOException {
    if (valueBufferedOutputStream != null) {
      valueBufferedOutputStream.flush();
    }
    if (keyFileOutputStream != null) {
      keyBufferedOutputStream.flush();
    }
  }

  public File getKeyFile() {
    return currentKeyIndexFile;
  }

  public File getValueFile() {
    return currentValueIndexFile;
  }

  public long keyFileLength() {
    return keyFileSize;
  }

  public long valueFileLength() {
    return valueFileSize;
  }

  public void fsync() throws IOException {
    flush();
    valueFileOutputStream.getFD().sync();
    keyFileOutputStream.getFD().sync();
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

  public void close() {
    try {
      fsync();
    } catch (IOException ignored) {
    }
    try {
      if (valueBufferedOutputStream != null) {
        valueBufferedOutputStream.close();
      }
    } catch (IOException ignored) {
    }
    try {
      if (keyBufferedOutputStream != null) {
        keyBufferedOutputStream.close();
      }
    } catch (IOException ignored) {
    }
  }
}
