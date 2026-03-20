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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_FILE_PREFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_START_SEARCH_INDEX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_STATUS_CODE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.WAL_VERSION_ID;

public class WALFileUtils {

  private static final Logger logger = LoggerFactory.getLogger(WALFileUtils.class);

  /**
   * versionId is a self-incremented id number, helping to maintain the order of wal files.
   * startSearchIndex is the valid search index of last flushed wal entry. statusCode is the. For
   * example: <br>
   * _0-0-1.wal: 1, 2, 3, -1, -1, 4, 5, -1 <br>
   * _1-5-0.wal: -1, -1, -1, -1 <br>
   * _2-5-1.wal: 6, 7, 8, 9, -1, -1, -1, 10, 11, -1, 12, 12 <br>
   * _3-12-1.wal: 12, 12, 12, 12, 12 <br>
   * _4-12-1.wal: 12, 13, 14, 15, 16, -1 <br>
   */
  public static final Pattern WAL_FILE_NAME_PATTERN =
      Pattern.compile(
          String.format(
              "%s(?<%s>\\d+)-(?<%s>\\d+)-(?<%s>\\d+)\\%s$",
              WAL_FILE_PREFIX,
              WAL_VERSION_ID,
              WAL_START_SEARCH_INDEX,
              WAL_STATUS_CODE,
              WAL_FILE_SUFFIX));

  public static final String WAL_FILE_NAME_FORMAT =
      WAL_FILE_PREFIX
          + "%d"
          + FILE_NAME_SEPARATOR
          + "%d"
          + FILE_NAME_SEPARATOR
          + "%d"
          + WAL_FILE_SUFFIX;

  /** Return true when this file is .wal file. */
  public static boolean walFilenameFilter(File dir, String name) {
    return WAL_FILE_NAME_PATTERN.matcher(name).find();
  }

  /** List all .wal files in the directory. */
  public static File[] listAllWALFiles(File dir) {
    return dir.listFiles(WALFileUtils::walFilenameFilter);
  }

  /** Get the .wal file starts with the specified version id in the directory. */
  public static File getWALFile(File dir, long versionId) throws FileNotFoundException {
    String filePrefix = WAL_FILE_PREFIX + versionId + FILE_NAME_SEPARATOR;
    File[] files =
        dir.listFiles((d, name) -> walFilenameFilter(d, name) && name.startsWith(filePrefix));
    if (files == null || files.length != 1) {
      throw new FileNotFoundException(
          String.format(
              "Fail to get wal file by versionId=%s and files=%s.",
              versionId, Arrays.toString(files)));
    }
    return files[0];
  }

  /** Parse version id from filename. */
  public static long parseVersionId(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(WAL_VERSION_ID));
    }
    throw new RuntimeException("Invalid wal file name: " + filename);
  }

  /** Parse start search index from filename. */
  public static long parseStartSearchIndex(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(WAL_START_SEARCH_INDEX));
    }
    throw new RuntimeException("Invalid wal file name: " + filename);
  }

  /** Parse status code from filename. */
  public static WALFileStatus parseStatusCode(String filename) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(filename);
    if (matcher.find()) {
      return WALFileStatus.valueOf(Integer.parseInt(matcher.group(WAL_STATUS_CODE)));
    }
    throw new RuntimeException("Invalid wal file name: " + filename);
  }

  /** Sort wal files by version id with ascending order. */
  public static void ascSortByVersionId(File[] walFiles) {
    Arrays.sort(walFiles, Comparator.comparingLong(file -> parseVersionId(file.getName())));
  }

  /**
   * Find index of the file which probably contains target insert plan. <br>
   * Given wal files [ _0-0-1.wal, _1-5-0.wal, _2-5-1.wal, _3-12-1.wal, _4-12-1.wal ], details as
   * below: <br>
   * _0-0-1.wal: 1, 2, 3, -1, -1, 4, 5, -1 <br>
   * _1-5-0.wal: -1, -1, -1, -1 <br>
   * _2-5-1.wal: 6, 7, 8, 9, -1, -1, -1, 10, 11, -1, 12, 12 <br>
   * _3-12-1.wal: 12, 12, 12, 12, 12 <br>
   * _4-12-1.wal: 12, 13, 14, 15, 16, -1 <br>
   * searching [1, 5] will return 0, searching [6, 12] will return 2, search [13, infinity) will
   * return 4， others will return -1
   *
   * @param files files to be searched
   * @param targetSearchIndex search index of target insert plan
   * @return index of the file which probably contains target insert plan , -1 if the target insert
   *     plan definitely doesn't exist
   */
  public static int binarySearchFileBySearchIndex(File[] files, long targetSearchIndex) {
    if (files == null
        || files.length == 0
        || targetSearchIndex <= parseStartSearchIndex(files[0].getName())) {
      return -1;
    }

    if (targetSearchIndex > parseStartSearchIndex(files[files.length - 1].getName())) {
      return files.length - 1;
    }

    int low = 0;
    int high = files.length - 1;
    // search file whose search index i < targetSearchIndex
    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = parseStartSearchIndex(files[mid].getName());

      if (midVal < targetSearchIndex) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return low - 1;
  }

  /** Get .wal filename. */
  public static String getLogFileName(long versionId, long startSearchIndex, WALFileStatus status) {
    return String.format(WAL_FILE_NAME_FORMAT, versionId, startSearchIndex, status.getCode());
  }

  /**
   * get tsFile relative path from sequence or unsequence dir. <br>
   * eg: <br>
   * input: <br>
   * /iotdb/absolute/path/data/datanode/data/sequence/root.db/1/2818/1704354353829-1-0-0.tsfile
   * output: <br>
   * sequence/root.db/1/2818/1704354353829-1-0-0.tsfile
   */
  public static String getTsFileRelativePath(String absolutePath) {
    Path path = new File(absolutePath).toPath();
    return path.subpath(path.getNameCount() - 5, path.getNameCount()).toString();
  }

  /**
   * Find the local searchIndex corresponding to the given (epoch, syncIndex) pair. Scans WAL files
   * in version order, reading only V3 metadata footers for efficiency.
   *
   * @param logDir the WAL directory for a specific data region
   * @param epoch the target epoch
   * @param syncIndex the target syncIndex within that epoch
   * @return the local searchIndex, or -1 if not found
   */
  public static long findSearchIndexByEpochAndSyncIndex(File logDir, long epoch, long syncIndex) {
    final long[] located = locateByEpochAndSyncIndex(logDir, epoch, syncIndex);
    return located != null && located[3] == 1L ? located[0] : -1L;
  }

  /**
   * Find the local searchIndex of the first entry strictly after the given (epoch, syncIndex).
   * Comparison order: epoch first, then syncIndex. Used for consumer-guided positioning to resume
   * from the entry after lastConsumed.
   *
   * @param logDir the WAL directory for a specific data region
   * @param epoch the last consumed epoch
   * @param syncIndex the last consumed syncIndex
   * @return the local searchIndex of the next entry, or -1 if no such entry exists
   */
  public static long findSearchIndexAfterEpochAndSyncIndex(
      File logDir, long epoch, long syncIndex) {
    final long[] located = locateByEpochAndSyncIndex(logDir, epoch, syncIndex);
    if (located == null) {
      return -1L;
    }
    if (located[3] == 0L) {
      return located[0];
    }
    return findNextSearchIndexAfter(logDir, epoch, syncIndex);
  }

  /**
   * Find the (epoch, syncIndex) pair for the given local WAL searchIndex. For V2 WAL files, epoch
   * is treated as 0 and syncIndex equals searchIndex.
   *
   * @param logDir the WAL directory for a specific data region
   * @param searchIndex the local searchIndex to look up
   * @return a two-element array [epoch, syncIndex], or null if not found
   */
  public static long[] findEpochAndSyncIndexBySearchIndex(File logDir, long searchIndex) {
    File[] walFiles = listSealedWALFiles(logDir);
    if (walFiles == null || walFiles.length == 0) {
      return null;
    }

    for (File walFile : walFiles) {
      try (RandomAccessFile raf = new RandomAccessFile(walFile, "r");
          FileChannel channel = raf.getChannel()) {
        final WALMetaData metaData = WALMetaData.readFromWALFile(walFile, channel);
        if (metaData.hasLogicalEntries()) {
          final List<Long> logicalSearchIndices = metaData.getLogicalSearchIndices();
          for (int i = 0; i < logicalSearchIndices.size(); i++) {
            if (logicalSearchIndices.get(i) == searchIndex) {
              return new long[] {
                metaData.getLogicalEpochs().get(i), metaData.getLogicalSyncIndices().get(i)
              };
            }
          }
        }

        final List<Long> epochs = metaData.getEpochs();
        final List<Long> syncIndices = metaData.getSyncIndices();
        if (!syncIndices.isEmpty()) {
          for (int i = 0; i < syncIndices.size(); i++) {
            if (syncIndices.get(i) == searchIndex) {
              final long entryEpoch = i < epochs.size() ? epochs.get(i) : 0L;
              return new long[] {entryEpoch, syncIndices.get(i)};
            }
          }
        }
        final long firstSearchIndex = metaData.getFirstSearchIndex();
        final int entryCount = metaData.getBuffersSize().size();
        final long lastSearchIndex = firstSearchIndex + entryCount - 1L;
        if (searchIndex < firstSearchIndex || searchIndex > lastSearchIndex) {
          continue;
        }
        if (epochFallbackSupported(metaData)) {
          return new long[] {0L, searchIndex};
        }
      } catch (IOException e) {
        logger.warn("Failed to read WAL metadata from {}", walFile, e);
      }
    }
    return null;
  }

  public static long[] locateByEpochAndSyncIndex(File logDir, long epoch, long syncIndex) {
    File[] walFiles = listSealedWALFiles(logDir);
    if (walFiles == null || walFiles.length == 0) {
      return null;
    }

    long previousEpoch = 0L;
    long previousSyncIndex = -1L;
    for (File walFile : walFiles) {
      try (RandomAccessFile raf = new RandomAccessFile(walFile, "r");
          FileChannel channel = raf.getChannel()) {
        final WALMetaData metaData = WALMetaData.readFromWALFile(walFile, channel);
        if (!metaData.hasLogicalEntries()) {
          if (epochFallbackSupported(metaData) && epoch == 0L) {
            final long firstSearchIndex = metaData.getFirstSearchIndex();
            final long lastSearchIndex = firstSearchIndex + metaData.getBuffersSize().size() - 1L;
            if (syncIndex < firstSearchIndex) {
              return new long[] {firstSearchIndex, previousEpoch, previousSyncIndex, 0L};
            }
            if (syncIndex <= lastSearchIndex) {
              return new long[] {syncIndex, previousEpoch, syncIndex - 1L, 1L};
            }
            previousEpoch = 0L;
            previousSyncIndex = lastSearchIndex;
          }
          continue;
        }

        if (compareLogicalKey(
                metaData.getLastLogicalEpoch(),
                metaData.getLastLogicalSyncIndex(),
                epoch,
                syncIndex)
            < 0) {
          previousEpoch = metaData.getLastLogicalEpoch();
          previousSyncIndex = metaData.getLastLogicalSyncIndex();
          continue;
        }

        if (compareLogicalKey(
                metaData.getFirstLogicalEpoch(),
                metaData.getFirstLogicalSyncIndex(),
                epoch,
                syncIndex)
            > 0) {
          return new long[] {
            metaData.getFirstLogicalSearchIndex(), previousEpoch, previousSyncIndex, 0L
          };
        }

        final List<Long> logicalSearchIndices = metaData.getLogicalSearchIndices();
        final List<Long> logicalEpochs = metaData.getLogicalEpochs();
        final List<Long> logicalSyncIndices = metaData.getLogicalSyncIndices();
        long legacyExactSearchIndex = -1L;
        long legacyFirstAfterSearchIndex = -1L;
        for (int i = 0; i < logicalSearchIndices.size(); i++) {
          final long currentEpoch = logicalEpochs.get(i);
          final long currentSyncIndex = logicalSyncIndices.get(i);
          if (currentEpoch == 0L) {
            if (currentSyncIndex == syncIndex && legacyExactSearchIndex < 0L) {
              legacyExactSearchIndex = logicalSearchIndices.get(i);
            } else if (currentSyncIndex > syncIndex && legacyFirstAfterSearchIndex < 0L) {
              legacyFirstAfterSearchIndex = logicalSearchIndices.get(i);
            }
          }
          final int cmp = compareLogicalKey(currentEpoch, currentSyncIndex, epoch, syncIndex);
          if (cmp == 0) {
            return new long[] {logicalSearchIndices.get(i), previousEpoch, previousSyncIndex, 1L};
          }
          if (cmp > 0) {
            return new long[] {logicalSearchIndices.get(i), previousEpoch, previousSyncIndex, 0L};
          }
          previousEpoch = currentEpoch;
          previousSyncIndex = currentSyncIndex;
        }
        if (legacyExactSearchIndex >= 0L) {
          return new long[] {legacyExactSearchIndex, previousEpoch, previousSyncIndex, 1L};
        }
        if (legacyFirstAfterSearchIndex >= 0L) {
          return new long[] {legacyFirstAfterSearchIndex, previousEpoch, previousSyncIndex, 0L};
        }
      } catch (IOException e) {
        logger.warn("Failed to read WAL metadata from {}", walFile, e);
      }
    }
    return null;
  }

  private static long findNextSearchIndexAfter(File logDir, long epoch, long syncIndex) {
    File[] walFiles = listSealedWALFiles(logDir);
    if (walFiles == null || walFiles.length == 0) {
      return -1L;
    }

    for (File walFile : walFiles) {
      try (RandomAccessFile raf = new RandomAccessFile(walFile, "r");
          FileChannel channel = raf.getChannel()) {
        final WALMetaData metaData = WALMetaData.readFromWALFile(walFile, channel);
        if (!metaData.hasLogicalEntries()) {
          if (epochFallbackSupported(metaData) && epoch == 0L) {
            final long firstSearchIndex = metaData.getFirstSearchIndex();
            final long lastSearchIndex = firstSearchIndex + metaData.getBuffersSize().size() - 1L;
            if (syncIndex < firstSearchIndex) {
              return firstSearchIndex;
            }
            if (syncIndex < lastSearchIndex) {
              return syncIndex + 1L;
            }
          }
          continue;
        }
        if (compareLogicalKey(
                metaData.getLastLogicalEpoch(),
                metaData.getLastLogicalSyncIndex(),
                epoch,
                syncIndex)
            <= 0) {
          continue;
        }
        final List<Long> logicalSearchIndices = metaData.getLogicalSearchIndices();
        final List<Long> logicalEpochs = metaData.getLogicalEpochs();
        final List<Long> logicalSyncIndices = metaData.getLogicalSyncIndices();
        long legacyFirstAfterSearchIndex = -1L;
        for (int i = 0; i < logicalSearchIndices.size(); i++) {
          if (logicalEpochs.get(i) == 0L
              && logicalSyncIndices.get(i) > syncIndex
              && legacyFirstAfterSearchIndex < 0L) {
            legacyFirstAfterSearchIndex = logicalSearchIndices.get(i);
          }
          if (compareLogicalKey(logicalEpochs.get(i), logicalSyncIndices.get(i), epoch, syncIndex)
              > 0) {
            return logicalSearchIndices.get(i);
          }
        }
        if (legacyFirstAfterSearchIndex >= 0L) {
          return legacyFirstAfterSearchIndex;
        }
      } catch (IOException e) {
        logger.warn("Failed to read WAL metadata from {}", walFile, e);
      }
    }
    return -1L;
  }

  private static boolean epochFallbackSupported(final WALMetaData metaData) {
    return metaData.getEpochs().isEmpty() && metaData.getSyncIndices().isEmpty();
  }

  private static int compareLogicalKey(
      final long leftEpoch,
      final long leftSyncIndex,
      final long rightEpoch,
      final long rightSyncIndex) {
    if (leftEpoch != rightEpoch) {
      return Long.compare(leftEpoch, rightEpoch);
    }
    return Long.compare(leftSyncIndex, rightSyncIndex);
  }

  private static File[] listSealedWALFiles(final File logDir) {
    final File[] walFiles = listAllWALFiles(logDir);
    if (walFiles == null || walFiles.length == 0) {
      return walFiles;
    }
    ascSortByVersionId(walFiles);
    if (walFiles.length == 1) {
      return new File[0];
    }
    return Arrays.copyOf(walFiles, walFiles.length - 1);
  }
}
