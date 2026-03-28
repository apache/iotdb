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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ProgressWALReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
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
  private static final int SEARCH_INDEX_OFFSET =
      WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES;

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
   * Find the earliest local searchIndex strictly after the given compatibility frontier. This
   * fallback path is only used when the caller has a coarse (physicalTime, localSeq) pair but no
   * writer identity.
   */
  public static long findSearchIndexAfterCompatibleProgress(
      final File logDir, final long physicalTime, final long localSeq) {
    final long[] bestSearchIndex = new long[] {-1L};
    final long[] bestPhysicalTime = new long[] {Long.MAX_VALUE};
    final long[] bestLocalSeq = new long[] {Long.MAX_VALUE};
    final int[] bestNodeId = new int[] {Integer.MAX_VALUE};

    forEachSealedSearchableRequest(
        logDir,
        request -> {
          if (compareCompatibleProgress(
                  request.physicalTime, request.nodeId, request.localSeq, physicalTime, localSeq)
              <= 0) {
            return true;
          }
          if (bestSearchIndex[0] < 0L
              || compareWriterProgress(
                      request.physicalTime,
                      request.nodeId,
                      request.localSeq,
                      bestPhysicalTime[0],
                      bestNodeId[0],
                      bestLocalSeq[0])
                  < 0) {
            bestSearchIndex[0] = request.searchIndex;
            bestPhysicalTime[0] = request.physicalTime;
            bestLocalSeq[0] = request.localSeq;
            bestNodeId[0] = request.nodeId;
          }
          return true;
        });
    return bestSearchIndex[0];
  }

  /**
   * Locate the first local searchIndex whose writer progress is equal to or strictly greater than
   * the given writer-local frontier. This is currently used by single-writer recovery paths, so it
   * matches only entries from the supplied (nodeId, writerEpoch) pair.
   *
   * @return [targetSearchIndex, exactMatchFlag], or null if no matching/later entry exists
   */
  public static long[] locateByWriterProgress(
      final File logDir,
      final int nodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq) {
    final long[] exactSearchIndex = new long[] {-1L};
    final long[] firstAfterSearchIndex = new long[] {-1L};
    final long[] firstAfterPhysicalTime = new long[] {Long.MAX_VALUE};
    final long[] firstAfterLocalSeq = new long[] {Long.MAX_VALUE};

    forEachSealedSearchableRequest(
        logDir,
        request -> {
          if (request.nodeId != nodeId || request.writerEpoch != writerEpoch) {
            return true;
          }
          final int cmp =
              compareWriterProgress(
                  request.physicalTime,
                  request.nodeId,
                  request.localSeq,
                  physicalTime,
                  nodeId,
                  localSeq);
          if (cmp == 0) {
            exactSearchIndex[0] = request.searchIndex;
            return false;
          }
          if (cmp > 0
              && (firstAfterSearchIndex[0] < 0L
                  || compareWriterProgress(
                          request.physicalTime,
                          request.nodeId,
                          request.localSeq,
                          firstAfterPhysicalTime[0],
                          nodeId,
                          firstAfterLocalSeq[0])
                      < 0)) {
            firstAfterSearchIndex[0] = request.searchIndex;
            firstAfterPhysicalTime[0] = request.physicalTime;
            firstAfterLocalSeq[0] = request.localSeq;
          }
          return true;
        });

    if (exactSearchIndex[0] >= 0L) {
      return new long[] {exactSearchIndex[0], 1L};
    }
    if (firstAfterSearchIndex[0] >= 0L) {
      return new long[] {firstAfterSearchIndex[0], 0L};
    }
    return null;
  }

  public static long findSearchIndexByWriterProgress(
      final File logDir,
      final int nodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq) {
    final long[] located =
        locateByWriterProgress(logDir, nodeId, writerEpoch, physicalTime, localSeq);
    return located != null && located[1] == 1L ? located[0] : -1L;
  }

  public static long findSearchIndexAfterWriterProgress(
      final File logDir,
      final int nodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq) {
    final long[] bestSearchIndex = new long[] {-1L};
    final long[] bestPhysicalTime = new long[] {Long.MAX_VALUE};
    final long[] bestLocalSeq = new long[] {Long.MAX_VALUE};
    forEachSealedSearchableRequest(
        logDir,
        request -> {
          if (request.nodeId != nodeId || request.writerEpoch != writerEpoch) {
            return true;
          }
          if (compareWriterProgress(
                  request.physicalTime,
                  request.nodeId,
                  request.localSeq,
                  physicalTime,
                  nodeId,
                  localSeq)
              <= 0) {
            return true;
          }
          if (bestSearchIndex[0] < 0L
              || compareWriterProgress(
                      request.physicalTime,
                      request.nodeId,
                      request.localSeq,
                      bestPhysicalTime[0],
                      nodeId,
                      bestLocalSeq[0])
                  < 0) {
            bestSearchIndex[0] = request.searchIndex;
            bestPhysicalTime[0] = request.physicalTime;
            bestLocalSeq[0] = request.localSeq;
          }
          return true;
        });
    return bestSearchIndex[0];
  }

  private interface SearchableRequestVisitor {
    boolean onRequest(SearchableRequestMeta request);
  }

  private static final class SearchableRequestMeta {
    private final long searchIndex;
    private final long physicalTime;
    private final int nodeId;
    private final long writerEpoch;
    private final long localSeq;

    private SearchableRequestMeta(
        final long searchIndex,
        final long physicalTime,
        final int nodeId,
        final long writerEpoch,
        final long localSeq) {
      this.searchIndex = searchIndex;
      this.physicalTime = physicalTime;
      this.nodeId = nodeId;
      this.writerEpoch = writerEpoch;
      this.localSeq = localSeq;
    }
  }

  private static void forEachSealedSearchableRequest(
      final File logDir, final SearchableRequestVisitor visitor) {
    final File[] walFiles = listSealedWALFiles(logDir);
    if (walFiles == null || walFiles.length == 0) {
      return;
    }

    for (final File walFile : walFiles) {
      try (final ProgressWALReader reader = new ProgressWALReader(walFile)) {
        long pendingSearchIndex = Long.MIN_VALUE;
        long pendingPhysicalTime = 0L;
        int pendingNodeId = -1;
        long pendingWriterEpoch = 0L;
        long pendingLocalSeq = Long.MIN_VALUE;
        boolean hasPending = false;

        while (reader.hasNext()) {
          final ByteBuffer buffer = reader.next();
          final WALEntryType type = WALEntryType.valueOf(buffer.get());
          buffer.clear();
          if (!type.needSearch()) {
            continue;
          }

          final long currentLocalSeq = reader.getCurrentEntryLocalSeq();
          final long currentPhysicalTime = reader.getCurrentEntryPhysicalTime();
          final int currentNodeId = reader.getCurrentEntryNodeId();
          final long currentWriterEpoch = reader.getCurrentEntryWriterEpoch();

          buffer.position(SEARCH_INDEX_OFFSET);
          final long bodySearchIndex = buffer.getLong();
          buffer.clear();
          final long currentSearchIndex = bodySearchIndex >= 0 ? bodySearchIndex : currentLocalSeq;

          if (hasPending
              && pendingLocalSeq == currentLocalSeq
              && pendingNodeId == currentNodeId
              && pendingWriterEpoch == currentWriterEpoch) {
            if (pendingSearchIndex < 0 && currentSearchIndex >= 0) {
              pendingSearchIndex = currentSearchIndex;
            }
            continue;
          }

          if (hasPending
              && !visitor.onRequest(
                  new SearchableRequestMeta(
                      pendingSearchIndex >= 0 ? pendingSearchIndex : pendingLocalSeq,
                      pendingPhysicalTime,
                      pendingNodeId,
                      pendingWriterEpoch,
                      pendingLocalSeq))) {
            return;
          }

          hasPending = true;
          pendingSearchIndex = currentSearchIndex;
          pendingPhysicalTime = currentPhysicalTime;
          pendingNodeId = currentNodeId;
          pendingWriterEpoch = currentWriterEpoch;
          pendingLocalSeq = currentLocalSeq;
        }

        if (hasPending
            && !visitor.onRequest(
                new SearchableRequestMeta(
                    pendingSearchIndex >= 0 ? pendingSearchIndex : pendingLocalSeq,
                    pendingPhysicalTime,
                    pendingNodeId,
                    pendingWriterEpoch,
                    pendingLocalSeq))) {
          return;
        }
      } catch (final IOException e) {
        logger.warn("Failed to scan WAL file {} for searchable request metadata", walFile, e);
      }
    }
  }

  private static int compareCompatibleProgress(
      final long leftPhysicalTime,
      final int leftNodeId,
      final long leftLocalSeq,
      final long rightPhysicalTime,
      final long rightLocalSeq) {
    if (leftPhysicalTime != rightPhysicalTime) {
      return Long.compare(leftPhysicalTime, rightPhysicalTime);
    }
    if (leftLocalSeq != rightLocalSeq) {
      return Long.compare(leftLocalSeq, rightLocalSeq);
    }
    return 0;
  }

  private static int compareWriterProgress(
      final long leftPhysicalTime,
      final int leftNodeId,
      final long leftLocalSeq,
      final long rightPhysicalTime,
      final int rightNodeId,
      final long rightLocalSeq) {
    if (leftPhysicalTime != rightPhysicalTime) {
      return Long.compare(leftPhysicalTime, rightPhysicalTime);
    }
    if (leftNodeId != rightNodeId) {
      return Integer.compare(leftNodeId, rightNodeId);
    }
    return Long.compare(leftLocalSeq, rightLocalSeq);
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
