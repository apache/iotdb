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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ModificationUtils {

  private ModificationUtils() {
    // util class
  }

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata. <br>
   * the caller should guarantee that chunkMetaData and modifications refer to the same time series
   * paths.
   *
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void modifyChunkMetaData(
      List<? extends IChunkMetadata> chunkMetaData, List<ModEntry> modifications) {
    for (IChunkMetadata metaData : chunkMetaData) {
      for (ModEntry modification : modifications) {
        doModifyChunkMetaData(modification, metaData);
      }
    }
    // remove chunks that are completely deleted
    chunkMetaData.removeIf(
        metaData -> {
          if (metaData.getDeleteIntervalList() != null) {
            for (TimeRange range : metaData.getDeleteIntervalList()) {
              if (range.contains(metaData.getStartTime(), metaData.getEndTime())) {
                return true;
              } else {
                if (range.overlaps(new TimeRange(metaData.getStartTime(), metaData.getEndTime()))) {
                  metaData.setModified(true);
                }
              }
            }
          }
          return false;
        });
  }

  public static void modifyAlignedChunkMetaData(
      List<AlignedChunkMetadata> chunkMetaData, List<List<ModEntry>> modifications) {
    for (AlignedChunkMetadata metaData : chunkMetaData) {
      List<IChunkMetadata> valueChunkMetadataList = metaData.getValueChunkMetadataList();
      // deal with each sub sensor
      for (int i = 0; i < valueChunkMetadataList.size(); i++) {
        IChunkMetadata v = valueChunkMetadataList.get(i);
        if (v != null) {
          List<ModEntry> modificationList = modifications.get(i);
          for (ModEntry modification : modificationList) {
            doModifyChunkMetaData(modification, v);
          }
        }
      }
    }
    // if all sub sensors' chunk metadata are deleted, then remove the aligned chunk metadata
    // otherwise, set the deleted chunk metadata of some sensors to null
    chunkMetaData.removeIf(
        alignedChunkMetadata -> {
          // the whole aligned path need to be removed, only set to be true if all the sub sensors
          // are deleted
          boolean removed = true;
          // the whole aligned path is modified, set to be true if any sub sensor is modified
          boolean modified = false;
          List<IChunkMetadata> valueChunkMetadataList =
              alignedChunkMetadata.getValueChunkMetadataList();
          for (int i = 0; i < valueChunkMetadataList.size(); i++) {
            IChunkMetadata valueChunkMetadata = valueChunkMetadataList.get(i);
            if (valueChunkMetadata == null) {
              continue;
            }
            // current sub sensor's chunk metadata is completely removed
            boolean currentRemoved = false;
            if (valueChunkMetadata.getDeleteIntervalList() != null) {
              for (TimeRange range : valueChunkMetadata.getDeleteIntervalList()) {
                if (range.contains(
                    valueChunkMetadata.getStartTime(), valueChunkMetadata.getEndTime())) {
                  valueChunkMetadataList.set(i, null);
                  currentRemoved = true;
                  break;
                } else {
                  if (range.overlaps(
                      new TimeRange(
                          valueChunkMetadata.getStartTime(), valueChunkMetadata.getEndTime()))) {
                    valueChunkMetadata.setModified(true);
                    modified = true;
                  }
                }
              }
            }
            // current sub sensor's chunk metadata is not completely removed,
            // so the whole aligned path don't need to be removed from list
            if (!currentRemoved) {
              removed = false;
            }
          }
          alignedChunkMetadata.setModified(modified);
          return removed;
        });
  }

  // Check whether the timestamp is deleted in deletionList
  // Timestamp and deletionList need to be ordered, and deleteCursor is array whose size is 1 stands
  // for the index of the deletionList
  public static boolean isPointDeleted(
      long timestamp, List<TimeRange> deletionList, int[] deleteCursor) {
    if (deleteCursor.length != 1) {
      throw new IllegalArgumentException("deleteCursor should be an array whose size is 1");
    }
    while (deletionList != null && deleteCursor[0] < deletionList.size()) {
      if (deletionList.get(deleteCursor[0]).contains(timestamp)) {
        return true;
      } else if (deletionList.get(deleteCursor[0]).getMax() < timestamp) {
        deleteCursor[0]++;
      } else {
        return false;
      }
    }
    return false;
  }

  // Check whether the timestamp is deleted in deletionList,
  // if timeRangeList is not ordered, use this method, otherwise use isPointDeleted instead.
  public static boolean isPointDeletedWithoutOrderedRange(
      long timestamp, List<TimeRange> timeRangeList) {
    for (TimeRange range : timeRangeList) {
      if (range.contains(timestamp)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isPointDeleted(long timestamp, List<TimeRange> deletionList) {
    int[] deleteCursor = {0};
    return isPointDeleted(timestamp, deletionList, deleteCursor);
  }

  /**
   * Check whether the device with start time and end time is completely deleted by mods or not.
   * There are some slight differences from that in {@link SettleSelectorImpl}.
   */
  public static boolean isDeviceDeletedByMods(
      Collection<ModEntry> modifications, IDeviceID device, long startTime, long endTime)
      throws IllegalPathException {
    for (ModEntry modification : modifications) {
      PartialPath path = ((TreeDeletionEntry) modification).getPathPattern();
      if (path.include(new PartialPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD))
          && modification.getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isTimeseriesDeletedByMods(
      Collection<ModEntry> modifications,
      IDeviceID device,
      String timeseriesId,
      long startTime,
      long endTime)
      throws IllegalPathException {
    for (ModEntry modification : modifications) {
      PartialPath path = ((TreeDeletionEntry) modification).getPathPattern();
      if (path.include(new PartialPath(device, timeseriesId))
          && modification.getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  private static void doModifyChunkMetaData(ModEntry modification, IChunkMetadata metaData) {
    if (modification instanceof TreeDeletionEntry) {
      TreeDeletionEntry deletion = (TreeDeletionEntry) modification;
      metaData.insertIntoSortedDeletions(deletion.getTimeRange());
    }
  }

  /** Methods for modification in memory table */
  public static List<List<TimeRange>> constructDeletionList(
      AlignedPath partialPath,
      IMemTable memTable,
      List<Pair<ModEntry, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    List<List<TimeRange>> deletionList = new ArrayList<>();
    for (String measurement : partialPath.getMeasurementList()) {
      List<TimeRange> columnDeletionList = new ArrayList<>();
      columnDeletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
      for (ModEntry modification :
          ModificationUtils.getModificationsForMemTable(memTable, modsToMemtable)) {
        if (modification instanceof TreeDeletionEntry) {
          TreeDeletionEntry deletion = (TreeDeletionEntry) modification;
          PartialPath fullPath = partialPath.concatNode(measurement);
          if (deletion.matchesFull(fullPath) && deletion.getTimeRange().getMax() > timeLowerBound) {
            long lowerBound = Math.max(deletion.getTimeRange().getMin(), timeLowerBound);
            columnDeletionList.add(new TimeRange(lowerBound, deletion.getTimeRange().getMax()));
          }
        }
      }
      deletionList.add(TimeRange.sortAndMerge(columnDeletionList));
    }
    return deletionList;
  }

  /**
   * construct a deletion list from a memtable.
   *
   * @param memTable memtable
   * @param timeLowerBound time watermark
   */
  public static List<TimeRange> constructDeletionList(
      MeasurementPath partialPath,
      IMemTable memTable,
      List<Pair<ModEntry, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (ModEntry modification : getModificationsForMemTable(memTable, modsToMemtable)) {
      if (modification instanceof TreeDeletionEntry) {
        TreeDeletionEntry deletion = (TreeDeletionEntry) modification;
        if (deletion.matchesFull(partialPath)
            && deletion.getTimeRange().getMax() > timeLowerBound) {
          long lowerBound = Math.max(deletion.getTimeRange().getMin(), timeLowerBound);
          deletionList.add(new TimeRange(lowerBound, deletion.getTimeRange().getMax()));
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }

  private static List<ModEntry> getModificationsForMemTable(
      IMemTable memTable, List<Pair<ModEntry, IMemTable>> modsToMemtable) {
    List<ModEntry> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<ModEntry, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }

  public static List<ModEntry> sortAndMerge(List<ModEntry> modifications) {
    modifications.sort(null);
    List<ModEntry> result = new ArrayList<>();
    if (!modifications.isEmpty()) {
      TreeDeletionEntry current = new TreeDeletionEntry((TreeDeletionEntry) modifications.get(0));
      for (int i = 1; i < modifications.size(); i++) {
        TreeDeletionEntry del = (TreeDeletionEntry) modifications.get(i);
        if (current.intersects(del)) {
          current.merge(del);
        } else {
          result.add(current);
          current = new TreeDeletionEntry(del);
        }
      }
      result.add(current);
    }
    return result;
  }
}
