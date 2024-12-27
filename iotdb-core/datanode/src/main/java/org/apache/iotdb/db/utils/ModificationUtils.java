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

import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.SettleSelectorImpl;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ModificationUtils {

  private ModificationUtils() {
    // util class
  }

  // both ranges are closed
  public static boolean overlap(long startA, long endA, long startB, long endB) {
    return endB >= startA && startB <= endA;
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
      modifyValueColumns(metaData, modifications);
    }
    // if all sub sensors' chunk metadata are deleted, then remove the aligned chunk metadata
    // otherwise, set the deleted chunk metadata of some sensors to null
    chunkMetaData.removeIf(
        alignedChunkMetadata -> {
          // the whole aligned path need to be removed, only set to be true if all the sub sensors
          // are deleted
          // the whole aligned path is modified, set to be true if any sub sensor is modified
          return areAllValueColumnsDeleted(alignedChunkMetadata, false);
        });
  }

  private static void modifyValueColumns(
      AlignedChunkMetadata metaData, List<List<ModEntry>> valueColumnsModifications) {
    List<IChunkMetadata> valueChunkMetadataList = metaData.getValueChunkMetadataList();
    // deal with each sub sensor
    for (int j = 0; j < valueChunkMetadataList.size(); j++) {
      IChunkMetadata v = valueChunkMetadataList.get(j);
      if (v != null) {
        List<ModEntry> modificationList = valueColumnsModifications.get(j);
        for (ModEntry modification : modificationList) {
          doModifyChunkMetaData(modification, v);
        }
      }
    }
  }

  private static boolean areAllValueColumnsDeleted(
      AlignedChunkMetadata alignedChunkMetadata, boolean modified) {

    // the whole aligned path need to be removed, only set to be true if all the sub sensors
    // are deleted and ignoreAllNullRows is true
    boolean allValueColumnsAreDeleted = true;
    List<IChunkMetadata> valueChunkMetadataList = alignedChunkMetadata.getValueChunkMetadataList();
    for (int i = 0; i < valueChunkMetadataList.size(); i++) {
      IChunkMetadata valueChunkMetadata = valueChunkMetadataList.get(i);
      if (valueChunkMetadata == null) {
        continue;
      }
      // current sub sensor's chunk metadata is completely removed
      boolean currentRemoved = false;
      if (valueChunkMetadata.getDeleteIntervalList() != null) {
        for (TimeRange range : valueChunkMetadata.getDeleteIntervalList()) {
          if (range.contains(valueChunkMetadata.getStartTime(), valueChunkMetadata.getEndTime())) {
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
        allValueColumnsAreDeleted = false;
      }
    }
    alignedChunkMetadata.setModified(modified);
    return allValueColumnsAreDeleted;
  }

  public static void modifyAlignedChunkMetaData(
      List<AlignedChunkMetadata> chunkMetaData,
      List<ModEntry> timeColumnModifications,
      List<List<ModEntry>> valueColumnsModifications,
      boolean ignoreAllNullRows) {
    for (AlignedChunkMetadata metaData : chunkMetaData) {
      IChunkMetadata timeColumnChunkMetadata = metaData.getTimeChunkMetadata();

      for (ModEntry modification : timeColumnModifications) {
        doModifyChunkMetaData(modification, timeColumnChunkMetadata);
      }
      modifyValueColumns(metaData, valueColumnsModifications);
    }

    // if all sub sensors' chunk metadata are deleted and ignoreAllNullRows is true, then remove the
    // aligned chunk metadata
    // otherwise, set the deleted chunk metadata of some sensors to null
    chunkMetaData.removeIf(
        alignedChunkMetadata -> {
          // the whole aligned path is modified, set to be true if any sub sensor is modified
          boolean modified = false;

          // deal with time column
          IChunkMetadata timeColumnChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
          if (timeColumnChunkMetadata.getDeleteIntervalList() != null) {
            for (TimeRange range : timeColumnChunkMetadata.getDeleteIntervalList()) {
              if (range.contains(
                  timeColumnChunkMetadata.getStartTime(), timeColumnChunkMetadata.getEndTime())) {
                // all rows are deleted
                return true;
              } else {
                if (range.overlaps(
                    new TimeRange(
                        timeColumnChunkMetadata.getStartTime(),
                        timeColumnChunkMetadata.getEndTime()))) {
                  timeColumnChunkMetadata.setModified(true);
                  modified = true;
                }
              }
            }
          }

          boolean allValueColumnsAreDeleted =
              areAllValueColumnsDeleted(alignedChunkMetadata, modified);
          return ignoreAllNullRows && allValueColumnsAreDeleted;
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
  public static boolean isAllDeletedByMods(
      Collection<ModEntry> modifications, IDeviceID device, long startTime, long endTime) {
    for (ModEntry modification : modifications) {
      if (modification.affectsAll(device)
          && modification.getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isAllDeletedByMods(
      Collection<ModEntry> modifications, long startTime, long endTime) {
    if (modifications == null || modifications.isEmpty()) {
      return false;
    }
    for (ModEntry modification : modifications) {
      if ((modification.getTimeRange().contains(startTime, endTime))) {
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
      long endTime) {
    for (ModEntry modification : modifications) {
      if (modification.affects(device)
          && modification.affects(timeseriesId)
          && modification.getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  private static void doModifyChunkMetaData(ModEntry modification, IChunkMetadata metaData) {
    metaData.insertIntoSortedDeletions(modification.getTimeRange());
  }

  /** Methods for modification in memory table */
  public static List<List<TimeRange>> constructDeletionList(
      IDeviceID deviceID,
      List<String> measurementList,
      IMemTable memTable,
      List<Pair<ModEntry, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    if (measurementList.isEmpty()) {
      return Collections.emptyList();
    }
    List<ModEntry> modifications =
        ModificationUtils.getModificationsForMemtable(memTable, modsToMemtable);
    List<List<TimeRange>> deletionList = new ArrayList<>();
    for (String measurement : measurementList) {
      List<TimeRange> columnDeletionList = new ArrayList<>();
      columnDeletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
      for (ModEntry modification : modifications) {
        if (modification.affects(deviceID)
            && modification.affects(measurement)
            && modification.getEndTime() > timeLowerBound) {
          long lowerBound = Math.max(modification.getStartTime(), timeLowerBound);
          columnDeletionList.add(new TimeRange(lowerBound, modification.getEndTime()));
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
      IDeviceID deviceID,
      String measurement,
      IMemTable memTable,
      List<Pair<ModEntry, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (ModEntry modification : getModificationsForMemtable(memTable, modsToMemtable)) {
      if (modification.affects(deviceID)
          && modification.affects(measurement)
          && modification.getEndTime() > timeLowerBound) {
        long lowerBound = Math.max(modification.getStartTime(), timeLowerBound);
        deletionList.add(new TimeRange(lowerBound, modification.getEndTime()));
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }

  private static List<ModEntry> getModificationsForMemtable(
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

  public static boolean canMerge(TimeRange left, TimeRange right) {
    // [1,3] can merge with [4, 5]
    // [1,3] cannot merge with [5,6]
    // [Long.MIN,3] can merge with [Long.MIN, 5]
    long extendedRightMin = right.getMin() == Long.MIN_VALUE ? right.getMin() : right.getMin() - 1;
    return extendedRightMin <= left.getMax();
  }

  public static boolean canMerge(ModEntry left, ModEntry right) {
    if (!Objects.equals(left.getClass(), right.getClass())
        || !canMerge(left.getTimeRange(), right.getTimeRange())) {
      return false;
    }

    if (left instanceof TreeDeletionEntry) {
      return Objects.equals(
          ((TreeDeletionEntry) left).getPathPattern(),
          ((TreeDeletionEntry) right).getPathPattern());
    } else if (left instanceof TableDeletionEntry) {
      return Objects.equals(
          ((TableDeletionEntry) left).getPredicate(), ((TableDeletionEntry) right).getPredicate());
    }
    return false;
  }

  public static List<ModEntry> sortAndMerge(List<ModEntry> modifications) {
    modifications.sort(Comparator.comparing(ModEntry::getTimeRange));
    List<ModEntry> result = new ArrayList<>();
    if (!modifications.isEmpty()) {
      ModEntry current = modifications.get(0).clone();
      for (int i = 1; i < modifications.size(); i++) {
        ModEntry next = modifications.get(i);
        if (canMerge(current, next)) {
          current.getTimeRange().merge(next.getTimeRange());
        } else {
          result.add(current);
          current = next.clone();
        }
      }
      result.add(current);
    }
    return result;
  }
}
