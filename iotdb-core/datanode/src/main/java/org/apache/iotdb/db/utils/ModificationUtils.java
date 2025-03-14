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
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

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
      List<? extends IChunkMetadata> chunkMetaData, List<Modification> modifications) {
    for (IChunkMetadata metaData : chunkMetaData) {
      for (Modification modification : modifications) {
        // The case modification.getFileOffset() == metaData.getOffsetOfChunkHeader()
        // is not supposed to exist as getFileOffset() is offset containing full chunk,
        // while getOffsetOfChunkHeader() returns the chunk header offset
        if (modification.getFileOffset() > metaData.getOffsetOfChunkHeader()) {
          doModifyChunkMetaData(modification, metaData);
        }
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
      List<AlignedChunkMetadata> chunkMetaData, List<List<Modification>> modifications) {
    for (AlignedChunkMetadata metaData : chunkMetaData) {
      List<IChunkMetadata> valueChunkMetadataList = metaData.getValueChunkMetadataList();
      // deal with each sub sensor
      for (int i = 0; i < valueChunkMetadataList.size(); i++) {
        IChunkMetadata v = valueChunkMetadataList.get(i);
        if (v != null) {
          List<Modification> modificationList = modifications.get(i);
          for (Modification modification : modificationList) {
            // The case modification.getFileOffset() == metaData.getOffsetOfChunkHeader()
            // is not supposed to exist as getFileOffset() is offset containing full chunk,
            // while getOffsetOfChunkHeader() returns the chunk header offset
            if (modification.getFileOffset() > v.getOffsetOfChunkHeader()) {
              doModifyChunkMetaData(modification, v);
            }
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
      Collection<Modification> modifications, IDeviceID device, long startTime, long endTime)
      throws IllegalPathException {
    for (Modification modification : modifications) {
      PartialPath path = modification.getPath();
      if (path.include(new PartialPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD))
          && ((Deletion) modification).getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isTimeseriesDeletedByMods(
      Collection<Modification> modifications,
      IDeviceID device,
      String timeseriesId,
      long startTime,
      long endTime)
      throws IllegalPathException {
    for (Modification modification : modifications) {
      PartialPath path = modification.getPath();
      if (path.include(new PartialPath(device, timeseriesId))
          && ((Deletion) modification).getTimeRange().contains(startTime, endTime)) {
        return true;
      }
    }
    return false;
  }

  private static void doModifyChunkMetaData(Modification modification, IChunkMetadata metaData) {
    if (modification instanceof Deletion) {
      Deletion deletion = (Deletion) modification;
      metaData.insertIntoSortedDeletions(deletion.getTimeRange());
    }
  }

  /** Methods for modification in memory table */
  public static List<List<TimeRange>> constructDeletionList(
      AlignedPath partialPath,
      IMemTable memTable,
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    List<List<TimeRange>> deletionList = new ArrayList<>();
    for (String measurement : partialPath.getMeasurementList()) {
      List<TimeRange> columnDeletionList = new ArrayList<>();
      columnDeletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
      for (Modification modification :
          ModificationUtils.getModificationsForMemtable(memTable, modsToMemtable)) {
        if (modification instanceof Deletion) {
          Deletion deletion = (Deletion) modification;
          PartialPath fullPath = partialPath.concatNode(measurement);
          if (deletion.getPath().matchFullPath(fullPath)
              && deletion.getEndTime() > timeLowerBound) {
            long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
            columnDeletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
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
      List<Pair<Modification, IMemTable>> modsToMemtable,
      long timeLowerBound) {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (Modification modification : getModificationsForMemtable(memTable, modsToMemtable)) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPath().matchFullPath(partialPath)
            && deletion.getEndTime() > timeLowerBound) {
          long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
          deletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }

  private static List<Modification> getModificationsForMemtable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable) {
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<Modification, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }

  public static boolean isDeviceDeletedByMods(
      Collection<Modification> currentModifications, ITimeIndex currentTimeIndex, IDeviceID device)
      throws IllegalPathException {
    if (currentTimeIndex == null) {
      return false;
    }
    Optional<Long> startTime = currentTimeIndex.getStartTime(device);
    Optional<Long> endTime = currentTimeIndex.getEndTime(device);
    if (startTime.isPresent() && endTime.isPresent()) {
      return isDeviceDeletedByMods(currentModifications, device, startTime.get(), endTime.get());
    }
    return false;
  }
}
