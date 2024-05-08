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
 *
 */

package org.apache.iotdb.db.sync.datasource;

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * This class provides below functions
 *
 * <p>1) Save many deletion time-intervals.
 *
 * <p>2) Merge overlap intervals to 1 interval.
 *
 * <p>3) Check whether 1 time-range's data has been deleted according to saved deletion
 * time-intervals.
 *
 * <p>4) Check whether 1 time-point's data has been deleted according to saved deletion
 * time-intervals.
 *
 * <p>5) For time-ascending batch data, provide better-performance method to check whether 1
 * time-point's data has been deleted.
 */
public class DeletionGroup {
  // TreeMap: StartTime => EndTime
  private TreeMap<Long, Long> delIntervalMap;

  public enum DeletedType {
    NO_DELETED, // Mo data has been deleted
    PARTIAL_DELETED, // Partial data has been deleted
    FULL_DELETED // All data has been deleted
  }

  public static class IntervalCursor {
    Iterator<Map.Entry<Long, Long>> iter = null;
    boolean subsequentNoDelete = false;
    public long startTime;
    public long endTime;

    public void reset() {
      iter = null;
      subsequentNoDelete = false;
    }
  }

  public DeletionGroup() {
    delIntervalMap = new TreeMap<>();
  }

  /**
   * Insert delete time interval data for every deletion.
   *
   * @param startTime
   * @param endTime
   */
  public void addDelInterval(long startTime, long endTime) {
    if (startTime > endTime) {
      throw new IllegalArgumentException("addDelInterval(), error: startTime > endTime.");
    }

    // == pay attention, intervalMap's Entries are not overlap.
    Map.Entry<Long, Long> startEntry = delIntervalMap.floorEntry(startTime);
    Map.Entry<Long, Long> endEntry = delIntervalMap.floorEntry(endTime);

    if ((startEntry != null) && (startTime <= startEntry.getValue())) {
      startTime = startEntry.getKey();
    }
    if ((endEntry != null) && (endTime < endEntry.getValue())) {
      endTime = endEntry.getValue();
    }

    // == find existing overlap entries and remove them
    Map<Long, Long> overlapEntries = delIntervalMap.subMap(startTime, true, endTime, true);
    Iterator<Map.Entry<Long, Long>> iter = overlapEntries.entrySet().iterator();
    while (iter.hasNext()) {
      iter.next();
      iter.remove();
    }

    delIntervalMap.put(startTime, endTime); // add new deletion interval
  }

  /**
   * If this object has no deletion data (i.e delIntervalMap is empty), return true
   *
   * @return
   */
  public boolean isEmpty() {
    return delIntervalMap.isEmpty();
  }

  /**
   * Check the deletion-state of the data-points of specific time range according to the info of
   * .mods
   *
   * @param startTime - the start time of data set, inclusive
   * @param endTime - the end time of data set, inclusive
   * @return - Please refer to the definition of DeletedType
   */
  public DeletedType checkDeletedState(long startTime, long endTime) {
    if (delIntervalMap.isEmpty()) {
      return DeletedType.NO_DELETED;
    }

    if (startTime > endTime) {
      throw new IllegalArgumentException("checkDeletedState(), error: startTime > endTime.");
    }

    Map.Entry<Long, Long> startEntry = delIntervalMap.floorEntry(startTime);
    Map.Entry<Long, Long> endEntry = delIntervalMap.floorEntry(endTime);

    if (!Objects.equals(startEntry, endEntry)) {
      return DeletedType.PARTIAL_DELETED;
    }

    // == when (startEntry == endEntry == null)
    if (startEntry == null) {
      return DeletedType.NO_DELETED;
    }

    if (startTime > startEntry.getValue()) {
      return DeletedType.NO_DELETED;
    }

    if (endTime <= startEntry.getValue()) {
      return DeletedType.FULL_DELETED;
    }

    return DeletedType.PARTIAL_DELETED;
  }

  /**
   * Check whether this timestamp's data has been deleted according to .mods info and data timestamp
   *
   * @param ts - data timestamp
   * @return
   */
  public boolean isDeleted(long ts) {
    if (delIntervalMap.isEmpty()) {
      return false;
    }

    Map.Entry<Long, Long> entry = delIntervalMap.floorEntry(ts);
    if (entry == null) {
      return false;
    }

    if (ts > entry.getValue()) {
      return false;
    }

    return true;
  }

  /**
   * Check whether ascending timestamp batch data have been deleted according to .mods info. This
   * method has better performance than method isDeleted(long ts) for time-ascending bath data.
   *
   * <p>Note1: This method is only used for processing time-ascending batch data.
   *
   * <p>Note2: Input parameter intervalCursor must be 1 variable. For first calling this method,
   * need use new variable intervalCursor or call intervalCursor.reset(). Then continue using same
   * variable intervalCursor for consequent calling.
   *
   * @param ts
   * @param intervalCursor
   * @return
   */
  public boolean isDeleted(long ts, IntervalCursor intervalCursor) {
    if (delIntervalMap.isEmpty()) {
      return false;
    }

    // == for first calling
    if (intervalCursor.iter == null) {
      Long floorKey = delIntervalMap.floorKey(ts);
      if (floorKey == null) {
        intervalCursor.iter = delIntervalMap.entrySet().iterator();
        intervalCursor.startTime = delIntervalMap.firstKey();
        intervalCursor.endTime = delIntervalMap.firstEntry().getValue();
        return false;
      }

      intervalCursor.iter = delIntervalMap.tailMap(floorKey, true).entrySet().iterator();
      Map.Entry<Long, Long> entry = intervalCursor.iter.next();
      intervalCursor.startTime = entry.getKey();
      intervalCursor.endTime = entry.getValue();
    }

    if (intervalCursor.subsequentNoDelete) {
      return false;
    }

    while (true) {
      if (ts < intervalCursor.startTime) {
        return false;
      }
      if (ts <= intervalCursor.endTime) {
        return true;
      }

      if (intervalCursor.iter.hasNext()) {
        Map.Entry<Long, Long> entry = intervalCursor.iter.next();
        intervalCursor.startTime = entry.getKey();
        intervalCursor.endTime = entry.getValue();
        continue;
      } else {
        intervalCursor.subsequentNoDelete = true;
        break;
      }
    }

    return false;
  }

  @TestOnly
  public TreeMap<Long, Long> getDelIntervalMap() {
    return delIntervalMap;
  }
}
