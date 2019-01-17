/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.filenodev2;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.db.engine.filenode.OverflowChangeType;

/**
 * This class is used to store the TsFile status.<br>
 *
 * @author liukun
 */
public class TimeIntervalTsFile implements Serializable {

  private static final long serialVersionUID = -4309683416067212549L;
  public OverflowChangeType overflowChangeType;
  private int baseDirIndex;
  private String relativePath;
  private Map<String, Long> startTimeMap;
  private Map<String, Long> endTimeMap;
  private Set<String> mergeChanged = new HashSet<>();

  /**
   * construct function for TimeIntervalTsFile.
   */
  public TimeIntervalTsFile(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap,
      OverflowChangeType type,
      int baseDirIndex, String relativePath) {

    this.overflowChangeType = type;
    this.baseDirIndex = baseDirIndex;
    this.relativePath = relativePath;

    this.startTimeMap = startTimeMap;
    this.endTimeMap = endTimeMap;

  }

  /**
   * This is just used to construct a new TsFile.
   */
  public TimeIntervalTsFile(OverflowChangeType type, String relativePath) {

    this.overflowChangeType = type;
    this.relativePath = relativePath;

    startTimeMap = new HashMap<>();
    endTimeMap = new HashMap<>();
  }

  public void setStartTime(String deviceId, long startTime) {

    startTimeMap.put(deviceId, startTime);
  }

  /**
   * get start time.
   *
   * @param deviceId -Map key
   * @return -start time
   */
  public long getStartTime(String deviceId) {

    if (startTimeMap.containsKey(deviceId)) {
      return startTimeMap.get(deviceId);
    } else {
      return -1;
    }
  }

  public Map<String, Long> getStartTimeMap() {

    return startTimeMap;
  }

  public void setStartTimeMap(Map<String, Long> startTimeMap) {

    this.startTimeMap = startTimeMap;
  }

  public void setEndTime(String deviceId, long timestamp) {

    this.endTimeMap.put(deviceId, timestamp);
  }

  /**
   * get end time for given device.
   *
   * @param deviceId -id of device
   * @return -end time of the device
   */
  public long getEndTime(String deviceId) {

    if (endTimeMap.get(deviceId) == null) {
      return -1;
    }
    return endTimeMap.get(deviceId);
  }

  public Map<String, Long> getEndTimeMap() {

    return endTimeMap;
  }

  public void setEndTimeMap(Map<String, Long> endTimeMap) {

    this.endTimeMap = endTimeMap;
  }

  /**
   * remove given device'startTime start time and end time.
   *
   * @param deviceId -id of the device
   */
  public void removeTime(String deviceId) {

    startTimeMap.remove(deviceId);
    endTimeMap.remove(deviceId);
  }

  /**
   * get file path.
   */
  public String getFilePath() {

    if (relativePath == null) {
      return relativePath;
    }
    return new File(Directories.getInstance().getTsFileFolder(baseDirIndex), relativePath)
        .getPath();
  }

  public String getRelativePath() {

    return relativePath;
  }

  public void setRelativePath(String relativePath) {

    this.relativePath = relativePath;
  }

  public boolean checkEmpty() {

    return startTimeMap.isEmpty() && endTimeMap.isEmpty();
  }

  /**
   * clear the member variable of the given object.
   */
  public void clear() {

    startTimeMap.clear();
    endTimeMap.clear();
    mergeChanged.clear();
    overflowChangeType = OverflowChangeType.NO_CHANGE;
    relativePath = null;
  }

  /**
   * change file type corresponding to the given param.
   */
  public void changeTypeToChanged(FileNodeProcessorStatus fileNodeProcessorState) {

    if (fileNodeProcessorState == FileNodeProcessorStatus.MERGING_WRITE) {
      overflowChangeType = OverflowChangeType.MERGING_CHANGE;
    } else {
      overflowChangeType = OverflowChangeType.CHANGED;
    }
  }

  public void addMergeChanged(String deviceId) {

    mergeChanged.add(deviceId);
  }

  public Set<String> getMergeChanged() {

    return mergeChanged;
  }

  public void clearMergeChanged() {

    mergeChanged.clear();
  }

  /**
   * judge whether the time interval is closed.
   */
  public boolean isClosed() {

    return !endTimeMap.isEmpty();

  }

  /**
   * back up the time interval of tsfile.
   */
  public TimeIntervalTsFile backUp() {

    Map<String, Long> startTimeMap = new HashMap<>(this.startTimeMap);
    Map<String, Long> endTimeMap = new HashMap<>(this.endTimeMap);
    return new TimeIntervalTsFile(startTimeMap, endTimeMap, overflowChangeType, baseDirIndex,
        relativePath);
  }

  @Override
  public int hashCode() {

    final int prime = 31;
    int result = 1;
    result = prime * result + ((endTimeMap == null) ? 0 : endTimeMap.hashCode());
    result = prime * result + ((relativePath == null) ? 0 : relativePath.hashCode());
    result = prime * result + ((overflowChangeType == null) ? 0 : overflowChangeType.hashCode());
    result = prime * result + ((startTimeMap == null) ? 0 : startTimeMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimeIntervalTsFile other = (TimeIntervalTsFile) obj;
    if (endTimeMap == null) {
      if (other.endTimeMap != null) {
        return false;
      }
    } else if (!endTimeMap.equals(other.endTimeMap)) {
      return false;
    }
    if (relativePath == null) {
      if (other.relativePath != null) {
        return false;
      }
    } else if (!relativePath.equals(other.relativePath)) {
      return false;
    }
    if (overflowChangeType != other.overflowChangeType) {
      return false;
    }
    if (startTimeMap == null) {
      if (other.startTimeMap != null) {
        return false;
      }
    } else if (!startTimeMap.equals(other.startTimeMap)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "TimeIntervalTsFile [relativePath=" + relativePath + ", overflowChangeType="
        + overflowChangeType
        + ", startTimeMap=" + startTimeMap + ", endTimeMap=" + endTimeMap + ", mergeChanged="
        + mergeChanged
        + "]";
  }

}
