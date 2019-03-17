/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.tsfiledata;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.bufferwrite.RestorableTsFileIOWriter;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.db.engine.filenode.OverflowChangeType;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

/**
 * This class is used to store one bufferwrite file status.<br>
 */
public class TsFileResource implements Serializable {

  private static final long serialVersionUID = -4309683416067212549L;

  private OverflowChangeType overflowChangeType;

  //the file index of `settled` folder in the Directories.
  private int baseDirIndex;
  private File file;
  private Map<String, Long> startTimeMap;
  private Map<String, Long> endTimeMap;
  private Set<String> mergeChanged = new HashSet<>();

  private transient ModificationFile modFile;

  /**
   * @param autoRead whether read the file to initialize startTimeMap and endTimeMap
   *
   */
  public TsFileResource(File file, boolean autoRead) throws IOException {
    this(new HashMap<>(), new HashMap<>(), OverflowChangeType.NO_CHANGE, file);
    if (autoRead) {
      //init startTime and endTime
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
        if (reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
          //this is a complete tsfile, and we can read the metadata directly.
          for (Map.Entry<String, TsDeviceMetadataIndex> deviceEntry : reader.readFileMetadata()
              .getDeviceMap().entrySet()) {
            startTimeMap.put(deviceEntry.getKey(), deviceEntry.getValue().getStartTime());
            endTimeMap.put(deviceEntry.getKey(), deviceEntry.getValue().getEndTime());
          }
        } else {
          //sadly, this is not a complete tsfile. we have to repair it bytes by bytes
          //TODO will implement it
          List<ChunkGroupMetaData> metaDataList = new ArrayList<>();
          reader.selfCheck(null, metaDataList, false);
          initTimeMapFromChunGroupMetaDatas(metaDataList);
        }
      }
    }
  }

  /**
   * @param writer an unclosed TsFile Writer
   */
  public TsFileResource(File file, RestorableTsFileIOWriter writer) {
    this(new HashMap<>(), new HashMap<>(), OverflowChangeType.NO_CHANGE, file);
    initTimeMapFromChunGroupMetaDatas(writer.getChunkGroupMetaDatas());
  }

  private void initTimeMapFromChunGroupMetaDatas(List<ChunkGroupMetaData> metaDataList) {
    for (ChunkGroupMetaData metaData : metaDataList) {
      long startTime = startTimeMap.getOrDefault(metaData.getDeviceID(), Long.MAX_VALUE);
      long endTime = endTimeMap.getOrDefault(metaData.getDeviceID(), Long.MIN_VALUE);
      for (ChunkMetaData chunk : metaData.getChunkMetaDataList()) {
        if (chunk.getStartTime() < startTime) {
          startTime = chunk.getStartTime();
        }
        if (chunk.getEndTime() > endTime) {
          endTime = chunk.getEndTime();
        }
      }
      startTimeMap.put(metaData.getDeviceID(), startTime);
      endTimeMap.put(metaData.getDeviceID(), endTime);
    }
  }


  public TsFileResource(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap,
      OverflowChangeType type, File file) {

    this.overflowChangeType = type;
    this.baseDirIndex = Directories.getInstance().getTsFileFolderIndex(file.getParentFile().getParent());
    this.file = file;

    this.startTimeMap = startTimeMap;
    this.endTimeMap = endTimeMap;
    this.modFile = new ModificationFile(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
  }


  public void setStartTime(String deviceId, long startTime) {

    startTimeMap.put(deviceId, startTime);
  }

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

  public void removeTime(String deviceId) {

    startTimeMap.remove(deviceId);
    endTimeMap.remove(deviceId);
  }

  public File getFile() {
    return file;
  }

  public int getBaseDirIndex() {
    return baseDirIndex;
  }

  public void setBaseDirIndex(int baseDirIndex) {
    this.baseDirIndex = baseDirIndex;
  }


  public boolean checkEmpty() {

    return startTimeMap.isEmpty() && endTimeMap.isEmpty();
  }

  public void clear() {
    startTimeMap.clear();
    endTimeMap.clear();
    mergeChanged.clear();
    overflowChangeType = OverflowChangeType.NO_CHANGE;
  }

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

  public boolean isClosed() {

    return !endTimeMap.isEmpty();

  }

  public TsFileResource backUp() {

    Map<String, Long> startTimeMapCopy = new HashMap<>(this.startTimeMap);
    Map<String, Long> endTimeMapCopy = new HashMap<>(this.endTimeMap);
    return new TsFileResource(startTimeMapCopy, endTimeMapCopy, overflowChangeType, file);
  }

  public Set<String> getDevices() {
    return this.startTimeMap.keySet();
  }

  @Override
  public int hashCode() {

    final int prime = 31;
    int result = 1;
    result = prime * result + ((endTimeMap == null) ? 0 : endTimeMap.hashCode());
    result = prime * result + ((file == null) ? 0 : file.hashCode());
    result = prime * result + ((overflowChangeType == null) ? 0 : overflowChangeType.hashCode());
    result = prime * result + ((startTimeMap == null) ? 0 : startTimeMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFileResource fileNode = (TsFileResource) o;
    return baseDirIndex == fileNode.baseDirIndex &&
            overflowChangeType == fileNode.overflowChangeType &&
            Objects.equals(file, fileNode.file) &&
            Objects.equals(startTimeMap, fileNode.startTimeMap) &&
            Objects.equals(endTimeMap, fileNode.endTimeMap) &&
            Objects.equals(mergeChanged, fileNode.mergeChanged);
  }

  @Override
  public String toString() {

    return String.format(
            "TsFileResource [file=%s,overflowChangeType=%s, startTimeMap=%s,"
                    + " endTimeMap=%s, mergeChanged=%s]",
            file.getAbsolutePath(), overflowChangeType, startTimeMap, endTimeMap, mergeChanged);
  }

  public OverflowChangeType getOverflowChangeType() {
    return overflowChangeType;
  }

  public void setOverflowChangeType(OverflowChangeType overflowChangeType) {
    this.overflowChangeType = overflowChangeType;
  }

  public synchronized ModificationFile getModFile() {
    if (modFile == null) {
      modFile = new ModificationFile(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
    }
    return modFile;
  }

  public boolean containsDevice(String deviceId) {
    return startTimeMap.containsKey(deviceId);
  }

  public void setModFile(ModificationFile modFile) {
    this.modFile = modFile;
  }

  public void close() throws IOException {
    modFile.close();
  }
}
