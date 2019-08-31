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
package org.apache.iotdb.db.engine.storagegroup;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.fileSystem.IoTDBFile;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TsFileResource {

  private IoTDBFile file;

  public static final String RESOURCE_SUFFIX = ".resource";
  public static final String TEMP_SUFFIX = ".temp";

  /**
   * device -> start time
   */
  private Map<String, Long> startTimeMap;

  /**
   * device -> end time. It is null if it's an unsealed sequence tsfile
   */
  private Map<String, Long> endTimeMap;

  private TsFileProcessor processor;

  private ModificationFile modFile;

  private volatile boolean closed = false;

  /**
   * Chunk metadata list of unsealed tsfile. Only be set in a temporal TsFileResource in a query
   * process.
   */
  private List<ChunkMetaData> chunkMetaDatas;

  /**
   * Mem chunk data. Only be set in a temporal TsFileResource in a query process.
   */
  private ReadOnlyMemChunk readOnlyMemChunk;

  private ReentrantReadWriteLock mergeQueryLock = new ReentrantReadWriteLock();

  public TsFileResource(IoTDBFile file) {
    this.file = file;
    this.startTimeMap = new ConcurrentHashMap<>();
    this.endTimeMap = new HashMap<>();
    this.closed = true;
  }

  public TsFileResource(IoTDBFile file, TsFileProcessor processor) {
    this.file = file;
    this.startTimeMap = new ConcurrentHashMap<>();
    this.endTimeMap = new ConcurrentHashMap<>();
    this.processor = processor;
  }

  public TsFileResource(IoTDBFile file,
      Map<String, Long> startTimeMap,
      Map<String, Long> endTimeMap) {
    this.file = file;
    this.startTimeMap = startTimeMap;
    this.endTimeMap = endTimeMap;
    this.closed = true;
  }

  public TsFileResource(IoTDBFile file,
      Map<String, Long> startTimeMap,
      Map<String, Long> endTimeMap,
      ReadOnlyMemChunk readOnlyMemChunk,
      List<ChunkMetaData> chunkMetaDatas) {
    this.file = file;
    this.startTimeMap = startTimeMap;
    this.endTimeMap = endTimeMap;
    this.chunkMetaDatas = chunkMetaDatas;
    this.readOnlyMemChunk = readOnlyMemChunk;
  }

  public void serialize() throws IOException {
    try (OutputStream outputStream = new BufferedOutputStream(
        new FileOutputStream(file + RESOURCE_SUFFIX + TEMP_SUFFIX))) {
      ReadWriteIOUtils.write(this.startTimeMap.size(), outputStream);
      for (Entry<String, Long> entry : this.startTimeMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      ReadWriteIOUtils.write(this.endTimeMap.size(), outputStream);
      for (Entry<String, Long> entry : this.endTimeMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }
    IoTDBFile src = new IoTDBFile(file + RESOURCE_SUFFIX + TEMP_SUFFIX);
    IoTDBFile dest = new IoTDBFile(file + RESOURCE_SUFFIX);
    dest.delete();
    FileUtils.moveFile(src, dest);
  }

  public void deSerialize() throws IOException {
    try (InputStream inputStream = new BufferedInputStream(
        new FileInputStream(file + RESOURCE_SUFFIX))) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      Map<String, Long> startTimes = new HashMap<>();
      for (int i = 0; i < size; i++) {
        String path = ReadWriteIOUtils.readString(inputStream);
        long time = ReadWriteIOUtils.readLong(inputStream);
        startTimes.put(path, time);
      }
      size = ReadWriteIOUtils.readInt(inputStream);
      Map<String, Long> endTimes = new HashMap<>();
      for (int i = 0; i < size; i++) {
        String path = ReadWriteIOUtils.readString(inputStream);
        long time = ReadWriteIOUtils.readLong(inputStream);
        endTimes.put(path, time);
      }
      this.startTimeMap = startTimes;
      this.endTimeMap = endTimes;
    }
  }

  public void updateStartTime(String device, long time) {
    long startTime = startTimeMap.getOrDefault(device, Long.MAX_VALUE);
    if (time < startTime) {
      startTimeMap.put(device, time);
    }
  }

  public void updateEndTime(String device, long time) {
    long endTime = endTimeMap.getOrDefault(device, Long.MIN_VALUE);
    if (time > endTime) {
      endTimeMap.put(device, time);
    }
  }

  public boolean fileExists() {
    return new IoTDBFile(file + RESOURCE_SUFFIX).exists();
  }

  public void forceUpdateEndTime(String device, long time) {
      endTimeMap.put(device, time);
  }

  public List<ChunkMetaData> getChunkMetaDatas() {
    return chunkMetaDatas;
  }

  public ReadOnlyMemChunk getReadOnlyMemChunk() {
    return readOnlyMemChunk;
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

  public IoTDBFile getFile() {
    return file;
  }

  public long getFileSize() {
    return file.length();
  }

  public Map<String, Long> getStartTimeMap() {
    return startTimeMap;
  }

  public void setEndTimeMap(Map<String, Long> endTimeMap) {
    this.endTimeMap = endTimeMap;
  }

  public Map<String, Long> getEndTimeMap() {
    return endTimeMap;
  }

  public boolean isClosed() {
    return closed;
  }

  public void close() throws IOException {
    closed = true;
    if (modFile != null) {
      modFile.close();
      modFile = null;
    }
    processor = null;
    chunkMetaDatas = null;
  }

  public TsFileProcessor getUnsealedFileProcessor() {
    return processor;
  }

  public ReentrantReadWriteLock getMergeQueryLock() {
    return mergeQueryLock;
  }

  public void removeModFile() throws IOException {
    getModFile().remove();
    modFile = null;
  }

  public void remove() {
    file.delete();
    new IoTDBFile(file.getPath() + RESOURCE_SUFFIX).delete();
    new IoTDBFile(file.getPath() + ModificationFile.FILE_SUFFIX).delete();
  }

  @Override
  public String toString() {
    return file.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TsFileResource that = (TsFileResource) o;
    return Objects.equals(file, that.file);
  }

  @Override
  public int hashCode() {
    return Objects.hash(file);
  }

  public void setClosed(boolean closed) {
    this.closed = closed;
  }
}
