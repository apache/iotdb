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
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.UnsealedTsFileProcessorV2;
import org.apache.iotdb.db.engine.filenode.OverflowChangeType;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.SealedTsFileV2;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFileV2;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public abstract class TsFileResourceV2 {

  private File file;

  // device -> start time
  private Map<String, Long> startTimeMap;

  // device -> end time
  // null if it's an unsealed tsfile
  private Map<String, Long> endTimeMap;

  private UnsealedTsFileProcessorV2 unsealedFileProcessor;

  private transient ModificationFile modFile;

  public TsFileResourceV2(File file) {
    this.file = file;
    this.startTimeMap = new HashMap<>();
    this.endTimeMap = new HashMap<>();
  }

  public TsFileResourceV2(File file, Map<String, Long> startTimeMap, Map<String, Long> endTimeMap) {
    this.file = file;
    this.startTimeMap = startTimeMap;
    this.endTimeMap = endTimeMap;
  }

  public void updateStartTime(String device, long time) {
    startTimeMap.putIfAbsent(device, time);
    long startTime = startTimeMap.get(device);
    if (time < startTimeMap.get(device)) {
      startTimeMap.put(device, startTime);
    }
  }

  public File getFile() {
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

  public ModificationFile getModFile() {
    return modFile;
  }

  public void setModFile(ModificationFile modFile) {
    this.modFile = modFile;
  }

  public void serialize(OutputStream outputStream) throws IOException {
//    ReadWriteIOUtils.write(this.overflowChangeType.serialize(), outputStream);
//    ReadWriteIOUtils.write(this.baseDirIndex, outputStream);
    ReadWriteIOUtils.writeIsNull(this.file, outputStream);
    if (this.file != null) {
      ReadWriteIOUtils.write(file.getAbsolutePath(), outputStream);
    }
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
//    ReadWriteIOUtils.write(mergeChanged.size(), outputStream);
//    for (String mergeChangedElement : this.mergeChanged) {
//      ReadWriteIOUtils.write(mergeChangedElement, outputStream);
//    }
  }

  public static TsFileResourceV2 deSerialize(InputStream inputStream) throws IOException {
    OverflowChangeType overflowChangeType = OverflowChangeType
        .deserialize(ReadWriteIOUtils.readShort(inputStream));
    int baseDirIndex = ReadWriteIOUtils.readInt(inputStream);
    boolean hasRelativePath = ReadWriteIOUtils.readIsNull(inputStream);

    File file = null;
    if (hasRelativePath) {
      String relativePath = ReadWriteIOUtils.readString(inputStream);
      file = new File(Directories.getInstance().getTsFileFolder(baseDirIndex), relativePath);
    }
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
//    size = ReadWriteIOUtils.readInt(inputStream);
//    Set<String> mergeChanaged = new HashSet<>();
//    for (int i = 0; i < size; i++) {
//      String path = ReadWriteIOUtils.readString(inputStream);
//      mergeChanaged.add(path);
//    }
//    tsFileResource.mergeChanged = mergeChanaged;

    return endTimes.isEmpty() ? new UnsealedTsFileV2(file, startTimes, endTimes)
        : new SealedTsFileV2(file, startTimes, endTimes);
  }

  public abstract TSFILE_TYPE getTsFileType();

  public enum TSFILE_TYPE {
    UNSEALED, SEALED
  }


  public boolean isClosed() {

    return !endTimeMap.isEmpty();

  }

  public UnsealedTsFileProcessorV2 getUnsealedFileProcessor() {
    return unsealedFileProcessor;
  }
}
