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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * FileNodeProcessorStore is used to store information about FileNodeProcessor's status.
 * lastUpdateTime is changed and stored by BufferWrite flushMetadata or BufferWrite close.
 * emptyTsFileResource and sequenceFileList are changed and stored by Overflow flushMetadata and
 * Overflow close. fileNodeProcessorState is changed and stored by the change of FileNodeProcessor's
 * status such as "work->merge merge->wait wait->work". numOfMergeFile is changed
 * and stored when FileNodeProcessor's status changes from work to merge.
 */
public class FileNodeProcessorStoreV2 implements Serializable {

  private static final long serialVersionUID = -54525372941897565L;

  private boolean isOverflowed;
  private Map<String, Long> latestTimeMap;
  private List<TsFileResourceV2> sequenceFileList;
  private List<TsFileResourceV2> unSequenceFileList;
  private int numOfMergeFile;
  private FileNodeProcessorStatus fileNodeProcessorStatus;

  /**
   * Constructor of FileNodeProcessorStore.
   *
   * @param isOverflowed whether this FileNode contains unmerged Overflow operations.
   * @param latestTimeMap the timestamp of last data point of each device in this FileNode.
   * @param sequenceFileList TsFiles in the FileNode.
   * @param fileNodeProcessorStatus the status of the FileNode.
   * @param numOfMergeFile the number of files already merged in one merge operation.
   */
  public FileNodeProcessorStoreV2(boolean isOverflowed, Map<String, Long> latestTimeMap,
      List<TsFileResourceV2> sequenceFileList, FileNodeProcessorStatus fileNodeProcessorStatus,
      int numOfMergeFile) {
    this.isOverflowed = isOverflowed;
    this.latestTimeMap = latestTimeMap;
    this.sequenceFileList = sequenceFileList;
    this.fileNodeProcessorStatus = fileNodeProcessorStatus;
    this.numOfMergeFile = numOfMergeFile;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(this.isOverflowed, byteArrayOutputStream);
    // latestTimeMap
    ReadWriteIOUtils.write(latestTimeMap.size(), byteArrayOutputStream);
    for (Entry<String, Long> entry : latestTimeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteArrayOutputStream);
      ReadWriteIOUtils.write(entry.getValue(), byteArrayOutputStream);
    }
    ReadWriteIOUtils.write(this.sequenceFileList.size(), byteArrayOutputStream);
    for (TsFileResourceV2 tsFileResource : this.sequenceFileList) {
      tsFileResource.serialize(byteArrayOutputStream);
    }
    ReadWriteIOUtils.write(this.numOfMergeFile, byteArrayOutputStream);
    ReadWriteIOUtils.write(this.fileNodeProcessorStatus.serialize(), byteArrayOutputStream);
    // buffer array to outputstream
    byteArrayOutputStream.writeTo(outputStream);
  }

  public static FileNodeProcessorStoreV2 deSerialize(InputStream inputStream) throws IOException {
    boolean isOverflowed = ReadWriteIOUtils.readBool(inputStream);
    Map<String, Long> lastUpdateTimeMap = new HashMap<>();
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      long time = ReadWriteIOUtils.readLong(inputStream);
      lastUpdateTimeMap.put(path, time);
    }
    size = ReadWriteIOUtils.readInt(inputStream);
    List<TsFileResourceV2> newFileNodes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      newFileNodes.add(TsFileResourceV2.deSerialize(inputStream));
    }
    int numOfMergeFile = ReadWriteIOUtils.readInt(inputStream);
    FileNodeProcessorStatus fileNodeProcessorStatus = FileNodeProcessorStatus
        .deserialize(ReadWriteIOUtils.readShort(inputStream));

    return new FileNodeProcessorStoreV2(isOverflowed, lastUpdateTimeMap,
        newFileNodes, fileNodeProcessorStatus, numOfMergeFile);
  }

  public boolean isOverflowed() {
    return isOverflowed;
  }

  public void setOverflowed(boolean isOverflowed) {
    this.isOverflowed = isOverflowed;
  }

  public FileNodeProcessorStatus getFileNodeProcessorStatus() {
    return fileNodeProcessorStatus;
  }

  public void setFileNodeProcessorStatus(FileNodeProcessorStatus fileNodeProcessorStatus) {
    this.fileNodeProcessorStatus = fileNodeProcessorStatus;
  }

  public Map<String, Long> getLatestTimeMap() {
    return new HashMap<>(latestTimeMap);
  }

  public void setLatestTimeMap(Map<String, Long> latestTimeMap) {
    this.latestTimeMap = latestTimeMap;
  }

  public List<TsFileResourceV2> getSequenceFileList() {
    return sequenceFileList;
  }

  public void setSequenceFileList(List<TsFileResourceV2> sequenceFileList) {
    this.sequenceFileList = sequenceFileList;
  }

  public List<TsFileResourceV2> getUnSequenceFileList() {
    return unSequenceFileList;
  }

  public int getNumOfMergeFile() {
    return numOfMergeFile;
  }

  public void setNumOfMergeFile(int numOfMergeFile) {
    this.numOfMergeFile = numOfMergeFile;
  }
}
