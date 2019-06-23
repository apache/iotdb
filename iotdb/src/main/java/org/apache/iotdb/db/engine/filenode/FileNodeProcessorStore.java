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
package org.apache.iotdb.db.engine.filenode;

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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * FileNodeProcessorStore is used to store information about FileNodeProcessor's status.
 * lastUpdateTime is changed and stored by BufferWrite flush or BufferWrite setCloseMark.
 * emptyTsFileResource and newFileNodes are changed and stored by Overflow flush and
 * Overflow setCloseMark. fileNodeProcessorState is changed and stored by the change of FileNodeProcessor's
 * status such as "work->merge merge->wait wait->work". numOfMergeFile is changed
 * and stored when FileNodeProcessor's status changes from work to merge.
 */
public class FileNodeProcessorStore implements Serializable {

  private static final long serialVersionUID = -54525372941897565L;

  private boolean isOverflowed;
  private Map<String, Long> lastUpdateTimeMap;
  private TsFileResource emptyTsFileResource;
  private List<TsFileResource> newFileNodes;
  private int numOfMergeFile;
  private FileNodeProcessorStatus fileNodeProcessorStatus;

  /**
   * Constructor of FileNodeProcessorStore.
   *
   * @param isOverflowed whether this FileNode contains unmerged Overflow operations.
   * @param lastUpdateTimeMap the timestamp of last data point of each device in this FileNode.
   * @param emptyTsFileResource a place holder when the FileNode contains no TsFile.
   * @param newFileNodes TsFiles in the FileNode.
   * @param fileNodeProcessorStatus the status of the FileNode.
   * @param numOfMergeFile the number of files already merged in one merge operation.
   */
  public FileNodeProcessorStore(boolean isOverflowed, Map<String, Long> lastUpdateTimeMap,
      TsFileResource emptyTsFileResource,
      List<TsFileResource> newFileNodes,
      FileNodeProcessorStatus fileNodeProcessorStatus,
      int numOfMergeFile) {
    this.isOverflowed = isOverflowed;
    this.lastUpdateTimeMap = lastUpdateTimeMap;
    this.emptyTsFileResource = emptyTsFileResource;
    this.newFileNodes = newFileNodes;
    this.fileNodeProcessorStatus = fileNodeProcessorStatus;
    this.numOfMergeFile = numOfMergeFile;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(this.isOverflowed, byteArrayOutputStream);
    // lastUpdateTimeMap
    ReadWriteIOUtils.write(lastUpdateTimeMap.size(), byteArrayOutputStream);
    for (Entry<String, Long> entry : lastUpdateTimeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteArrayOutputStream);
      ReadWriteIOUtils.write(entry.getValue(), byteArrayOutputStream);
    }
    this.emptyTsFileResource.serialize(byteArrayOutputStream);
    ReadWriteIOUtils.write(this.newFileNodes.size(), byteArrayOutputStream);
    for (TsFileResource tsFileResource : this.newFileNodes) {
      tsFileResource.serialize(byteArrayOutputStream);
    }
    ReadWriteIOUtils.write(this.numOfMergeFile, byteArrayOutputStream);
    ReadWriteIOUtils.write(this.fileNodeProcessorStatus.serialize(), byteArrayOutputStream);
    // buffer array to outputstream
    byteArrayOutputStream.writeTo(outputStream);
  }

  public static FileNodeProcessorStore deSerialize(InputStream inputStream) throws IOException {
    boolean isOverflowed = ReadWriteIOUtils.readBool(inputStream);
    Map<String, Long> lastUpdateTimeMap = new HashMap<>();
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      long time = ReadWriteIOUtils.readLong(inputStream);
      lastUpdateTimeMap.put(path, time);
    }
    TsFileResource emptyTsFileResource = TsFileResource.deSerialize(inputStream);
    size = ReadWriteIOUtils.readInt(inputStream);
    List<TsFileResource> newFileNodes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      newFileNodes.add(TsFileResource.deSerialize(inputStream));
    }
    int numOfMergeFile = ReadWriteIOUtils.readInt(inputStream);
    FileNodeProcessorStatus fileNodeProcessorStatus = FileNodeProcessorStatus
        .deserialize(ReadWriteIOUtils.readShort(inputStream));

    return new FileNodeProcessorStore(isOverflowed, lastUpdateTimeMap, emptyTsFileResource,
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

  public Map<String, Long> getLastUpdateTimeMap() {
    return new HashMap<>(lastUpdateTimeMap);
  }

  public void setLastUpdateTimeMap(Map<String, Long> lastUpdateTimeMap) {
    this.lastUpdateTimeMap = lastUpdateTimeMap;
  }

  public TsFileResource getEmptyTsFileResource() {
    return emptyTsFileResource;
  }

  public void setEmptyTsFileResource(TsFileResource emptyTsFileResource) {
    this.emptyTsFileResource = emptyTsFileResource;
  }

  public List<TsFileResource> getNewFileNodes() {
    return newFileNodes;
  }

  public void setNewFileNodes(List<TsFileResource> newFileNodes) {
    this.newFileNodes = newFileNodes;
  }

  public int getNumOfMergeFile() {
    return numOfMergeFile;
  }

  public void setNumOfMergeFile(int numOfMergeFile) {
    this.numOfMergeFile = numOfMergeFile;
  }
}
