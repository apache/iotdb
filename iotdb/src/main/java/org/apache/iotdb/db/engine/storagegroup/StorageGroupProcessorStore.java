/**
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
package org.apache.iotdb.db.engine.storagegroup;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StorageGroupProcessorStore is used to store information about StorageGroupProcessor's status.
 * lastUpdateTime is changed and stored by BufferWrite flush or BufferWrite close.
 * emptyTsFileInstance and newFileNodes are changed and stored by Overflow flush and
 * Overflow close. fileNodeProcessorState is changed and stored by the change of StorageGroupProcessor's
 * status such as "work->merge merge->wait wait->work". numOfMergeFile is changed
 * and stored when StorageGroupProcessor's status changes from work to merge.
 *
 * @author liukun
 */
public class StorageGroupProcessorStore implements Serializable {

  private static final long serialVersionUID = -54525372941897565L;

  private boolean isOverflowed;
  private Map<String, Long> lastUpdateTimeMap;
  private TsFileInstance emptyTsFileInstance;
  private List<TsFileInstance> newFileNodes;
  private int numOfMergeFile;
  private StorageGroupProcessorStatus storageGroupProcessorStatus;

  /**
   * Constructor of StorageGroupProcessorStore.
   * @param isOverflowed whether this FileNode contains unmerged Overflow operations.
   * @param lastUpdateTimeMap the timestamp of last data point of each device in this FileNode.
   * @param emptyTsFileInstance a place holder when the FileNode contains no TsFile.
   * @param newFileNodes TsFiles in the FileNode.
   * @param storageGroupProcessorStatus the status of the FileNode.
   * @param numOfMergeFile the number of files already merged in one merge operation.
   */
  public StorageGroupProcessorStore(boolean isOverflowed, Map<String, Long> lastUpdateTimeMap,
      TsFileInstance emptyTsFileInstance,
      List<TsFileInstance> newFileNodes,
      StorageGroupProcessorStatus storageGroupProcessorStatus,
      int numOfMergeFile) {
    this.isOverflowed = isOverflowed;
    this.lastUpdateTimeMap = lastUpdateTimeMap;
    this.emptyTsFileInstance = emptyTsFileInstance;
    this.newFileNodes = newFileNodes;
    this.storageGroupProcessorStatus = storageGroupProcessorStatus;
    this.numOfMergeFile = numOfMergeFile;
  }

  public boolean isOverflowed() {
    return isOverflowed;
  }

  public void setOverflowed(boolean isOverflowed) {
    this.isOverflowed = isOverflowed;
  }

  public StorageGroupProcessorStatus getStorageGroupProcessorStatus() {
    return storageGroupProcessorStatus;
  }

  public void setStorageGroupProcessorStatus(
      StorageGroupProcessorStatus storageGroupProcessorStatus) {
    this.storageGroupProcessorStatus = storageGroupProcessorStatus;
  }

  public Map<String, Long> getLastUpdateTimeMap() {
    return new HashMap<>(lastUpdateTimeMap);
  }

  public void setLastUpdateTimeMap(Map<String, Long> lastUpdateTimeMap) {
    this.lastUpdateTimeMap = lastUpdateTimeMap;
  }

  public TsFileInstance getEmptyTsFileInstance() {
    return emptyTsFileInstance;
  }

  public void setEmptyTsFileInstance(TsFileInstance emptyTsFileInstance) {
    this.emptyTsFileInstance = emptyTsFileInstance;
  }

  public List<TsFileInstance> getNewFileNodes() {
    return newFileNodes;
  }

  public void setNewFileNodes(List<TsFileInstance> newFileNodes) {
    this.newFileNodes = newFileNodes;
  }

  public int getNumOfMergeFile() {
    return numOfMergeFile;
  }

  public void setNumOfMergeFile(int numOfMergeFile) {
    this.numOfMergeFile = numOfMergeFile;
  }
}
