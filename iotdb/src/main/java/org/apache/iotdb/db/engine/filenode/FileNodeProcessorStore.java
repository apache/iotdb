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
package org.apache.iotdb.db.engine.filenode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FileNodeProcessorStore is used to store information about FileNodeProcessor's status.
 * lastUpdateTime is changed and stored by BufferWrite flush or BufferWrite close.
 * emptyIntervalFileNode and newFileNodes are changed and stored by Overflow flush and
 * Overflow close. fileNodeProcessorState is changed and stored by the change of FileNodeProcessor's
 * status such as "work->merge merge->wait wait->work". numOfMergeFile is changed
 * and stored when FileNodeProcessor's status changes from work to merge.
 *
 * @author liukun
 */
public class FileNodeProcessorStore implements Serializable {

  private static final long serialVersionUID = -54525372941897565L;

  private boolean isOverflowed;
  private Map<String, Long> lastUpdateTimeMap;
  private IntervalFileNode emptyIntervalFileNode;
  private List<IntervalFileNode> newFileNodes;
  private int numOfMergeFile;
  private FileNodeProcessorStatus fileNodeProcessorStatus;

  /**
   * Constructor of FileNodeProcessorStore.
   * @param isOverflowed whether this FileNode contains unmerged Overflow operations.
   * @param lastUpdateTimeMap the timestamp of last data point of each device in this FileNode.
   * @param emptyIntervalFileNode a place holder when the FileNode contains no TsFile.
   * @param newFileNodes TsFiles in the FileNode.
   * @param fileNodeProcessorStatus the status of the FileNode.
   * @param numOfMergeFile the number of files already merged in one merge operation.
   */
  public FileNodeProcessorStore(boolean isOverflowed, Map<String, Long> lastUpdateTimeMap,
      IntervalFileNode emptyIntervalFileNode,
      List<IntervalFileNode> newFileNodes,
      FileNodeProcessorStatus fileNodeProcessorStatus,
      int numOfMergeFile) {
    this.isOverflowed = isOverflowed;
    this.lastUpdateTimeMap = lastUpdateTimeMap;
    this.emptyIntervalFileNode = emptyIntervalFileNode;
    this.newFileNodes = newFileNodes;
    this.fileNodeProcessorStatus = fileNodeProcessorStatus;
    this.numOfMergeFile = numOfMergeFile;
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
    return new HashMap<String, Long>(lastUpdateTimeMap);
  }

  public void setLastUpdateTimeMap(Map<String, Long> lastUpdateTimeMap) {
    this.lastUpdateTimeMap = lastUpdateTimeMap;
  }

  public IntervalFileNode getEmptyIntervalFileNode() {
    return emptyIntervalFileNode;
  }

  public void setEmptyIntervalFileNode(IntervalFileNode emptyIntervalFileNode) {
    this.emptyIntervalFileNode = emptyIntervalFileNode;
  }

  public List<IntervalFileNode> getNewFileNodes() {
    return newFileNodes;
  }

  public void setNewFileNodes(List<IntervalFileNode> newFileNodes) {
    this.newFileNodes = newFileNodes;
  }

  public int getNumOfMergeFile() {
    return numOfMergeFile;
  }

  public void setNumOfMergeFile(int numOfMergeFile) {
    this.numOfMergeFile = numOfMergeFile;
  }
}
