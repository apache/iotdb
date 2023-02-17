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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.sync.externalpipe.operation.Operation;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class' function: 1) manage the all Data from Tsfile/DeleteFile etc. 2) It will simulate 1
 * operation queue to let consumers get. *
 */
public class PipeOpManager {
  private static final Logger logger = LoggerFactory.getLogger(PipeOpManager.class);

  TsFilePipe filePipe;

  /** ConcurrentHashMap: SG => PipeSGManager (DataIndex => OperatorBlock) */
  private Map<String, PipeOpSgManager> pipeSgManagerMap = new ConcurrentHashMap<>();

  // save current in-use and not-committed filePipeSerialNumber
  private TreeSet<Long> filePipeSerialNumberSet = new TreeSet<>();
  // == record the maximum filePipeSerialNumber ever existing in filePipeSerialNumberSet.
  private Long maxFilePipeSerialNumber = Long.MIN_VALUE;

  @FunctionalInterface
  public interface NewDataEventHandler {
    void handle(String sgName, long newDataBeginIndex, long newDataCount);
  }
  // Used to notify consumers that new data arrived.
  private NewDataEventHandler newDataEventHandler = null;

  public PipeOpManager(TsFilePipe filePipe) {
    this.filePipe = filePipe;
  }

  public Set<String> getSgSet() {
    return pipeSgManagerMap.keySet();
  }

  /**
   * Append 1 data BlockEntry to pipeOpManager
   *
   * @param sgName
   * @param opBlockEntry
   */
  public void appendOpBlock(String sgName, AbstractOpBlock opBlockEntry) {
    // == record pipeDataSerialNumber for future commit
    appendPipeDataSerialNumber(opBlockEntry.getPipeDataSerialNumber());

    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);

    if (pipeOpSgManager == null) {
      pipeOpSgManager = new PipeOpSgManager(sgName);
      pipeSgManagerMap.put(sgName, pipeOpSgManager);
    }

    long newDataBeginIndex = pipeOpSgManager.getNextIndex();
    long newDataCount = opBlockEntry.getDataCount();
    pipeOpSgManager.addPipeOpBlock(opBlockEntry);

    notifyNewDataArrive(sgName, newDataBeginIndex, newDataCount);
  }

  private void appendPipeDataSerialNumber(long pipeDataSerialNumber) {
    filePipeSerialNumberSet.add(pipeDataSerialNumber);

    if (pipeDataSerialNumber > maxFilePipeSerialNumber) {
      maxFilePipeSerialNumber = pipeDataSerialNumber;
    }
  }

  /**
   * Add 1 TsFileOpBlock to PipeOpManager. *
   *
   * @param sgName
   * @param tsFilename
   * @param modsFileFullName
   * @param pipeDataSerialNumber
   * @throws IOException
   */
  public void appendTsFileOpBlock(
      String sgName, String tsFilename, String modsFileFullName, long pipeDataSerialNumber)
      throws IOException {
    File file = new File(tsFilename);
    if (!file.exists()) {
      logger.error("appendTsFileOpBlock(), can not find TsFile: {}", tsFilename);
      throw new IOException("No TsFile: " + tsFilename);
    }

    TsFileOpBlock tsFileOpBlock =
        new TsFileOpBlock(sgName, tsFilename, modsFileFullName, pipeDataSerialNumber);
    appendOpBlock(sgName, tsFileOpBlock);
  }

  /**
   * Add 1 DeletionBlock to pipeOpSgManager.
   *
   * @param sgName - StorageGroup Name
   * @param deletion
   * @param pipeDataSerialNumber
   */
  public void appendDeletionOpBlock(String sgName, Deletion deletion, long pipeDataSerialNumber) {
    // == check whether deletion path is valid
    try {
      if (!deletion.getPath().matchPrefixPath(new PartialPath(sgName))) {
        return;
      }
    } catch (IllegalPathException e) {
      logger.error("appendDeletionOpBlock(), error sgName {}", sgName, e);
      return;
    }

    DeletionOpBlock deletionOpBlock =
        new DeletionOpBlock(
            sgName,
            deletion.getPath(),
            deletion.getStartTime(),
            deletion.getEndTime(),
            pipeDataSerialNumber);

    appendOpBlock(sgName, deletionOpBlock);
  }

  /**
   * Use SG and index to get Operation from all data files (Tsfile/.modes etc.). 1 Operation may
   * contain multiple data points.
   *
   * @param sgName
   * @param index
   * @return
   */
  public Operation getOperation(String sgName, long index, long length) throws IOException {
    logger.debug("getOperation(), sgName={}, index={}, length={}.", sgName, index, length);

    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("getOperation(), invalid sgName={}. continue.", sgName);
      return null;
    }

    return pipeOpSgManager.getOperation(index, length);
  }

  /**
   * check whether this data commitIndex will cause committing 1 or more complete opBlock
   *
   * @param sgName
   * @param commitIndex
   * @return
   * @throws IOException
   */
  public boolean opBlockNeedCommit(String sgName, long commitIndex) throws IOException {
    logger.debug("opBlockNeedCommit(), sgName={}, commitIndex={}.", sgName, commitIndex);

    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("opBlockNeedCommit(), invalid sgName={}. continue.", sgName);
      return false;
    }

    return pipeOpSgManager.opBlockNeedCommit(commitIndex);
  }

  /**
   * Get the committed Index . Note: The return result may be not same to the value set by last
   * commitData()
   *
   * @param sgName
   * @return
   */
  public long getCommittedIndex(String sgName) {
    return (getFirstAvailableIndex(sgName) - 1);
  }

  /**
   * Get the first available data in dedicated StorageGroup. Note: Even if those committed data may
   * still be available as long as it is not deleted.
   *
   * @param sgName
   * @return
   */
  public long getFirstAvailableIndex(String sgName) {
    try {
      return pipeSgManagerMap.get(sgName).getFirstAvailableIndex();
    } catch (NullPointerException e) {
      logger.error("getFirstAvailableIndex(), Can not find sgName: {}.", sgName);
      throw new IllegalArgumentException(
          "getFirstAvailableIndex(), Can not find sgName: " + sgName);
    }
  }

  /**
   * Calculate and commit the filePipeSerialNumber
   *
   * @param filePipeSerialNumberList, committed filePipeSerialNumber List. Pay attention: the data
   *     in list can be discrete
   */
  private void commitFilePipe(List<Long> filePipeSerialNumberList) {
    if (filePipeSerialNumberList.isEmpty()) {
      return;
    }

    if (filePipeSerialNumberSet.isEmpty()) {
      logger.error("commitFilePipe(), filePipeSerialNumberSet should not be empty.");
      return;
    }

    long minNum = filePipeSerialNumberSet.first();
    for (long filePipeSerialNumber : filePipeSerialNumberList) {
      if (!filePipeSerialNumberSet.remove(filePipeSerialNumber)) {
        logger.error("commitFilePipe(), invalid filePipeSerialNumber={}.", filePipeSerialNumber);
      }
    }

    // In real product, filePipe will not be null.
    // Only in test cases, filePipe can be null.
    if (filePipe == null) { // only for test
      return;
    }

    if (!filePipeSerialNumberSet.isEmpty()) {
      if (filePipeSerialNumberSet.first() > minNum) {
        filePipe.commit(filePipeSerialNumberSet.first() - 1);
      }
      return;
    }

    filePipe.commit(maxFilePipeSerialNumber);
  }

  /**
   * Get the number of in-using opBlocks in PipeOpManager. If return 0, means all FilePipes in
   * PipeOpManager have been consumed.
   *
   * @return
   */
  public int getInUseOpBlockNum() {
    return filePipeSerialNumberSet.size();
  }

  /**
   * Check whether PipeOpManager has no data(OpBlocks)
   *
   * @return True - PipeOpManager has no Pipe data
   */
  public boolean isEmpty() {
    return filePipeSerialNumberSet.isEmpty();
  }

  /**
   * Notify that the data (whose index <= committedIndex) has been committed. May remove the data
   * entry whose whole data's index <= committedIndex.
   *
   * @param sgName, StorageGroup Name
   * @param committedIndex, the data (whose index <= committedIndex) may be removed.
   */
  public void commitData(String sgName, long committedIndex) {
    logger.debug(
        "PipeOpManager commitData(), sgName={}, committedIndex={}.", sgName, committedIndex);

    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("commitData(), invalid sgName = {}, continue.", sgName);
      return;
    }

    // == Calculate the filePipeSerialNumber that need to be committed.
    List<Long> filePipeSerialNumberList = pipeOpSgManager.commitData(committedIndex);
    commitFilePipe(filePipeSerialNumberList);
  }

  /**
   * Get the data index next to the last available data of StorageGroup
   *
   * @param sgName
   * @return
   */
  public long getNextIndex(String sgName) {
    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("getNextIndex(), can not find database: {}.", sgName);
      return Long.MIN_VALUE;
    }

    return pipeOpSgManager.getNextIndex();
  }

  /**
   * Set NewDataEventHandler to get notification when new data arrive. This function is optionally
   * used.
   *
   * @param newDataEventHandler
   */
  public void setNewDataEventHandler(NewDataEventHandler newDataEventHandler) {
    this.newDataEventHandler = newDataEventHandler;
  }

  private void notifyNewDataArrive(String sgName, long newDataBeginIndex, long newDataCount) {
    if (newDataEventHandler == null) {
      return;
    }

    newDataEventHandler.handle(sgName, newDataBeginIndex, newDataCount);
  }

  /** release the resource of PipeSrcManager */
  public void close() {
    // == use commitData to release all PipeSrcEntry's resource
    for (String sgName : pipeSgManagerMap.keySet()) {
      commitData(sgName, Long.MAX_VALUE);
    }
  }
}
