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

package org.apache.iotdb.db.newsync.datasource;

import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.pipe.external.operation.Operation;

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

  private TreeSet<Long> filePipeSerialNumberSet = new TreeSet<>();
  // == record the maximum filePipeSerialNumber ever existing in filePipeSerialNumberSet.
  private Long maxFilePipeSerialNumber = Long.MIN_VALUE;

  public PipeOpManager(TsFilePipe filePipe) {
    this.filePipe = filePipe;
  }

  public Set<String> getSgSet() {
    return pipeSgManagerMap.keySet();
  }

  /**
   * Append 1 dataSrcEntry to PipeSrcManager
   *
   * @param sgName
   * @param dataSrcEntry
   */
  public void appendDataSrc(String sgName, AbstractPipeOpBlock dataSrcEntry) {
    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);

    if (pipeOpSgManager == null) {
      pipeOpSgManager = new PipeOpSgManager(sgName);
      pipeSgManagerMap.put(sgName, pipeOpSgManager);
    }
    pipeOpSgManager.addPipeOpBlock(dataSrcEntry);
  }

  public void appendTsFile(String sgName, String tsFilename, long pipeDataSerialNumber)
      throws IOException {
    File file = new File(tsFilename);
    if (!file.exists()) {
      logger.error("appendTsFile(), can not find TsFile: {}", tsFilename);
      throw new IOException("No TsFile: " + tsFilename);
    }

    filePipeSerialNumberSet.add(pipeDataSerialNumber);
    if (pipeDataSerialNumber > maxFilePipeSerialNumber) {
      maxFilePipeSerialNumber = pipeDataSerialNumber;
    }

    TsfilePipeOpBlock tsfileDataSrcEntry =
        new TsfilePipeOpBlock(sgName, tsFilename, pipeDataSerialNumber);
    appendDataSrc(sgName, tsfileDataSrcEntry);
  }

  /**
   * Use SG and index to get Operation from all data files (Tsfile/.modes etc.) 1 Operation contains
   * multiple data.
   *
   * @param sgName
   * @param index
   * @return
   */
  public Operation getOperation(String sgName, long index, long length) throws IOException {
    logger.debug("getOperation(), sgName={}, index={}, length={}.", sgName, index, length);

    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("getOperation(), invalid sgName = {}. continue.", sgName);
      return null;
    }

    return pipeOpSgManager.getOperation(index, length);
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
    if (filePipeSerialNumberList.size() <= 0) {
      return;
    }

    if (filePipeSerialNumberSet.size() <= 0) {
      logger.error("commitFilePipe(), filePipeSerialNumberSet should not be empty.");
      return;
    }

    long minNum = filePipeSerialNumberSet.first();
    long maxNum = filePipeSerialNumberSet.last();
    for (long filePipeSerialNumber : filePipeSerialNumberList) {
      if (!filePipeSerialNumberSet.remove(filePipeSerialNumber)) {
        logger.error("commitFilePipe(), invalid filePipeSerialNumber={}.", filePipeSerialNumber);
      }
    }

    if (filePipeSerialNumberSet.size() > 0) {
      if (filePipeSerialNumberSet.first() > minNum) {
        filePipe.commit(filePipeSerialNumberSet.first() - 1);
      }
      return;
    }
    filePipe.commit(maxFilePipeSerialNumber);
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

  public long getNextIndex(String sgName) {
    PipeOpSgManager pipeOpSgManager = pipeSgManagerMap.get(sgName);
    if (pipeOpSgManager == null) {
      logger.error("getNextIndex(), can not find Storage Group: {}.", sgName);
      return Long.MIN_VALUE;
    }

    return pipeOpSgManager.getNextIndex();
  }

  /** release the resource of PipeSrcManager */
  public void close() {
    // == use commitData to release all PipeSrcEntry's resource
    for (String sgName : pipeSgManagerMap.keySet()) {
      commitData(sgName, Long.MAX_VALUE);
    }
  }
}
