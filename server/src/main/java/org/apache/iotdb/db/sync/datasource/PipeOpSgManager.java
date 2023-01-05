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

import org.apache.iotdb.db.sync.externalpipe.operation.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/** This class manage the operation data for 1 StorageGroup */
public class PipeOpSgManager {
  private static final Logger logger = LoggerFactory.getLogger(PipeOpSgManager.class);

  private String storageGroupName;

  /** ConcurrentHashMap: OperatorIndex => OperatorBlock */
  private ConcurrentSkipListMap<Long, AbstractOpBlock> opBlockMap = new ConcurrentSkipListMap<>();

  // record First Operator's index
  private long beginIndex = 0;
  // Operators number of this StorageGroup
  private long dataCount = 0;
  private long committedIndex = -1;

  public PipeOpSgManager(String storageGroupName) {
    this.storageGroupName = storageGroupName;
  }

  /**
   * Add 1 new operation block to SG
   *
   * @param opBlock, 1 new operation block
   */
  public synchronized void addPipeOpBlock(AbstractOpBlock opBlock) {
    long nextIndex = beginIndex + dataCount;

    opBlock.setBeginIndex(nextIndex);
    opBlockMap.put(nextIndex, opBlock);
    dataCount += opBlock.getDataCount();
  }

  /**
   * Get Operations from current StorageGroup. returned Operations rang is [beginIndex, endIndex),
   * endIndex <= (beginIndex+length). Note: Real returned Operations size can be <= length
   *
   * @param beginIndex, the start beginIndex of operation
   * @param length, the max number of operations
   * @return
   * @throws IOException
   */
  public synchronized Operation getOperation(long beginIndex, long length) throws IOException {
    if (beginIndex >= (this.beginIndex + dataCount)) {
      return null;
    }

    Map.Entry<Long, AbstractOpBlock> opBlockEntry = opBlockMap.floorEntry(beginIndex);
    if ((opBlockEntry == null)) {
      logger.error(
          "getOperation(), Error. invalid beginIndex {} for StorageGroup {}",
          beginIndex,
          storageGroupName);
      throw new IOException(
          "getOperation(), Invalid beginIndex "
              + beginIndex
              + " for StorageGroup "
              + storageGroupName);
    }

    return opBlockEntry.getValue().getOperation(beginIndex, length);
  }

  /**
   * Check whether this data commit-index will cause committing some opBlocks
   *
   * @param commitIndex
   * @return
   * @throws IOException
   */
  public synchronized boolean opBlockNeedCommit(long commitIndex) throws IOException {
    if ((commitIndex < this.beginIndex) || (commitIndex >= (this.beginIndex + dataCount))) {
      logger.error("opBlockNeedCommit(), invalid commitIndex={}", commitIndex);
      throw new IOException("opBlockNeedCommit(), invalid commitIndex=" + commitIndex);
    }

    if (commitIndex <= this.committedIndex) {
      return false;
    }

    Long opBlockBeginIndex = opBlockMap.floorKey(commitIndex);
    if (opBlockBeginIndex == null) {
      logger.error("opBlockNeedCommit(), invalid commitIndex={}", commitIndex);
      throw new IOException("opBlockNeedCommit(), invalid commitIndex=" + commitIndex);
    }

    Long currentOpBlockBeginIndex = opBlockMap.floorKey(this.committedIndex);
    if (currentOpBlockBeginIndex == null) {
      return true;
    }

    return opBlockBeginIndex > currentOpBlockBeginIndex;
  }

  /**
   * Get the first available data. Note: Even if those committed data may still be available as long
   * as it is not deleted.
   *
   * @return
   */
  public synchronized long getFirstAvailableIndex() {
    return beginIndex;
  }

  /**
   * Get Next Operation Index after last operation.
   *
   * @return
   */
  public synchronized long getNextIndex() {
    return beginIndex + dataCount;
  }

  public synchronized boolean isEmpty() {
    return (dataCount == 0);
  }

  /**
   * Notify that the data (whose index <= committedIndex) has been committed. May remove the data
   * entry whose whole data's index <= committedIndex.
   *
   * @param committedIndex
   * @return If successful, return filePipeSerialNumber. Otherwise, return Long.MIN_VALUE.
   */
  public synchronized List<Long> commitData(long committedIndex) {
    List<Long> filePipeSerialNumberList = new LinkedList<>();

    if (this.committedIndex < committedIndex) {
      this.committedIndex = committedIndex;
    }

    if (dataCount == 0) {
      return filePipeSerialNumberList;
    }

    if (committedIndex
        < (opBlockMap.firstKey() + opBlockMap.firstEntry().getValue().getDataCount() - 1)) {
      logger.debug(
          "commitData(), the first DataSrcEntry is still used, so need not remove it. storageGroupName={}, committedIndex={}",
          storageGroupName,
          committedIndex);
      return filePipeSerialNumberList;
    }

    Iterator<Map.Entry<Long, AbstractOpBlock>> iter = opBlockMap.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<Long, AbstractOpBlock> entry = iter.next();
      long nextIndex = entry.getKey() + entry.getValue().getDataCount();
      if (nextIndex <= (committedIndex + 1)) {
        AbstractOpBlock dataSrcEntry = entry.getValue();
        dataSrcEntry.close(); // release resource
        filePipeSerialNumberList.add(dataSrcEntry.getPipeDataSerialNumber());
        beginIndex = nextIndex;
        dataCount -= entry.getValue().getDataCount();
        iter.remove();
      } else {
        break;
      }
    }

    return filePipeSerialNumberList;
  }
}
