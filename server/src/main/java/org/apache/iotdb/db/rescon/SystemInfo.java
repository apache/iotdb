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
 */

package org.apache.iotdb.db.rescon;

import java.util.Iterator;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SystemInfo {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  long totalTspInfoMemCost;
  long arrayPoolMemCost;

  // temporary value
  private final double rejectProportion = 0.9;
  private final int flushQueueThreshold = 6;

  /**
   * Inform system applying a new out of buffered array. Attention: It should be invoked before
   * applying new OOB array actually.
   *
   * @param dataType data type of array
   * @param size     size of array
   * @return Return true if it's agreed when memory is enough.
   */
  public synchronized boolean applyNewOOBArray(TSDataType dataType, int size) {
    // if current memory is enough
    if (arrayPoolMemCost + totalTspInfoMemCost + dataType.getDataTypeSize() * size
        < config.getAllocateMemoryForWrite() * rejectProportion) {
      arrayPoolMemCost += dataType.getDataTypeSize() * size;
      return true;
    } else {
      // invoke flush()
      flush();
      return false;
    }
  }

  /**
   * Inform system increasing mem cost of tsp.
   *
   * @param processor processor
   * @return Return true if it's agreed when memory is enough.
   */
  public synchronized boolean reportTsFileProcessorStatus(TsFileProcessor processor) {
    long accumulatedCost = processor.getTsFileProcessorInfo().getAccumulatedMemCost();
    // set the accumulated increasing mem cost to 0
    processor.getTsFileProcessorInfo().clearAccumulatedMemCost();

    if (this.totalTspInfoMemCost + accumulatedCost
        < config.getAllocateMemoryForWrite() * rejectProportion) {
      this.totalTspInfoMemCost += accumulatedCost;
      return true;
    } else {
      return false;
    }

  }

  /**
   * Update the current mem cost of buffered array pool
   *
   * @param increasingArraySize increasing size of buffered array
   */
  public void reportIncreasingArraySize(int increasingArraySize) {
    this.arrayPoolMemCost += increasingArraySize;
  }

  /**
   * Inform system releasing a out of buffered array. Attention: It should be invoked after
   * releasing.
   *
   * @param dataType data type of array
   * @param size     size of array
   */
  public void reportReleaseOOBArray(TSDataType dataType, int size) {
    this.arrayPoolMemCost -= dataType.getDataTypeSize() * size;
  }

  /**
   * 通知system将重置processor的内存占用量 （关闭文件后调用）。 设置自身reject为false
   *
   * @param processor processor
   * @param original  original value
   */
  public void resetTsFileProcessorStatus(TsFileProcessor processor, long original) {
    this.totalTspInfoMemCost -= original;
  }

  /**
   * Flush the tsfileProcessor with the max mem cost. If the queue size of flushing > threshold,
   * it's identified as flushing is in progress.
   */
  public void flush() {
    if (FlushManager.getInstance().getTsFileProcessorQueueSize() > flushQueueThreshold) {
      return;
    }

    Iterator<StorageGroupProcessor> allSGProcessors = StorageEngine.getInstance()
        .getProcessorMap().values().iterator();
    long maxMemCost = Long.MIN_VALUE;
    TsFileProcessor flushedProcessor = null;
    while (allSGProcessors.hasNext()) {
      StorageGroupProcessor storageGroupProcessor = allSGProcessors.next();

      // calculate the max mem cost of sequence tsfile
      Iterator<TsFileProcessor> sequenceTsFileProcessors = storageGroupProcessor
          .getWorkSequenceTsFileProcessors().iterator();

      while (sequenceTsFileProcessors.hasNext()) {
        TsFileProcessor sequenceTsFileProcessor = sequenceTsFileProcessors.next();
        if (sequenceTsFileProcessor.getTsFileProcessorInfo().getTsFileProcessorMemCost() > maxMemCost) {
          maxMemCost = sequenceTsFileProcessor.getTsFileProcessorInfo().getTsFileProcessorMemCost();
          flushedProcessor = sequenceTsFileProcessor;
        }
      }
      // calculate the max mem cost of unSequence tsfiles
      Iterator<TsFileProcessor> unSequenceTsFileProcessors = storageGroupProcessor
          .getWorkUnsequenceTsFileProcessors().iterator();

      while (unSequenceTsFileProcessors.hasNext()) {
        TsFileProcessor unSequenceTsFileProcessor = sequenceTsFileProcessors.next();
        if (unSequenceTsFileProcessor.getTsFileProcessorInfo().getTsFileProcessorMemCost()
            > maxMemCost) {
          maxMemCost = unSequenceTsFileProcessor.getTsFileProcessorInfo().getTsFileProcessorMemCost();
          flushedProcessor = unSequenceTsFileProcessor;
        }
      }
    }

    if (flushedProcessor != null) {
      flushedProcessor.asyncFlush();
    }

  }

  public static SystemInfo getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static SystemInfo instance = new SystemInfo();
  }
}
