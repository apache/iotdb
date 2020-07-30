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

import java.util.TreeMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SystemInfo {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private long totalTspInfoMemCost;
  private long arrayPoolMemCost;

  // processor -> mem cost of it
  private TreeMap<TsFileProcessor, Long> reportedTspMemCostMap = new TreeMap<>(
      (o1, o2) -> (int) (o2.getTsFileProcessorInfo().getTsFileProcessorMemCost() - o1
          .getTsFileProcessorInfo().getTsFileProcessorMemCost()));

  private static final double REJECT_PROPORTION = config.getRejectProportion();

  /**
   * Report applying a new out of buffered array to system. Attention: It should be invoked before
   * applying new OOB array actually.
   *
   * @param dataType data type of array
   * @param size     size of array
   * @return Return true if it's agreed when memory is enough.
   */
  public synchronized boolean applyNewOOBArray(TSDataType dataType, int size) {
    // if current memory is enough
    if (arrayPoolMemCost + totalTspInfoMemCost + dataType.getDataTypeSize() * size
        < config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
      arrayPoolMemCost += dataType.getDataTypeSize() * size;
      return true;
    } else {
      // invoke flush()
      flush();
      return false;
    }
  }

  /**
   * Report current mem cost of tsp to system.
   *
   * @param processor processor
   * @return Return true if it's agreed when memory is enough.
   */
  public synchronized boolean reportTsFileProcessorStatus(TsFileProcessor processor) {
    long variation;
    Long originalValue = reportedTspMemCostMap.get(processor);
    // update the mem cost of processor
    reportedTspMemCostMap
        .put(processor, processor.getTsFileProcessorInfo().getTsFileProcessorMemCost());
    if (originalValue == null) {
      variation = processor.getTsFileProcessorInfo().getTsFileProcessorMemCost();
    } else {
      variation = processor.getTsFileProcessorInfo().getTsFileProcessorMemCost() - originalValue;
    }

    if (this.totalTspInfoMemCost + variation
        < config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
      this.totalTspInfoMemCost += variation;
      return true;
    } else {
      return false;
    }

  }

  /**
   * Update the current mem cost of buffered array pool.
   *
   * @param increasingArraySize increasing size of buffered array
   */
  public synchronized void reportIncreasingArraySize(int increasingArraySize) {
    this.arrayPoolMemCost += increasingArraySize;
  }

  /**
   * Report releasing an out of buffered array to system. Attention: It should be invoked after
   * releasing.
   *
   * @param dataType data type of array
   * @param size     size of array
   */
  public synchronized void reportReleaseOOBArray(TSDataType dataType, int size) {
    this.arrayPoolMemCost -= dataType.getDataTypeSize() * size;
  }

  /**
   * Report resetting the mem cost of processor to system. It will be invoked after closing file.
   *
   * @param processor closing processor
   */
  public synchronized void resetTsFileProcessorStatus(TsFileProcessor processor) {
    if (reportedTspMemCostMap.containsKey(processor)) {
      this.totalTspInfoMemCost -= processor.getTsFileProcessorInfo().getTsFileProcessorMemCost();
      reportedTspMemCostMap.remove(processor);
    }
  }

  /**
   * Flush the tsfileProcessor with the max mem cost. If the queue size of flushing > threshold,
   * it's identified as flushing is in progress.
   */
  public void flush() {
    if (FlushManager.getInstance().getTsFileProcessorQueueSize() >= 1) {
      return;
    }

    // get the first processor which has the max mem cost
    TsFileProcessor flushedProcessor = reportedTspMemCostMap.firstKey();

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
