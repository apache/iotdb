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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SystemInfo {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  long totalTspInfoMemCost;
  long arrayPoolMemCost;

  boolean reject = false;
  // temporary value
  private final double rejectProportion = 0.9;
  private final double flushProportion = 0.6;

  /**
   * Inform system applying a new out of buffered array.
   * Attention: It should be invoked before applying new OOB array actually.
   *
   * @param dataType data type of array
   * @param size size of array
   * @return 如果同意，则返回true；否则返回false。
   */
  public synchronized boolean applyNewOOBArray(TSDataType dataType, int size) {
    if (!reject) {

    }
    // if current memory is enough
    if (arrayPoolMemCost + totalTspInfoMemCost + dataType.getDataTypeSize() * size
        < config.getAllocateMemoryForWrite() * rejectProportion) {
      arrayPoolMemCost += dataType.getDataTypeSize() * size;
      return reject;
    } else {
      this.reject = true;
      flush();
      return reject;
    }
  }

  /**
   * 通知system自身tsp内存将发生变化。 当内存够用时，返回同意，否则不同意。 若不同意，则设置自身reject为false，触发flush。
   *
   * @param processor processor
   */
  public synchronized boolean reportTsFileProcessorStatus(TsFileProcessor processor) {
    long accumulatedCost = processor.getTsFileProcessorInfo().getAccumulatedMemCost();
    processor.getTsFileProcessorInfo().clearAccumulatedMemCost();

    if (this.totalTspInfoMemCost + accumulatedCost
        < config.getAllocateMemoryForWrite() * rejectProportion) {
      this.totalTspInfoMemCost += accumulatedCost;
      return false;
    } else {
      return true;
    }

  }

  /**
   * @param IncreasingSize increasing size of buffered array
   */
  public void reportIncreasingArraySize(int IncreasingSize) {
    this.arrayPoolMemCost += IncreasingSize;
  }

  /**
   * Inform system releasing a out of buffered array.
   * Attention: It should be invoked after releasing.
   *
   * @param dataType data type of array
   * @param size size of array
   */
  public void reportReleaseOOBArray(TSDataType dataType, int size) {
    this.arrayPoolMemCost -=  dataType.getDataTypeSize() * size;
  }

  /**
   * 通知system将重置processor的内存占用量 （关闭文件后调用）。 设置自身reject为false
   *
   * @param processor processor
   * @param original  原有值
   */
  public void resetTsFileProcessorStatus(TsFileProcessor processor, long original) {

  }

  /**
   * 触发刷写。 若发现队列队长大于k，则不触发，否则触发。
   */
  public void flush() {

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
