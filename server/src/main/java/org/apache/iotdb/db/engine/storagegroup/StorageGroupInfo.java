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
package org.apache.iotdb.db.engine.storagegroup;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.SystemInfo;

/**
 * The storageGroupInfo records the total memory cost of the Storage Group.
 */
public class StorageGroupInfo {

  private StorageGroupProcessor storageGroupProcessor;

  /**
   * The total Storage group memory cost
   */
  private AtomicLong memoryCost;

  /**
   * The threshold of reporting it's size to SystemInfo
   */
  private long storageGroupSizeReportThreshold = 
      IoTDBDescriptor.getInstance().getConfig().getStorageGroupSizeReportThreshold();

  private long lastReportedSize = 0L;

  /**
   * A set of all unclosed TsFileProcessors in this SG
   */
  private Set<TsFileProcessor> reportedTsps = new HashSet<>();

  public StorageGroupInfo(StorageGroupProcessor storageGroupProcessor) {
    this.storageGroupProcessor = storageGroupProcessor;
    memoryCost = new AtomicLong();
  }

  public StorageGroupProcessor getStorageGroupProcessor() {
    return storageGroupProcessor;
  }

  /**
   * When create a new TsFileProcessor, call this method
   */
  public void initTsFileProcessorInfo(TsFileProcessor tsFileProcessor) {
    if (reportedTsps.add(tsFileProcessor)) {
      memoryCost.getAndAdd(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize());
    }
  }

  public void addStorageGroupMemCost(long cost) {
    memoryCost.getAndAdd(cost);
  }

  public void releaseStorageGroupMemCost(long cost) {
    memoryCost.getAndAdd(-cost);
  }

  public long getSgMemCost() {
    return memoryCost.get();
  }

  public Set<TsFileProcessor> getAllReportedTsp() {
    return reportedTsps;
  }

  public boolean needToReportToSystem() {
    return memoryCost.get() - lastReportedSize > storageGroupSizeReportThreshold;
  }

  public void setLastReportedSize(long size) {
    lastReportedSize = size;
  }

  /**
   * When a TsFileProcessor is closing, remove it from reportedTsps, and report to systemInfo
   * to update SG cost.
   * 
   * @param tsFileProcessor
   */
  public void closeTsFileProcessorAndReportToSystem(TsFileProcessor tsFileProcessor) {
    reportedTsps.remove(tsFileProcessor);
    SystemInfo.getInstance().resetStorageGroupStatus(this);
  }
}
