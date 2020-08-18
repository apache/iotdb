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
import java.util.TreeSet;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.SystemInfo;

/**
 * The storageGroupInfo records the total memory cost of the Storage Group.
 */
public class StorageGroupInfo {

  /**
   * Report the SG memory cost to SystemInfo if the memory cost increasing more
   * than this threshold.
   */
  private long storageGroupReportThreshold;

  /**
   * The total memory cost of the unsealed TsFileResources in this SG
   */
  private long unsealedResourceMemCost;

  /**
   * The total memory cost of TEXT data in this SG
   */
  private long bytesMemCost;

  /**
   * The total memory cost of ChunkMetadata in this SG
   */
  private long chunkMetadataMemCost;

  /**
   * The total memory cost of WALs in this SG
   */
  private long walMemCost;

  /**
   * A set of all unclosed TsFileProcessors in this SG
   */
  private Set<TsFileProcessor> reportedTsps = new HashSet<>();

  public StorageGroupInfo() {
    storageGroupReportThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getStorageGroupMemBlockSize();
    unsealedResourceMemCost = 0;
    bytesMemCost = 0;
    chunkMetadataMemCost = 0;
    walMemCost = 0;
  }

  /**
   * When create a new TsFileProcessor, call this method to report it
   */
  public void reportTsFileProcessorInfo(TsFileProcessor tsFileProcessor) {
    if (reportedTsps.add(tsFileProcessor)) {
      walMemCost += IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();
    }
  }

  public void addUnsealedResourceMemCost(long cost) {
    unsealedResourceMemCost += cost;
  }

  public void addChunkMetadataMemCost(long cost) {
    chunkMetadataMemCost += cost;
  }

  public void addBytesMemCost(long cost) {
    bytesMemCost += cost;
  }

  /**
   * called by TSPInfo when closing a TSP
   */
  public void resetUnsealedResourceMemCost(long cost) {
    unsealedResourceMemCost -= cost;
  }

  /**
   * called by TSPInfo when closing a TSP
   */
  public void resetChunkMetadataMemCost(long cost) {
    chunkMetadataMemCost -= cost;
  }

  /**
   * called by TSPInfo when a memTable contains TEXT data flushed
   */
  public void resetBytesMemCost(long cost) {
    bytesMemCost -= cost;
  }

  /**
   * called by TSPInfo when closing a TSP
   */
  public void resetWalMemCost(long cost) {
    walMemCost -= cost;
  }

  public boolean checkIfNeedToReportStatusToSystem() {
    return getSgMemCost() >= storageGroupReportThreshold;
  }

  public long getSgMemCost() {
    return unsealedResourceMemCost + bytesMemCost + chunkMetadataMemCost + walMemCost;
  }

  public void addStorageGroupReportThreshold(long value) {
    storageGroupReportThreshold += value;
  }

  public TsFileProcessor getLargestTsFileProcessor() {
    TreeSet<TsFileProcessor> tsps = new TreeSet<>(
        (o1, o2) -> Long.compare(o2.getTsFileProcessorInfo().getTsFileProcessorMemCost(),
            o1.getTsFileProcessorInfo().getTsFileProcessorMemCost()));
    for (TsFileProcessor tsp : reportedTsps) {
      tsps.add(tsp);
    }
    return tsps.first();
  }

  /**
   * When a TsFileProcessor is closing, remove it from reportedTsps, and report to systemInfo
   * to update SG cost.
   * 
   * @param tsFileProcessor
   */
  public void closeTsFileProcessorAndReportToSystem(TsFileProcessor tsFileProcessor) {
    reportedTsps.remove(tsFileProcessor);
    SystemInfo.getInstance().resetStorageGroupInfoStatus(this);
  }
}
