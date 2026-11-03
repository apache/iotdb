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
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.rescon.memory.WriteMemoryBlock;

/** The TsFileProcessorInfo records the memory cost of this TsFileProcessor. */
public class TsFileProcessorInfo {

  /** memory occupation of unsealed TsFileResource, ChunkMetadata, WAL */
  private final WriteMemoryBlock memoryBlock;

  private final TsFileProcessorInfoMetrics metrics;

  public TsFileProcessorInfo(DataRegionInfo dataRegionInfo) {
    this(dataRegionInfo, "TsFileMetadata");
  }

  public TsFileProcessorInfo(DataRegionInfo dataRegionInfo, String tsFilePath) {
    this.memoryBlock =
        dataRegionInfo
            .getWriteMemoryManager()
            .createBlock(tsFilePath, WriteMemoryBlock.Category.TSFILE_METADATA);
    this.metrics =
        new TsFileProcessorInfoMetrics(dataRegionInfo.getDataRegion().getDatabaseName(), this);
    MetricService.getInstance().addMetricSet(metrics);
  }

  /** called in each insert */
  public void addTSPMemCost(long cost) {
    memoryBlock.add(cost);
  }

  /** called when meet exception */
  public void releaseTSPMemCost(long cost) {
    memoryBlock.release(cost);
  }

  /** called when closing TSP */
  public void clear() {
    memoryBlock.close();
    MetricService.getInstance().removeMetricSet(metrics);
  }

  public WriteMemoryBlock getMemoryBlock() {
    return memoryBlock;
  }

  /** get memCost */
  public long getMemCost() {
    return memoryBlock.getAccountedMemoryInBytes();
  }
}
