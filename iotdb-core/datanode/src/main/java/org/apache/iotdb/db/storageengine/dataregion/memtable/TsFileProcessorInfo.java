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

import java.util.concurrent.atomic.AtomicLong;

/** The TsFileProcessorInfo records the memory cost of this TsFileProcessor. */
public class TsFileProcessorInfo {

  /** Once tspInfo updated, report to storageGroupInfo that this TSP belongs to. */
  private final DataRegionInfo dataRegionInfo;

  /** memory occupation of unsealed TsFileResource, ChunkMetadata, WAL */
  private AtomicLong memCost;

  private final TsFileProcessorInfoMetrics metrics;

  public TsFileProcessorInfo(DataRegionInfo dataRegionInfo) {
    this.dataRegionInfo = dataRegionInfo;
    this.memCost = new AtomicLong(0);
    this.metrics =
        new TsFileProcessorInfoMetrics(dataRegionInfo.getDataRegion().getDatabaseName(), this);
    MetricService.getInstance().addMetricSet(metrics);
  }

  /** called in each insert */
  public void addTSPMemCost(long cost) {
    memCost.addAndGet(cost);
    dataRegionInfo.addStorageGroupMemCost(cost);
  }

  /** called when meet exception */
  public void releaseTSPMemCost(long cost) {
    dataRegionInfo.releaseStorageGroupMemCost(cost);
    memCost.addAndGet(-cost);
  }

  /** called when closing TSP */
  public void clear() {
    dataRegionInfo.releaseStorageGroupMemCost(memCost.get());
    memCost.set(0);
    MetricService.getInstance().removeMetricSet(metrics);
  }

  /** get memCost */
  public long getMemCost() {
    return memCost.get();
  }
}
