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

package org.apache.iotdb.db.quotas;

import org.apache.iotdb.db.engine.StorageEngineV2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// TODO: Regularly count the disk sizes of all regions included in this DataNode. Save in its
// memory.
public class DataNodeSizeStore {

  private final StorageEngineV2 storageEngine;
  private final Map<Integer, Long> dataRegionDisk;
  private final ScheduledExecutorService scheduledExecutorService;
  private List<Integer> dataRegionIds;

  public DataNodeSizeStore() {
    storageEngine = StorageEngineV2.getInstance();
    dataRegionDisk = new HashMap<>();
    dataRegionIds = new ArrayList<>();
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> calculateRegionSize(dataRegionIds), 0, 30, TimeUnit.MILLISECONDS);
  }

  public void calculateRegionSize(List<Integer> dataRegionIds) {
    storageEngine.getDiskSizeByDataRegion(dataRegionDisk, dataRegionIds);
  }

  public Map<Integer, Long> getDataRegionDisk() {
    return dataRegionDisk;
  }

  public void setDataRegionIds(List<Integer> dataRegionIds) {
    this.dataRegionIds = dataRegionIds;
  }
}
