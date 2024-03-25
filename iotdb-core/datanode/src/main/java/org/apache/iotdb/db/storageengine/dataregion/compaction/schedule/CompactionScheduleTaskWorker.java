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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class CompactionScheduleTaskWorker implements Callable<Void> {

  private static final Logger logger = LoggerFactory.getLogger(CompactionScheduleTaskWorker.class);
  private final List<DataRegion> dataRegionList;
  private final int workerId;
  private final int workerNum;

  public CompactionScheduleTaskWorker(
      List<DataRegion> dataRegionList, int workerId, int workerNum) {
    this.dataRegionList = dataRegionList;
    this.workerId = workerId;
    this.workerNum = workerNum;
  }

  @Override
  public Void call() {
    while (true) {
      try {
        Thread.sleep(IoTDBDescriptor.getInstance().getConfig().getCompactionScheduleIntervalInMs());
        if (!StorageEngine.getInstance().isAllSgReady()) {
          continue;
        }
        List<DataRegion> dataRegionListSnapshot = new ArrayList<>(dataRegionList);
        List<DataRegion> dataRegionsToScheduleCompaction = new ArrayList<>();
        for (int i = 0; i < dataRegionListSnapshot.size(); i++) {
          if (i % workerNum != workerId) {
            continue;
          }
          dataRegionsToScheduleCompaction.add(dataRegionListSnapshot.get(i));
        }
        Collections.shuffle(dataRegionsToScheduleCompaction);
        for (DataRegion dataRegion : dataRegionsToScheduleCompaction) {
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          dataRegion.executeCompaction();
        }
      } catch (InterruptedException ignored) {
        logger.info(
            "[CompactionScheduleTaskWorker-{}] compaction schedule is interrupted", workerId);
        return null;
      }
    }
  }
}
