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
package org.apache.iotdb.db.engine.compaction.schedule;

import org.apache.iotdb.db.engine.storagegroup.TsFileManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class compactionScheduledWorker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger("COMPACTION");

  private final int threadId;

  private Map<Integer, List<TsFileManager>> dataRegionMap;

  public compactionScheduledWorker(int threadId, Map<Integer, List<TsFileManager>> dataRegionMap) {
    this.threadId = threadId;
    this.dataRegionMap = dataRegionMap;
  }

  @Override
  public void run() {
    try {
      if (!dataRegionMap.containsKey(threadId)) {
        return;
      }
      for (TsFileManager tsFileManager : dataRegionMap.get(threadId)) {
        List<Long> timePartitions = new ArrayList<>(tsFileManager.getTimePartitions());
        // sort the time partition from largest to smallest
        timePartitions.sort(Comparator.reverseOrder());
        for (long timePartition : timePartitions) {
          CompactionScheduler.scheduleCompaction(tsFileManager, timePartition);
        }
      }
    } catch (Throwable e) {
      logger.error("Meet error in compaction schedule.", e);
    }
  }
}
