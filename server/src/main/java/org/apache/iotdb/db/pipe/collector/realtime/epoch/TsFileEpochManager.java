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

package org.apache.iotdb.db.pipe.collector.realtime.epoch;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TsFileEpochManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileEpochManager.class);

  private static final String[] EMPTY_MEASUREMENT_ARRAY = new String[0];

  private final Map<String, TsFileEpoch> filePath2Epoch = new HashMap<>();

  public PipeRealtimeCollectEvent bindPipeTsFileInsertionEvent(
      PipeTsFileInsertionEvent event, TsFileResource resource) {
    final String filePath = resource.getTsFilePath();

    // this would not happen, but just in case
    filePath2Epoch.computeIfAbsent(
        filePath,
        path -> {
          LOGGER.info("TsFileEpoch not found for TsFile {}, creating a new one", path);
          return new TsFileEpoch(path);
        });

    return new PipeRealtimeCollectEvent(
        event,
        filePath2Epoch.remove(filePath),
        resource.getDevices().stream()
            .collect(Collectors.toMap(device -> device, device -> EMPTY_MEASUREMENT_ARRAY)),
        event.getPattern());
  }

  public PipeRealtimeCollectEvent bindPipeInsertNodeTabletInsertionEvent(
      PipeInsertNodeTabletInsertionEvent event, InsertNode node, TsFileResource resource) {
    return new PipeRealtimeCollectEvent(
        event,
        filePath2Epoch.computeIfAbsent(resource.getTsFilePath(), TsFileEpoch::new),
        Collections.singletonMap(node.getDevicePath().getFullPath(), node.getMeasurements()),
        event.getPattern());
  }
}
