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

package org.apache.iotdb.db.pipe.core.event.realtime;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.event.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.tablet.PipeTabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PipeRealtimeCollectEventManager {
  private static final Logger logger =
      LoggerFactory.getLogger(PipeRealtimeCollectEventManager.class);
  private final Map<String, TsFileEpoch> filePath2Epoch;

  public PipeRealtimeCollectEventManager() {
    this.filePath2Epoch = new HashMap<>();
  }

  public PipeRealtimeCollectEvent createRealtimeCollectEventFromTsFile(
      PipeTsFileInsertionEvent event, TsFileResource resource) {
    String filePath = resource.getTsFilePath();
    if (!filePath2Epoch.containsKey(filePath)) {
      logger.warn(String.format("Can not find TsFileEpoch for TsFile %s", filePath));
      filePath2Epoch.put(filePath, new TsFileEpoch(filePath));
    }
    return new PipeRealtimeCollectEvent(
        event,
        resource.getDevices().stream().collect(Collectors.toMap(s -> s, s -> new String[0])),
        filePath2Epoch.remove(filePath));
  }

  public PipeRealtimeCollectEvent createRealtimeCollectEventFromInsertNode(
      PipeTabletInsertionEvent event, InsertNode node, TsFileResource resource) {
    return new PipeRealtimeCollectEvent(
        event,
        Collections.singletonMap(node.getDevicePath().getFullPath(), node.getMeasurements()),
        filePath2Epoch.computeIfAbsent(resource.getTsFilePath(), TsFileEpoch::new));
  }
}
