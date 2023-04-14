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

package org.apache.iotdb.db.pipe.core.event.factory;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.event.PipeCollectEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.Collections;
import java.util.stream.Collectors;

public class PipeCollectorEventFactory {
  private long collectIndex;

  public PipeCollectorEventFactory() {
    this.collectIndex = 0;
  }

  public PipeCollectEvent createCollectorEvent(
      Event event, long timePartitionId, boolean isSeq, TsFileResource resource) {
    return new PipeCollectEvent(
        event,
        timePartitionId,
        isSeq,
        ++collectIndex,
        resource.getDevices().stream().collect(Collectors.toMap(s -> s, s -> new String[0])));
  }

  public PipeCollectEvent createCollectorEvent(
      Event event, long timePartitionId, boolean isSeq, InsertNode node) {
    return new PipeCollectEvent(
        event,
        timePartitionId,
        isSeq,
        ++collectIndex,
        Collections.singletonMap(node.getDevicePath().getFullPath(), node.getMeasurements()));
  }
}
