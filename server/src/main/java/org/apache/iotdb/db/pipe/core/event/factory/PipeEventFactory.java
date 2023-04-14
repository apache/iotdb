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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.event.PipeCollectEvent;
import org.apache.iotdb.db.pipe.core.event.PipeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.io.File;

public class PipeEventFactory {
  private static final PipeCollectorEventFactory COLLECTOR_EVENT_FACTORY =
      new PipeCollectorEventFactory();

  public static PipeTabletInsertionEvent createTabletInsertEvent(InsertNode planNode) {
    return new PipeTabletInsertionEvent(planNode);
  }

  public static PipeTsFileInsertionEvent createTsFileInsertionEvent(File tsFile) {
    return new PipeTsFileInsertionEvent(tsFile);
  }

  public static PipeCollectEvent createCollectorEvent(
      Event event, long timePartitionId, boolean isSeq, InsertNode node) {
    return COLLECTOR_EVENT_FACTORY.createCollectorEvent(event, timePartitionId, isSeq, node);
  }

  public static PipeCollectEvent createCollectorEvent(
      Event event, long timePartitionId, boolean isSeq, TsFileResource resource) {
    return COLLECTOR_EVENT_FACTORY.createCollectorEvent(event, timePartitionId, isSeq, resource);
  }
}
