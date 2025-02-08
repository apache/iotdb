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

package org.apache.iotdb.db.pipe.event.realtime;

import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch.TsFileEpochManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

public class PipeRealtimeEventFactory {

  private static final TsFileEpochManager TS_FILE_EPOCH_MANAGER = new TsFileEpochManager();

  public static PipeRealtimeEvent createRealtimeEvent(
      final String databaseName,
      final TsFileResource resource,
      final boolean isLoaded,
      final boolean isGeneratedByPipe) {
    return TS_FILE_EPOCH_MANAGER.bindPipeTsFileInsertionEvent(
        new PipeTsFileInsertionEvent(databaseName, resource, isLoaded, isGeneratedByPipe, false),
        resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String databaseName,
      final WALEntryHandler walEntryHandler,
      final InsertNode insertNode,
      final TsFileResource resource) {
    return TS_FILE_EPOCH_MANAGER.bindPipeInsertNodeTabletInsertionEvent(
        new PipeInsertNodeTabletInsertionEvent(
            databaseName,
            walEntryHandler,
            insertNode.getTargetPath(),
            insertNode.getProgressIndex(),
            insertNode.isAligned(),
            insertNode.isGeneratedByPipe()),
        insertNode,
        resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId, final boolean shouldPrintMessage) {
    return new PipeRealtimeEvent(
        new PipeHeartbeatEvent(dataRegionId, shouldPrintMessage), null, null, null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(final AbstractDeleteDataNode node) {
    return new PipeRealtimeEvent(
        new PipeDeleteDataNodeEvent(node, node.isGeneratedByPipe()), null, null, null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(final ProgressReportEvent event) {
    return new PipeRealtimeEvent(event, null, null, null, null);
  }

  private PipeRealtimeEventFactory() {
    // factory class, do not instantiate
  }
}
