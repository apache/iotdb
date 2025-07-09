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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import java.util.stream.Collectors;

public class PipeRealtimeEventFactory {
  private static final TsFileEpochManager TS_FILE_EPOCH_MANAGER = new TsFileEpochManager();

  public static PipeRealtimeEvent createRealtimeEvent(
      final Boolean isTableModel,
      final String databaseNameFromDataRegion,
      final TsFileResource resource,
      final boolean isLoaded) {
    PipeTsFileInsertionEvent tsFileInsertionEvent =
        new PipeTsFileInsertionEvent(
            isTableModel, databaseNameFromDataRegion, resource, isLoaded, false);

    return TS_FILE_EPOCH_MANAGER.bindPipeTsFileInsertionEvent(tsFileInsertionEvent, resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final Boolean isTableModel,
      final String databaseNameFromDataRegion,
      final WALEntryHandler walEntryHandler,
      final InsertNode insertNode,
      final TsFileResource resource) {
    final PipeInsertNodeTabletInsertionEvent insertionEvent =
        new PipeInsertNodeTabletInsertionEvent(
            isTableModel,
            databaseNameFromDataRegion,
            walEntryHandler,
            insertNode.getTargetPath(),
            (insertNode instanceof InsertRowsNode)
                ? ((InsertRowsNode) insertNode)
                    .getInsertRowNodeList().stream()
                        .map(
                            node ->
                                DeviceIDFactory.getInstance()
                                    .getDeviceID(node.getTargetPath())
                                    .getTableName())
                        .collect(Collectors.toSet())
                : null,
            insertNode.getProgressIndex(),
            insertNode.isAligned(),
            insertNode.isGeneratedByPipe());

    return TS_FILE_EPOCH_MANAGER.bindPipeInsertNodeTabletInsertionEvent(
        insertionEvent, insertNode, resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId, final boolean shouldPrintMessage) {
    return new PipeRealtimeEvent(
        new PipeHeartbeatEvent(dataRegionId, shouldPrintMessage), null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(final AbstractDeleteDataNode node) {
    PipeDeleteDataNodeEvent deleteDataNodeEvent =
        new PipeDeleteDataNodeEvent(node, node.isGeneratedByPipe());

    return new PipeRealtimeEvent(deleteDataNodeEvent, null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(final ProgressReportEvent event) {
    return new PipeRealtimeEvent(event, null, null);
  }

  private PipeRealtimeEventFactory() {
    // factory class, do not instantiate
  }
}
