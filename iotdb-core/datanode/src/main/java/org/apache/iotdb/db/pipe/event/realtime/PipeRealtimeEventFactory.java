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
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch.TsFileEpochManager;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class PipeRealtimeEventFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRealtimeEventFactory.class);
  private static final TsFileEpochManager TS_FILE_EPOCH_MANAGER = new TsFileEpochManager();

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId,
      final Boolean isTableModel,
      final String databaseNameFromDataRegion,
      final TsFileResource resource,
      final boolean isLoaded) {
    PipeTsFileInsertionEvent tsFileInsertionEvent =
        new PipeTsFileInsertionEvent(
            isTableModel, databaseNameFromDataRegion, resource, isLoaded, false);

    // if using IoTV2, assign a replicateIndex for this event
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus
        && PipeConsensusProcessor.isShouldReplicate(tsFileInsertionEvent)) {
      tsFileInsertionEvent.setReplicateIndexForIoTV2(
          ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(dataRegionId));
      LOGGER.info(
          "[Region{}]Set {} for event {}",
          dataRegionId,
          tsFileInsertionEvent.getReplicateIndexForIoTV2(),
          tsFileInsertionEvent);
    }

    return TS_FILE_EPOCH_MANAGER.bindPipeTsFileInsertionEvent(tsFileInsertionEvent, resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId,
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

    // if using IoTV2, assign a replicateIndex for this event
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus
        && PipeConsensusProcessor.isShouldReplicate(insertionEvent)) {
      insertionEvent.setReplicateIndexForIoTV2(
          ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(dataRegionId));
      LOGGER.info(
          "[Region{}]Set {} for event {}",
          dataRegionId,
          insertionEvent.getReplicateIndexForIoTV2(),
          insertionEvent);
    }

    return TS_FILE_EPOCH_MANAGER.bindPipeInsertNodeTabletInsertionEvent(
        insertionEvent, insertNode, resource);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId, final boolean shouldPrintMessage) {
    return new PipeRealtimeEvent(
        new PipeHeartbeatEvent(dataRegionId, shouldPrintMessage), null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(
      final String dataRegionId, final AbstractDeleteDataNode node) {
    PipeDeleteDataNodeEvent deleteDataNodeEvent =
        new PipeDeleteDataNodeEvent(node, node.isGeneratedByPipe());

    // if using IoTV2, assign a replicateIndex for this event
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus
        && PipeConsensusProcessor.isShouldReplicate(deleteDataNodeEvent)) {
      deleteDataNodeEvent.setReplicateIndexForIoTV2(
          ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(dataRegionId));
      LOGGER.info(
          "[Region{}]Set {} for event {}",
          dataRegionId,
          deleteDataNodeEvent.getReplicateIndexForIoTV2(),
          deleteDataNodeEvent);
    }

    return new PipeRealtimeEvent(deleteDataNodeEvent, null, null);
  }

  public static PipeRealtimeEvent createRealtimeEvent(final ProgressReportEvent event) {
    return new PipeRealtimeEvent(event, null, null);
  }

  private PipeRealtimeEventFactory() {
    // factory class, do not instantiate
  }
}
