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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PipeInsertionEventListener is a singleton in each data node.
 *
 * <p>It is used to listen to events from storage engine and publish them to pipe engine.
 *
 * <p>2 kinds of events are extracted: 1. level-0 tsfile sealed event 2. insertion operation event
 *
 * <p>All events extracted by this listener will be first published to different
 * PipeEventDataRegionAssigners (identified by data region id), and then PipeEventDataRegionAssigner
 * will filter events and assign them to different PipeRealtimeEventDataRegionExtractors.
 */
public class PipeInsertionDataNodeListener {
  private final ConcurrentMap<String, PipeDataRegionAssigner> dataRegionId2Assigner =
      new ConcurrentHashMap<>();

  private final AtomicInteger listenToTsFileExtractorCount = new AtomicInteger(0);
  private final AtomicInteger listenToInsertNodeExtractorCount = new AtomicInteger(0);

  //////////////////////////// start & stop ////////////////////////////

  public synchronized void startListenAndAssign(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    dataRegionId2Assigner
        .computeIfAbsent(dataRegionId, o -> new PipeDataRegionAssigner(dataRegionId))
        .startAssignTo(extractor);

    if (extractor.isNeedListenToTsFile()) {
      listenToTsFileExtractorCount.incrementAndGet();
    }
    if (extractor.isNeedListenToInsertNode()) {
      listenToInsertNodeExtractorCount.incrementAndGet();
    }
  }

  public synchronized void stopListenAndAssign(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    final PipeDataRegionAssigner assigner = dataRegionId2Assigner.get(dataRegionId);
    if (assigner == null) {
      return;
    }

    assigner.stopAssignTo(extractor);

    if (extractor.isNeedListenToTsFile()) {
      listenToTsFileExtractorCount.decrementAndGet();
    }
    if (extractor.isNeedListenToInsertNode()) {
      listenToInsertNodeExtractorCount.decrementAndGet();
    }

    if (assigner.notMoreExtractorNeededToBeAssigned()) {
      // The removed assigner will is the same as the one referenced by the variable `assigner`
      dataRegionId2Assigner.remove(dataRegionId);
      // This will help to release the memory occupied by the assigner
      assigner.close();
    }
  }

  //////////////////////////// listen to events ////////////////////////////

  public void listenToTsFile(
      String dataRegionId,
      String databaseName,
      TsFileResource tsFileResource,
      boolean isLoaded,
      boolean isGeneratedByPipe) {
    // We don't judge whether listenToTsFileExtractorCount.get() == 0 here on purpose
    // because extractors may use tsfile events when some exceptions occur in the
    // insert nodes listening process.

    final PipeDataRegionAssigner assigner = dataRegionId2Assigner.get(dataRegionId);

    // only events from registered data region will be extracted
    if (assigner == null) {
      return;
    }

    assigner.publishToAssign(
        PipeRealtimeEventFactory.createRealtimeEvent(
            databaseName, tsFileResource, isLoaded, isGeneratedByPipe));
  }

  public void listenToInsertNode(
      String dataRegionId,
      String databaseName,
      WALEntryHandler walEntryHandler,
      InsertNode insertNode,
      TsFileResource tsFileResource) {
    if (listenToInsertNodeExtractorCount.get() == 0) {
      return;
    }

    final PipeDataRegionAssigner assigner = dataRegionId2Assigner.get(dataRegionId);

    // only events from registered data region will be extracted
    if (assigner == null) {
      return;
    }

    assigner.publishToAssign(
        PipeRealtimeEventFactory.createRealtimeEvent(
            databaseName, walEntryHandler, insertNode, tsFileResource));
  }

  // TODO: record database name in enriched events?
  public DeletionResource listenToDeleteData(
      final String regionId, final AbstractDeleteDataNode node) {
    final PipeDataRegionAssigner assigner = dataRegionId2Assigner.get(regionId);
    // only events from registered data region will be extracted
    if (assigner == null) {
      return null;
    }

    final DeletionResource deletionResource;
    // register a deletionResource and return it to DataRegion
    final DeletionResourceManager manager = DeletionResourceManager.getInstance(regionId);
    // deleteNode generated by remote consensus leader shouldn't be persisted to DAL.
    if (Objects.nonNull(manager) && DeletionResource.isDeleteNodeGeneratedInLocalByIoTV2(node)) {
      deletionResource = manager.registerDeletionResource(node);
      // if persist failed, skip sending/publishing this event to keep consistency with the
      // behavior of storage engine.
      if (deletionResource.waitForResult() == DeletionResource.Status.FAILURE) {
        return deletionResource;
      }
    } else {
      deletionResource = null;
    }

    assigner.publishToAssign(PipeRealtimeEventFactory.createRealtimeEvent(node));

    return deletionResource;
  }

  public void listenToHeartbeat(boolean shouldPrintMessage) {
    dataRegionId2Assigner.forEach(
        (key, value) ->
            value.publishToAssign(
                PipeRealtimeEventFactory.createRealtimeEvent(key, shouldPrintMessage)));
  }

  /////////////////////////////// singleton ///////////////////////////////

  private PipeInsertionDataNodeListener() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            "PipeInsertionDataNodeListener#listenToHeartbeat(false)",
            () -> listenToHeartbeat(false),
            PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
  }

  public static PipeInsertionDataNodeListener getInstance() {
    return PipeChangeDataCaptureListenerHolder.INSTANCE;
  }

  private static class PipeChangeDataCaptureListenerHolder {
    private static final PipeInsertionDataNodeListener INSTANCE =
        new PipeInsertionDataNodeListener();
  }
}
