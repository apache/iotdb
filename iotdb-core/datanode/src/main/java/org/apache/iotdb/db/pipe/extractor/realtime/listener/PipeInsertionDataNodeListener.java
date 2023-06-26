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

package org.apache.iotdb.db.pipe.extractor.realtime.listener;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PipeInsertionEventListener is a singleton in each data node.
 *
 * <p>It is used to listen to events from storage storageengine and publish them to pipe storageengine.
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
        .computeIfAbsent(dataRegionId, o -> new PipeDataRegionAssigner())
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
      // the removed assigner will is the same as the one referenced by the variable `assigner`
      dataRegionId2Assigner.remove(dataRegionId);
      // this will help to release the memory occupied by the assigner
      assigner.gc();
    }
  }

  //////////////////////////// listen to events ////////////////////////////

  public void listenToTsFile(String dataRegionId, TsFileResource tsFileResource) {
    // wo don't judge whether listenToTsFileExtractorCount.get() == 0 here, because
    // when using SimpleProgressIndex, the tsfile event needs to be assigned to the
    // extractor even if listenToTsFileExtractorCount.get() == 0 to record the progress

    PipeAgent.runtime().assignSimpleProgressIndexIfNeeded(tsFileResource);

    final PipeDataRegionAssigner assigner = dataRegionId2Assigner.get(dataRegionId);

    // only events from registered data region will be extracted
    if (assigner == null) {
      return;
    }

    assigner.publishToAssign(PipeRealtimeEventFactory.createRealtimeEvent(tsFileResource));
  }

  public void listenToInsertNode(
      String dataRegionId,
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
        PipeRealtimeEventFactory.createRealtimeEvent(walEntryHandler, insertNode, tsFileResource));
  }

  /////////////////////////////// singleton ///////////////////////////////

  private PipeInsertionDataNodeListener() {}

  public static PipeInsertionDataNodeListener getInstance() {
    return PipeChangeDataCaptureListenerHolder.INSTANCE;
  }

  private static class PipeChangeDataCaptureListenerHolder {
    private static final PipeInsertionDataNodeListener INSTANCE =
        new PipeInsertionDataNodeListener();
  }
}
