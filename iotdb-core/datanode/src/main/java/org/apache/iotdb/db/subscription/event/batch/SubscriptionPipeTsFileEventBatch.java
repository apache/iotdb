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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch.TabletTransformResult;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.columnfilter.TabletColumnPruner;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTsFileBatchEvents;
import org.apache.iotdb.db.subscription.tagfilter.TabletTagFilter;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterEvaluationException;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionPipeTsFileEventBatch extends SubscriptionPipeEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTsFileEventBatch.class);

  private final PipeTabletEventTsFileBatch batch;
  private final List<Pair<String, File>> sealedFilePairs = new ArrayList<>();
  private volatile SubscriptionFilterSnapshot filterSnapshot;
  private volatile SubscriptionTreeViewProjector treeViewProjector;

  public SubscriptionPipeTsFileEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTsFileQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
    this.batch =
        new PipeTabletEventTsFileBatch(maxDelayInMs, maxBatchSizeInBytes, this::transformTablet);
  }

  @TestOnly
  SubscriptionPipeTsFileEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTsFileQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes,
      final PipeTabletEventTsFileBatch batch) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);
    this.batch = batch;
  }

  @Override
  public synchronized void ack() {
    batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
  }

  @Override
  public synchronized void cleanUp(final boolean force) {
    try {
      // close batch, it includes clearing the reference count of events
      batch.close();
    } finally {
      for (final Pair<String, File> sealedFilePair : sealedFilePairs) {
        final File sealedFile = sealedFilePair.right;
        if (!org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(sealedFile)) {
          LOGGER.warn(DataNodePipeMessages.FAILED_TO_DELETE_BATCH_FILE_THIS_FILE, sealedFilePair);
        }
      }
      sealedFilePairs.clear();
      enrichedEvents.clear();
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  @Override
  protected void onTabletInsertionEvent(final TabletInsertionEvent event) throws Exception {
    ensureFilterSnapshot();
    // Keep the queue's reference when transformation fails: the prefetching queue retries this
    // same event without acquiring another reference. Release it only after successful batching.
    batch.onEvent(event);
    ((EnrichedEvent) event)
        .decreaseReferenceCount(
            SubscriptionPipeTsFileEventBatch.class.getName(),
            false); // missing releaseLastEvent decreases reference count
  }

  @Override
  protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {
    LOGGER.warn(
        DataNodePipeMessages
            .PIPE_LOG_SUBSCRIPTIONPIPETSFILEEVENTBATCH_IGNORE_TSFILEINSERTIONEVENT_88189024,
        this,
        event);
  }

  @Override
  protected List<SubscriptionEvent> generateSubscriptionEvents() throws Exception {
    if (batch.isEmpty()) {
      return discardEmptyBatch();
    }

    final List<SubscriptionEvent> events = new ArrayList<>();
    final List<Pair<String, File>> dbTsFilePairs = batch.sealTsFiles();
    if (dbTsFilePairs.isEmpty()) {
      batch.decreaseEventsReferenceCount(this.getClass().getName(), true);
      batch.onSuccess();
      return discardEmptyBatch();
    }
    sealedFilePairs.addAll(dbTsFilePairs);
    final AtomicInteger ackReferenceCount = new AtomicInteger(dbTsFilePairs.size());
    final AtomicInteger cleanReferenceCount = new AtomicInteger(dbTsFilePairs.size());
    for (final Pair<String, File> pair : dbTsFilePairs) {
      final SubscriptionCommitContext commitContext =
          prefetchingQueue.generateSubscriptionCommitContext();
      events.add(
          new SubscriptionEvent(
              new SubscriptionPipeTsFileBatchEvents(this, ackReferenceCount, cleanReferenceCount),
              pair.right,
              pair.left,
              commitContext));
    }
    return events;
  }

  /** Closes the inner conversion batch when filtering produced no TsFile payload. */
  private List<SubscriptionEvent> discardEmptyBatch() {
    try {
      batch.close();
    } finally {
      enrichedEvents.clear();
    }
    return Collections.emptyList();
  }

  @Override
  protected boolean shouldEmit() {
    return (!enrichedEvents.isEmpty() && batch.isEmpty()) || batch.shouldEmit();
  }

  private TabletTransformResult transformTablet(
      final String databaseName,
      final Tablet tablet,
      final boolean isTableModel,
      final boolean isAligned) {
    final SubscriptionFilterSnapshot snapshot = ensureFilterSnapshot();
    if (isTableModel) {
      return new TabletTransformResult(
          pruneTableModelTablet(databaseName, tablet, snapshot), databaseName, true, isAligned);
    }
    if (!snapshot.getTopicConfig().isTableTopic()) {
      return new TabletTransformResult(tablet, databaseName, false, isAligned);
    }

    if (!prepareTreeViewProjector(snapshot)) {
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages.EXCEPTION_TABLE_SCHEMA_IS_NOT_AVAILABLE_FOR_TAG_FILTER_993AB728);
    }
    if (!treeViewProjector.isAvailable()) {
      if (snapshot.hasNonTrivialFilter()) {
        throw new TagFilterEvaluationException(
            DataNodeMiscMessages
                .EXCEPTION_TREE_VIEW_PROJECTOR_IS_UNAVAILABLE_FOR_FILTERED_SUBSCRIPTION_DATA_B5F396A5);
      }
      return new TabletTransformResult(tablet, databaseName, false, isAligned);
    }

    final Tablet projectedTablet = treeViewProjector.project(tablet);
    return Objects.isNull(projectedTablet)
        ? null
        : new TabletTransformResult(
            pruneTableModelTablet(treeViewProjector.getDatabaseName(), projectedTablet, snapshot),
            treeViewProjector.getDatabaseName(),
            true,
            false);
  }

  private Tablet pruneTableModelTablet(
      final String databaseName, final Tablet tablet, final SubscriptionFilterSnapshot snapshot) {
    return TabletColumnPruner.pruneTableModelTablet(
        TabletTagFilter.filter(tablet, snapshot.getTagFilterMatcher(), databaseName),
        databaseName,
        snapshot.getColumnFilterMatcher());
  }

  private boolean prepareTreeViewProjector(final SubscriptionFilterSnapshot snapshot) {
    if (Objects.isNull(treeViewProjector)) {
      treeViewProjector = new SubscriptionTreeViewProjector(snapshot.getTopicConfig());
    }
    final boolean prepared = treeViewProjector.prepare();
    if (!prepared) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D,
          prefetchingQueue.getTopicName(),
          snapshot
              .getTopicConfig()
              .getStringOrDefault(TopicConstant.DATABASE_KEY, TopicConstant.DATABASE_DEFAULT_VALUE),
          snapshot
              .getTopicConfig()
              .getStringOrDefault(TopicConstant.TABLE_KEY, TopicConstant.TABLE_DEFAULT_VALUE));
    }
    return prepared;
  }

  @Override
  protected boolean isCompatibleWithCurrentTopicConfig() {
    return Objects.isNull(filterSnapshot) || filterSnapshot.isCurrent(prefetchingQueue);
  }

  private synchronized SubscriptionFilterSnapshot ensureFilterSnapshot() {
    if (Objects.isNull(filterSnapshot)) {
      filterSnapshot = SubscriptionFilterSnapshot.capture(prefetchingQueue);
    }
    return filterSnapshot;
  }
}
