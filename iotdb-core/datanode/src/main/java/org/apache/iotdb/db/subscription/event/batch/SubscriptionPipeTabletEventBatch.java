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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.columnfilter.TabletColumnPruner;
import org.apache.iotdb.db.subscription.columnfilter.TreeViewTabletProjector;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.tagfilter.TabletTagFilter;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterEvaluationException;
import org.apache.iotdb.metrics.core.utils.IoTDBMovingAverage;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionPipeTabletEventBatch extends SubscriptionPipeEventBatch
    implements Iterator<SubscriptionPipeTabletEventBatch.TabletBatch> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private long firstEventProcessingTime = Long.MIN_VALUE;
  private long totalBufferSize = 0;

  private volatile Iterator<EnrichedEvent> currentEnrichedEventsIterator;
  private volatile Iterator<TabletInsertionEvent> currentTabletInsertionEventsIterator;
  private volatile TsFileInsertionEvent currentTsFileInsertionEvent;

  private final Meter insertNodeTabletInsertionEventSizeEstimator;
  private final Meter rawTabletInsertionEventSizeEstimator;
  private final TableViewTabletProjectorProvider tableViewTabletProjectorProvider;
  private volatile SubscriptionFilterSnapshot filterSnapshot;

  private volatile SubscriptionPipeTabletIterationSnapshot iterationSnapshot;
  private final AtomicInteger referenceCount = new AtomicInteger();

  private static final long ITERATED_COUNT_REPORT_FREQ =
      30000; // based on the full parse of a 128MB tsfile estimate
  private final AtomicLong iteratedCount = new AtomicLong();

  public SubscriptionPipeTabletEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this(
        regionId,
        prefetchingQueue,
        new TableViewTabletProjectorProvider(prefetchingQueue),
        maxDelayInMs,
        maxBatchSizeInBytes);
  }

  public SubscriptionPipeTabletEventBatch(
      final int regionId,
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final TableViewTabletProjectorProvider tableViewTabletProjectorProvider,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    super(regionId, prefetchingQueue, maxDelayInMs, maxBatchSizeInBytes);

    this.insertNodeTabletInsertionEventSizeEstimator =
        new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
    this.rawTabletInsertionEventSizeEstimator =
        new Meter(new IoTDBMovingAverage(), Clock.defaultClock());

    this.tableViewTabletProjectorProvider = tableViewTabletProjectorProvider;

    resetForIteration();
  }

  /////////////////////////////// ack & clean ///////////////////////////////

  @Override
  public synchronized void ack() {
    referenceCount.decrementAndGet();

    // we decrease the reference count of events if and only if when the whole batch is consumed
    if (!hasNext() && referenceCount.get() == 0) {
      for (final EnrichedEvent enrichedEvent : enrichedEvents) {
        enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
      }
    }
  }

  @Override
  public synchronized void cleanUp(final boolean force) {
    // do nothing if it has next or still referenced by unacked response
    if (!force && (hasNext() || referenceCount.get() != 0)) {
      return;
    }

    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
    enrichedEvents.clear();

    resetForIteration();
  }

  /////////////////////////////// utility ///////////////////////////////

  @Override
  protected void onTabletInsertionEvent(final TabletInsertionEvent event) {
    ensureFilterSnapshot();

    // update processing time
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }

    // update buffer size
    // TODO: more precise computation
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      totalBufferSize += getEstimatedInsertNodeTabletInsertionEventSize();
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      totalBufferSize += getEstimatedRawTabletInsertionEventSize();
    }
  }

  @Override
  protected void onTsFileInsertionEvent(final TsFileInsertionEvent event) {
    ensureFilterSnapshot();

    // update processing time
    if (firstEventProcessingTime == Long.MIN_VALUE) {
      firstEventProcessingTime = System.currentTimeMillis();
    }

    // update buffer size
    // TODO: more precise computation
    // NOTE: Considering the possibility of large tsfile, the final generated response size may be
    // larger than totalBufferSize, therefore limit control is also required in
    // SubscriptionEventTabletResponse.
    totalBufferSize += ((PipeTsFileInsertionEvent) event).getTsFile().length();
  }

  @Override
  protected List<SubscriptionEvent> generateSubscriptionEvents() {
    if (!tableViewTabletProjectorProvider.prepareForEmission(
        ensureFilterSnapshot().getTopicConfig())) {
      return null;
    }
    resetForIteration();
    return Collections.singletonList(new SubscriptionEvent(this, prefetchingQueue));
  }

  @Override
  protected boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs
        // considering the inaccuracy of the estimation, configure a hard limit here to avoid an
        // excessively large batch
        || enrichedEvents.size()
            >= SubscriptionConfig.getInstance().getSubscriptionMaxAllowedEventCountInTabletBatch();
  }

  private TabletBatch convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    final TabletBatch result;
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final List<Tablet> tablets =
          ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
      final long estimatedSizeInBytes = calculateTabletsSizeInBytes(tablets);
      updateEstimatedInsertNodeTabletInsertionEventSize(estimatedSizeInBytes);
      result =
          new TabletBatch(
              ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).isTableModelEvent()
                  ? ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent)
                      .getTableModelDatabaseName()
                  : null,
              tablets,
              estimatedSizeInBytes);
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      final Tablet tablet = ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet();
      final long estimatedSizeInBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
      updateEstimatedRawTabletInsertionEventSize(estimatedSizeInBytes);
      result =
          new TabletBatch(
              ((PipeRawTabletInsertionEvent) tabletInsertionEvent).isTableModelEvent()
                  ? ((PipeRawTabletInsertionEvent) tabletInsertionEvent).getTableModelDatabaseName()
                  : null,
              Collections.singletonList(tablet),
              estimatedSizeInBytes);
    } else {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_ONLY_SUPPORT_CONVERT_PIPEINSERTNODETABLETINSERTIONEVENT_B888B8AA,
          this,
          tabletInsertionEvent);
      return null;
    }

    final TabletBatch projected = projectTreeViewTabletsIfNecessary(result);
    if (Objects.isNull(projected)) {
      return null;
    }
    final TabletBatch pruned = pruneTablets(projected);
    if (Objects.isNull(pruned) || pruned.estimatedSizeMatchesTablets) {
      return pruned;
    }
    return pruned.withEstimatedSize(calculateTabletsSizeInBytes(pruned.tablets));
  }

  private TabletBatch projectTreeViewTabletsIfNecessary(final TabletBatch tablets) {
    if (Objects.isNull(tablets)
        || Objects.nonNull(tablets.databaseName)
        || Objects.isNull(tablets.tablets)) {
      return tablets;
    }

    final TreeViewTabletProjector treeViewTabletProjector =
        tableViewTabletProjectorProvider.prepareForEmission(ensureFilterSnapshot().getTopicConfig())
            ? tableViewTabletProjectorProvider.getTreeViewTabletProjector()
            : null;
    if (Objects.isNull(treeViewTabletProjector)) {
      if (!ensureFilterSnapshot().hasNonTrivialFilter()) {
        return tablets;
      }
      throw new TagFilterEvaluationException(
          DataNodeMiscMessages
              .EXCEPTION_TREE_VIEW_PROJECTOR_IS_UNAVAILABLE_FOR_FILTERED_SUBSCRIPTION_DATA_B5F396A5);
    }

    final List<Tablet> projectedTablets = new ArrayList<>(tablets.tablets.size());
    for (final Tablet tablet : tablets.tablets) {
      final Tablet projectedTablet = treeViewTabletProjector.project(tablet);
      if (Objects.nonNull(projectedTablet)) {
        projectedTablets.add(projectedTablet);
      }
    }
    return projectedTablets.isEmpty()
        ? null
        : tablets.withTablets(treeViewTabletProjector.getDatabaseName(), projectedTablets);
  }

  private TabletBatch pruneTablets(final TabletBatch tablets) {
    if (Objects.isNull(tablets)
        || Objects.isNull(tablets.databaseName)
        || Objects.isNull(tablets.tablets)) {
      return tablets;
    }

    final SubscriptionFilterSnapshot snapshot = ensureFilterSnapshot();

    final List<Tablet> prunedTablets = new ArrayList<>(tablets.tablets.size());
    for (final Tablet tablet : tablets.tablets) {
      final Tablet prunedTablet =
          TabletColumnPruner.pruneTableModelTablet(
              TabletTagFilter.filter(tablet, snapshot.getTagFilterMatcher(), tablets.databaseName),
              tablets.databaseName,
              snapshot.getColumnFilterMatcher());
      if (Objects.nonNull(prunedTablet)) {
        prunedTablets.add(prunedTablet);
      }
    }
    return prunedTablets.isEmpty()
        ? null
        : tablets.withTablets(tablets.databaseName, prunedTablets);
  }

  private long calculateTabletsSizeInBytes(final List<Tablet> tablets) {
    long estimatedSizeInBytes = 0L;
    for (final Tablet tablet : tablets) {
      estimatedSizeInBytes += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
    }
    return estimatedSizeInBytes;
  }

  private static boolean haveSameTabletReferences(
      final List<Tablet> left, final List<Tablet> right) {
    if (left == right) {
      return true;
    }
    if (Objects.isNull(left) || Objects.isNull(right) || left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (left.get(i) != right.get(i)) {
        return false;
      }
    }
    return true;
  }

  public static final class TabletBatch {
    private final String databaseName;
    private final List<Tablet> tablets;
    private final long estimatedSizeInBytes;
    private final boolean estimatedSizeMatchesTablets;

    private TabletBatch(
        final String databaseName, final List<Tablet> tablets, final long estimatedSizeInBytes) {
      this(databaseName, tablets, estimatedSizeInBytes, true);
    }

    private TabletBatch(
        final String databaseName,
        final List<Tablet> tablets,
        final long estimatedSizeInBytes,
        final boolean estimatedSizeMatchesTablets) {
      this.databaseName = databaseName;
      this.tablets = tablets;
      this.estimatedSizeInBytes = estimatedSizeInBytes;
      this.estimatedSizeMatchesTablets = estimatedSizeMatchesTablets;
    }

    private TabletBatch withTablets(final String databaseName, final List<Tablet> tablets) {
      return new TabletBatch(
          databaseName,
          tablets,
          estimatedSizeInBytes,
          estimatedSizeMatchesTablets && haveSameTabletReferences(this.tablets, tablets));
    }

    private TabletBatch withEstimatedSize(final long estimatedSizeInBytes) {
      return new TabletBatch(databaseName, tablets, estimatedSizeInBytes, true);
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public List<Tablet> getTablets() {
      return tablets;
    }

    public long getEstimatedSizeInBytes() {
      return estimatedSizeInBytes;
    }
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

  /////////////////////////////// estimator ///////////////////////////////

  private long getEstimatedInsertNodeTabletInsertionEventSize() {
    return Math.max(
        SubscriptionConfig.getInstance()
            .getSubscriptionEstimatedInsertNodeTabletInsertionEventSize(),
        (long) insertNodeTabletInsertionEventSizeEstimator.getOneMinuteRate());
  }

  private void updateEstimatedInsertNodeTabletInsertionEventSize(final long size) {
    insertNodeTabletInsertionEventSizeEstimator.mark(size);
  }

  private long getEstimatedRawTabletInsertionEventSize() {
    return Math.max(
        SubscriptionConfig.getInstance().getSubscriptionEstimatedRawTabletInsertionEventSize(),
        (long) rawTabletInsertionEventSizeEstimator.getOneMinuteRate());
  }

  private void updateEstimatedRawTabletInsertionEventSize(final long size) {
    rawTabletInsertionEventSizeEstimator.mark(size);
  }

  /////////////////////////////// iterator ///////////////////////////////

  public synchronized SubscriptionPipeTabletIterationSnapshot sendIterationSnapshot() {
    final SubscriptionPipeTabletIterationSnapshot result = iterationSnapshot;
    iterationSnapshot = new SubscriptionPipeTabletIterationSnapshot();
    referenceCount.incrementAndGet();
    return result;
  }

  public ColumnFilterMatcher getColumnFilterMatcher() {
    return ensureFilterSnapshot().getColumnFilterMatcher();
  }

  public synchronized void resetForIteration() {
    currentEnrichedEventsIterator = enrichedEvents.iterator();
    currentTabletInsertionEventsIterator = null;
    if (Objects.nonNull(currentTsFileInsertionEvent)
        && currentTsFileInsertionEvent instanceof PipeTsFileInsertionEvent) {
      ((PipeTsFileInsertionEvent) currentTsFileInsertionEvent).close();
    }
    currentTsFileInsertionEvent = null;

    if (Objects.nonNull(iterationSnapshot)) {
      iterationSnapshot.cleanUp();
    }
    iterationSnapshot = new SubscriptionPipeTabletIterationSnapshot();
    referenceCount.set(0);

    iteratedCount.set(0);
  }

  @Override
  public synchronized boolean hasNext() {
    if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
      if (currentTabletInsertionEventsIterator.hasNext()) {
        return true;
      } else {
        finishCurrentTsFileInsertionEvent();
        return hasNext();
      }
    }

    if (Objects.isNull(currentEnrichedEventsIterator)) {
      return false;
    }

    if (currentEnrichedEventsIterator.hasNext()) {
      return true;
    } else {
      // reset
      currentEnrichedEventsIterator = null;
      return false;
    }
  }

  @Override
  public synchronized TabletBatch next() {
    final TabletBatch tablets = nextInternal();
    if (Objects.isNull(tablets)) {
      return null;
    }
    if (iteratedCount.incrementAndGet() % ITERATED_COUNT_REPORT_FREQ == 0) {
      LOGGER.info(
          DataNodePipeMessages
              .PIPE_LOG_HAS_BEEN_ITERATED_TIMES_CURRENT_TSFILEINSERTIONEVENT_0939C298,
          this,
          iteratedCount,
          Objects.isNull(currentTsFileInsertionEvent)
              ? "<unknown>"
              : ((EnrichedEvent) currentTsFileInsertionEvent).coreReportMessage());
    }
    return tablets;
  }

  private TabletBatch nextInternal() {
    if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
      if (currentTabletInsertionEventsIterator.hasNext()) {
        final TabletInsertionEvent tabletInsertionEvent =
            currentTabletInsertionEventsIterator.next();
        if (!(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_UNEXPECTED_TABLET_INSERTION_8FB1B507,
              tabletInsertionEvent);
        } else {
          if (!((PipeRawTabletInsertionEvent) tabletInsertionEvent)
              .increaseReferenceCount(this.getClass().getName())) {
            LOGGER.warn(
                DataNodePipeMessages
                    .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_FAILED_TO_INCREASE_THE_595722D8,
                ((PipeRawTabletInsertionEvent) tabletInsertionEvent).coreReportMessage());
          } else {
            iterationSnapshot.addParsedEnrichedEvent(
                (PipeRawTabletInsertionEvent) tabletInsertionEvent);
          }
        }
        if (!currentTabletInsertionEventsIterator.hasNext()) {
          finishCurrentTsFileInsertionEvent();
        }
        return convertToTablets(tabletInsertionEvent);
      } else {
        finishCurrentTsFileInsertionEvent();
      }
    }

    if (Objects.isNull(currentEnrichedEventsIterator)) {
      return null;
    }

    if (!currentEnrichedEventsIterator.hasNext()) {
      return null;
    }

    final EnrichedEvent enrichedEvent = currentEnrichedEventsIterator.next();
    if (enrichedEvent instanceof TsFileInsertionEvent) {
      if (Objects.nonNull(currentTabletInsertionEventsIterator)) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_OVERRIDE_NON_NULL_CURRENTTABLETINSERTIONEVENTSITERATOR_2633B158,
            this);
      }
      final PipeTsFileInsertionEvent tsFileInsertionEvent =
          (PipeTsFileInsertionEvent) enrichedEvent;
      currentTsFileInsertionEvent = tsFileInsertionEvent;
      currentTabletInsertionEventsIterator =
          tsFileInsertionEvent
              .toTabletInsertionEvents(
                  // disrupt parsing requests through the introduction of randomness
                  (long) ((1 + Math.random()) * SubscriptionAgent.receiver().remainingMs()))
              .iterator();
      return next();
    } else if (enrichedEvent instanceof TabletInsertionEvent) {
      // There are two types of TabletInsertionEvent:
      //   1. PipeInsertNodeTabletInsertionEvent, which binds memory blocks when parsed into tablets
      //   2. PipeRawTabletInsertionEvent, which is parsed and bound with memory blocks upstream
      iterationSnapshot.addIteratedEnrichedEvent(enrichedEvent);
      return convertToTablets((TabletInsertionEvent) enrichedEvent);
    } else {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_E6BAEACE,
          this,
          enrichedEvent);
      return null;
    }
  }

  private void finishCurrentTsFileInsertionEvent() {
    final PipeTsFileInsertionEvent event = (PipeTsFileInsertionEvent) currentTsFileInsertionEvent;
    iterationSnapshot.addIteratedEnrichedEvent(event);

    // Release the parser reservation before opening the next file in the same region. Waiting for
    // the whole batch to be ACKed can block that file behind this exhausted parser. The parsed
    // tablet events remain referenced by the iteration snapshot until ACK or cleanup.
    event.close();
    currentTabletInsertionEventsIterator = null;
    currentTsFileInsertionEvent = null;
  }
}
