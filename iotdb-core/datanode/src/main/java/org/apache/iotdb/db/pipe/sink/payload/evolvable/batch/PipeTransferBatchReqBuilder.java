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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeCacheLeaderClientManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.metrics.impl.DoNothingHistogram;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_TS_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_TS_FILE_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_TS_FILE_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LEADER_CACHE_ENABLE_KEY;

public class PipeTransferBatchReqBuilder implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferBatchReqBuilder.class);

  private final boolean useLeaderCache;

  private final int requestMaxDelayInMs;
  private final long requestMaxBatchSizeInBytes;

  private Histogram tabletBatchSizeHistogram = new DoNothingHistogram();
  private Histogram tsFileBatchSizeHistogram = new DoNothingHistogram();
  private Histogram tabletBatchTimeIntervalHistogram = new DoNothingHistogram();
  private Histogram tsFileBatchTimeIntervalHistogram = new DoNothingHistogram();

  private Histogram eventSizeHistogram = new DoNothingHistogram();

  // If the leader cache is disabled (or unable to find the endpoint of event in the leader cache),
  // the event will be stored in the default batch.
  private final PipeTabletEventBatch defaultBatch;
  // If the leader cache is enabled, the batch will be divided by the leader endpoint,
  // each endpoint has a batch.
  // This is only used in plain batch since tsfile does not return redirection info.
  private final Map<TEndPoint, PipeTabletEventPlainBatch> endPointToBatch =
      new ConcurrentHashMap<>();

  public PipeTransferBatchReqBuilder(final PipeParameters parameters) {
    final boolean usingTsFileBatch =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_FORMAT_KEY, SINK_FORMAT_KEY), CONNECTOR_FORMAT_HYBRID_VALUE)
            .equals(CONNECTOR_FORMAT_TS_FILE_VALUE);

    useLeaderCache =
        !usingTsFileBatch
            && parameters.getBooleanOrDefault(
                Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
                CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE);

    final Integer requestMaxDelayInMillis =
        parameters.getIntByKeys(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, SINK_IOTDB_BATCH_DELAY_MS_KEY);
    if (Objects.isNull(requestMaxDelayInMillis)) {
      final int requestMaxDelayConfig =
          parameters.getIntOrDefault(
              Arrays.asList(
                  CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY, SINK_IOTDB_BATCH_DELAY_SECONDS_KEY),
              usingTsFileBatch
                  ? CONNECTOR_IOTDB_TS_FILE_BATCH_DELAY_DEFAULT_VALUE * 1000
                  : CONNECTOR_IOTDB_BATCH_DELAY_MS_DEFAULT_VALUE);
      requestMaxDelayInMs = requestMaxDelayConfig < 0 ? Integer.MAX_VALUE : requestMaxDelayConfig;
    } else {
      requestMaxDelayInMs =
          requestMaxDelayInMillis < 0 ? Integer.MAX_VALUE : requestMaxDelayInMillis;
    }
    requestMaxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            usingTsFileBatch
                ? CONNECTOR_IOTDB_TS_FILE_BATCH_SIZE_DEFAULT_VALUE
                : CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE);
    this.defaultBatch =
        usingTsFileBatch
            ? new PipeTabletEventTsFileBatch(
                requestMaxDelayInMs, requestMaxBatchSizeInBytes, this::recordTsFileMetric)
            : new PipeTabletEventPlainBatch(
                requestMaxDelayInMs, requestMaxBatchSizeInBytes, this::recordTabletMetric);
  }

  /**
   * Try offer {@link Event} into the corresponding batch if the given {@link Event} is not
   * duplicated.
   *
   * @param event the given {@link Event}
   */
  public synchronized void onEvent(final TabletInsertionEvent event)
      throws IOException, WALPipeException {
    if (!(event instanceof EnrichedEvent)) {
      LOGGER.warn(
          "Unsupported event {} type {} when building transfer request", event, event.getClass());
      return;
    }

    if (!useLeaderCache) {
      defaultBatch.onEvent(event);
      return;
    }

    String deviceId = null;
    if (event instanceof PipeRawTabletInsertionEvent) {
      deviceId = ((PipeRawTabletInsertionEvent) event).getDeviceId();
    } else if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      deviceId = ((PipeInsertNodeTabletInsertionEvent) event).getDeviceId();
    }

    if (Objects.isNull(deviceId)) {
      defaultBatch.onEvent(event);
      return;
    }

    final TEndPoint endPoint =
        IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId);
    if (Objects.isNull(endPoint)) {
      defaultBatch.onEvent(event);
      return;
    }
    endPointToBatch
        .computeIfAbsent(
            endPoint,
            k ->
                new PipeTabletEventPlainBatch(
                    requestMaxDelayInMs, requestMaxBatchSizeInBytes, this::recordTabletMetric))
        .onEvent(event);
  }

  /** Get all batches that have at least 1 event. */
  public synchronized List<Pair<TEndPoint, PipeTabletEventBatch>>
      getAllNonEmptyAndShouldEmitBatches() {
    final List<Pair<TEndPoint, PipeTabletEventBatch>> nonEmptyAndShouldEmitBatches =
        new ArrayList<>();
    if (!defaultBatch.isEmpty() && defaultBatch.shouldEmit()) {
      nonEmptyAndShouldEmitBatches.add(new Pair<>(null, defaultBatch));
    }
    endPointToBatch.forEach(
        (endPoint, batch) -> {
          if (!batch.isEmpty() && batch.shouldEmit()) {
            nonEmptyAndShouldEmitBatches.add(new Pair<>(endPoint, batch));
          }
        });
    return nonEmptyAndShouldEmitBatches;
  }

  public boolean isEmpty() {
    return defaultBatch.isEmpty()
        && endPointToBatch.values().stream().allMatch(PipeTabletEventPlainBatch::isEmpty);
  }

  public synchronized void discardEventsOfPipe(final String pipeNameToDrop, final int regionId) {
    defaultBatch.discardEventsOfPipe(pipeNameToDrop, regionId);
    endPointToBatch.values().forEach(batch -> batch.discardEventsOfPipe(pipeNameToDrop, regionId));
  }

  public int size() {
    try {
      return defaultBatch.events.size()
          + endPointToBatch.values().stream()
              .map(batch -> batch.events.size())
              .reduce(0, Integer::sum);
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to get the size of PipeTransferBatchReqBuilder, return 0. Exception: {}",
          e.getMessage(),
          e);
      return 0;
    }
  }

  @Override
  public synchronized void close() {
    defaultBatch.close();
    endPointToBatch.values().forEach(PipeTabletEventPlainBatch::close);
  }

  public void recordTabletMetric(long timeInterval, long bufferSize, long eventSize) {
    this.tabletBatchTimeIntervalHistogram.update(timeInterval);
    this.tabletBatchSizeHistogram.update(bufferSize);
    this.eventSizeHistogram.update(eventSize);
  }

  public void recordTsFileMetric(long timeInterval, long bufferSize, long eventSize) {
    this.tsFileBatchTimeIntervalHistogram.update(timeInterval);
    this.tsFileBatchSizeHistogram.update(bufferSize);
    this.eventSizeHistogram.update(eventSize);
  }

  public void setTabletBatchSizeHistogram(Histogram tabletBatchSizeHistogram) {
    if (tabletBatchSizeHistogram != null) {
      this.tabletBatchSizeHistogram = tabletBatchSizeHistogram;
    }
  }

  public void setTsFileBatchSizeHistogram(Histogram tsFileBatchSizeHistogram) {
    if (tsFileBatchSizeHistogram != null) {
      this.tsFileBatchSizeHistogram = tsFileBatchSizeHistogram;
    }
  }

  public void setTabletBatchTimeIntervalHistogram(Histogram tabletBatchTimeIntervalHistogram) {
    if (tabletBatchTimeIntervalHistogram != null) {
      this.tabletBatchTimeIntervalHistogram = tabletBatchTimeIntervalHistogram;
    }
  }

  public void setTsFileBatchTimeIntervalHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    if (tsFileBatchTimeIntervalHistogram != null) {
      this.tsFileBatchTimeIntervalHistogram = tsFileBatchTimeIntervalHistogram;
    }
  }

  public void setEventSizeHistogram(Histogram eventSizeHistogram) {
    if (eventSizeHistogram != null) {
      this.eventSizeHistogram = eventSizeHistogram;
    }
  }
}
