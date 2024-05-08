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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.client.IoTDBDataNodeCacheLeaderClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_LEADER_CACHE_ENABLE_KEY;

public class PipeTransferBatchReqBuilder implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferBatchReqBuilder.class);

  private final boolean useLeaderCache;
  private final Map<TEndPoint, PipeEventBatch> endPointToBatch = new HashMap<>();
  private final PipeEventBatch defaultBatch;

  // limit in delayed time
  private final int maxDelayInMs;
  private final long requestMaxBatchSizeInBytes;

  public PipeTransferBatchReqBuilder(final PipeParameters parameters) {
    this.maxDelayInMs =
        parameters.getIntOrDefault(
                Arrays.asList(CONNECTOR_IOTDB_BATCH_DELAY_KEY, SINK_IOTDB_BATCH_DELAY_KEY),
                CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE)
            * 1000;

    this.requestMaxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE);

    // leader cache configuration
    this.useLeaderCache =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
            CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE);

    this.defaultBatch = new PipeEventBatch(this.maxDelayInMs, this.requestMaxBatchSizeInBytes);
  }

  /**
   * Try offer {@link Event} into cache if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@link true} if the batch can be transferred
   */
  public synchronized Pair<TEndPoint, PipeEventBatch> onEvent(final TabletInsertionEvent event)
      throws IOException, WALPipeException {
    if (!useLeaderCache) {
      return defaultBatch.onEvent(event) ? new Pair<>(null, defaultBatch) : null;
    }

    if (!(event instanceof EnrichedEvent)) {
      return null;
    }

    String deviceId = null;
    if (event instanceof PipeRawTabletInsertionEvent) {
      deviceId = ((PipeRawTabletInsertionEvent) event).getDeviceId();
    } else if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      deviceId =
          ((PipeInsertNodeTabletInsertionEvent) event)
              .getInsertNode()
              .getDevicePath()
              .getFullPath();
    }

    if (Objects.isNull(deviceId)) {
      return defaultBatch.onEvent(event) ? new Pair<>(null, defaultBatch) : null;
    }

    TEndPoint endPoint =
        IoTDBDataNodeCacheLeaderClientManager.LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId);
    if (endPoint == null) {
      return defaultBatch.onEvent(event) ? new Pair<>(null, defaultBatch) : null;
    }

    PipeEventBatch batch =
        endPointToBatch.computeIfAbsent(
            endPoint, k -> new PipeEventBatch(maxDelayInMs, requestMaxBatchSizeInBytes));
    return batch.onEvent(event) ? new Pair<>(endPoint, batch) : null;
  }

  public List<Pair<TEndPoint, PipeEventBatch>> getAllNonEmptyBatches() {
    List<Pair<TEndPoint, PipeEventBatch>> nonEmptyBatches = new ArrayList<>();
    if (!defaultBatch.isEmpty()) {
      nonEmptyBatches.add(new Pair<>(null, defaultBatch));
    }
    endPointToBatch.forEach(
        (endPoint, batch) -> {
          if (!batch.isEmpty()) {
            nonEmptyBatches.add(new Pair<>(endPoint, batch));
          }
        });
    return nonEmptyBatches;
  }

  public synchronized void onSuccess() {
    for (PipeEventBatch batch : endPointToBatch.values()) {
      batch.onSuccess();
    }
    defaultBatch.onSuccess();
  }

  public synchronized void onSuccess(TEndPoint endPoint) {
    if (endPoint == null) {
      defaultBatch.onSuccess();
    }

    PipeEventBatch batch = endPointToBatch.get(endPoint);
    if (batch != null) {
      batch.onSuccess();
    }
  }

  public PipeTransferTabletBatchReq toTPipeTransferReq(TEndPoint endPoint) throws IOException {
    if (endPoint == null) {
      return defaultBatch.toTPipeTransferReq();
    }

    PipeEventBatch batch = endPointToBatch.get(endPoint);
    if (batch == null) {
      return null;
    }
    return batch.toTPipeTransferReq();
  }

  public boolean isEmpty() {
    return defaultBatch.isEmpty()
        && endPointToBatch.values().stream().allMatch(PipeEventBatch::isEmpty);
  }

  public List<Event> deepCopyEvents(TEndPoint endPoint) {
    if (endPoint == null) {
      return defaultBatch.deepCopyEvents();
    }

    PipeEventBatch batch = endPointToBatch.get(endPoint);
    if (batch == null) {
      return new ArrayList<>();
    }
    return batch.deepCopyEvents();
  }

  public List<Long> deepCopyRequestCommitIds(TEndPoint endPoint) {
    if (endPoint == null) {
      return defaultBatch.deepCopyRequestCommitIds();
    }

    PipeEventBatch batch = endPointToBatch.get(endPoint);
    if (batch == null) {
      return new ArrayList<>();
    }
    return batch.deepCopyRequestCommitIds();
  }

  @Override
  public synchronized void close() {
    endPointToBatch.values().forEach(PipeEventBatch::close);
    defaultBatch.close();
  }

  public void decreaseEventsReferenceCount(final String holderMessage, final boolean shouldReport) {
    /*
    for (final Event event : events) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).decreaseReferenceCount(holderMessage, shouldReport);
      }
    }*/
  }
}
