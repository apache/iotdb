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

package org.apache.iotdb.db.pipe.extractor.realtime;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;

public abstract class PipeRealtimeDataRegionExtractor implements PipeExtractor {

  protected String pipeName;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected String pattern;
  private boolean isDbNameCoveredByPattern = false;

  protected boolean isForwardingPipeRequests;

  // This queue is used to store pending events extracted by the method extract(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  protected final UnboundedBlockingPendingQueue<Event> pendingQueue =
      new UnboundedBlockingPendingQueue<>();

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  private String taskID;

  protected PipeRealtimeDataRegionExtractor() {
    // Do nothing
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    dataRegionId = String.valueOf(environment.getRegionId());
    pipeTaskMeta = environment.getPipeTaskMeta();

    pattern =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY),
            PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE);
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (dataRegion != null) {
      final String databaseName = dataRegion.getDatabaseName();
      if (databaseName != null
          && pattern.length() <= databaseName.length()
          && databaseName.startsWith(pattern)) {
        isDbNameCoveredByPattern = true;
      }
    }

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                PipeExtractorConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);

    // Metrics related to TsFileEpoch are managed in PipeExtractorMetrics. These metrics are
    // indexed by the taskID of IoTDBDataRegionExtractor. To avoid PipeRealtimeDataRegionExtractor
    // holding a reference to IoTDBDataRegionExtractor, the taskID should be constructed to
    // match that of IoTDBDataRegionExtractor.
    long creationTime = configuration.getRuntimeEnvironment().getCreationTime();
    taskID = pipeName + "_" + dataRegionId + "_" + creationTime;
  }

  @Override
  public void start() throws Exception {
    PipeInsertionDataNodeListener.getInstance().startListenAndAssign(dataRegionId, this);
  }

  @Override
  public void close() throws Exception {
    PipeInsertionDataNodeListener.getInstance().stopListenAndAssign(dataRegionId, this);

    synchronized (isClosed) {
      clearPendingQueue();
      isClosed.set(true);
    }
  }

  private void clearPendingQueue() {
    final List<Event> eventsToDrop = new ArrayList<>(pendingQueue.size());

    // processor stage is closed later than extractor stage, {@link supply()} may be called after
    // processor stage is closed. To avoid concurrent issues, we should clear the pending queue
    // before clearing all the reference count of the events in the pending queue.
    pendingQueue.forEach(eventsToDrop::add);
    pendingQueue.clear();

    eventsToDrop.forEach(
        event -> {
          if (event instanceof EnrichedEvent) {
            ((EnrichedEvent) event)
                .clearReferenceCount(PipeRealtimeDataRegionExtractor.class.getName());
          }
        });
  }

  /** @param event the event from the storage engine */
  public final void extract(PipeRealtimeEvent event) {
    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    doExtract(event);

    synchronized (isClosed) {
      if (isClosed.get()) {
        clearPendingQueue();
      }
    }
  }

  protected abstract void doExtract(PipeRealtimeEvent event);

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final String getPattern() {
    return pattern;
  }

  public final boolean isForwardingPipeRequests() {
    return isForwardingPipeRequests;
  }

  public final String getPipeName() {
    return pipeName;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionExtractor{"
        + "pattern='"
        + pattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }

  public int getTabletInsertionEventCount() {
    return pendingQueue.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return pendingQueue.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return pendingQueue.getPipeHeartbeatEventCount();
  }

  public String getTaskID() {
    return taskID;
  }
}
