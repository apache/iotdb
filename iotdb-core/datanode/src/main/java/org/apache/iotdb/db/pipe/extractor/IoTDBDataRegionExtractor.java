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

package org.apache.iotdb.db.pipe.extractor;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.extractor.historical.PipeHistoricalDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.historical.PipeHistoricalDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionFakeExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionLogExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.metric.PipeExtractorMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_LOG_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_MODE_KEY;

public class IoTDBDataRegionExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionExtractor.class);

  private final AtomicBoolean hasBeenStarted;

  private PipeHistoricalDataRegionExtractor historicalExtractor;
  private PipeRealtimeDataRegionExtractor realtimeExtractor;

  private String taskID;
  private int dataRegionId;

  public IoTDBDataRegionExtractor() {
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Validate extractor.history.enable and extractor.realtime.enable
    validator
        .validateAttributeValueRange(
            EXTRACTOR_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            EXTRACTOR_REALTIME_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            SOURCE_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            SOURCE_REALTIME_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validate(
            args -> (boolean) args[0] || (boolean) args[1],
            "Should not set both history.enable and realtime.enable to false.",
            validator
                .getParameters()
                .getBooleanOrDefault(
                    Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                    EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE),
            validator
                .getParameters()
                .getBooleanOrDefault(
                    Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
                    EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE));

    // Validate extractor.realtime.mode
    if (validator
        .getParameters()
        .getBooleanOrDefault(
            Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
            EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      validator.validateAttributeValueRange(
          validator.getParameters().hasAttribute(EXTRACTOR_REALTIME_MODE_KEY)
              ? EXTRACTOR_REALTIME_MODE_KEY
              : SOURCE_REALTIME_MODE_KEY,
          true,
          EXTRACTOR_REALTIME_MODE_FILE_VALUE,
          EXTRACTOR_REALTIME_MODE_HYBRID_VALUE,
          EXTRACTOR_REALTIME_MODE_LOG_VALUE,
          EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE);
    }

    constructHistoricalExtractor();
    constructRealtimeExtractor(validator.getParameters());

    historicalExtractor.validate(validator);
    realtimeExtractor.validate(validator);
  }

  private void constructHistoricalExtractor() {
    // Enable historical extractor by default
    historicalExtractor = new PipeHistoricalDataRegionTsFileExtractor();
  }

  private void constructRealtimeExtractor(PipeParameters parameters) {
    // Enable realtime extractor by default
    if (!parameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      realtimeExtractor = new PipeRealtimeDataRegionFakeExtractor();
      LOGGER.info(
          "'{}' is set to false, use fake realtime extractor.", EXTRACTOR_REALTIME_ENABLE_KEY);
      return;
    }

    // Use hybrid mode by default
    if (!parameters.hasAnyAttributes(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
      LOGGER.info("'{}' is not set, use hybrid mode by default.", EXTRACTOR_REALTIME_MODE_KEY);
      return;
    }

    switch (parameters.getString(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      case EXTRACTOR_REALTIME_MODE_FILE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionTsFileExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_HYBRID_VALUE:
      case EXTRACTOR_REALTIME_MODE_LOG_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionLogExtractor();
        break;
      default:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Unsupported extractor realtime mode: {}, create a hybrid extractor.",
              parameters.getString(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY));
        }
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    dataRegionId =
        ((PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment()).getRegionId();
    String pipeName = configuration.getRuntimeEnvironment().getPipeName();
    long creationTime = configuration.getRuntimeEnvironment().getCreationTime();
    taskID = pipeName + "_" + dataRegionId + "_" + creationTime;

    historicalExtractor.customize(parameters, configuration);
    realtimeExtractor.customize(parameters, configuration);

    // register metric after generating taskID
    PipeExtractorMetrics.getInstance().register(this);
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);

    final AtomicReference<Exception> exceptionHolder = new AtomicReference<>(null);
    final DataRegionId dataRegionIdObject = new DataRegionId(this.dataRegionId);
    while (true) {
      // try to start extractors in the data region ...
      // first try to run if data region exists, then try to run if data region does not exist.
      // both conditions fail is not common, which means the data region is created during the
      // runIfPresent and runIfAbsent operations. in this case, we need to retry.
      if (StorageEngine.getInstance()
              .runIfPresent(
                  dataRegionIdObject,
                  (dataRegion -> {
                    dataRegion.writeLock(
                        String.format(
                            "Pipe: starting %s", IoTDBDataRegionExtractor.class.getName()));
                    try {
                      startHistoricalExtractorAndRealtimeExtractor(exceptionHolder);
                    } finally {
                      dataRegion.writeUnlock();
                    }
                  }))
          || StorageEngine.getInstance()
              .runIfAbsent(
                  dataRegionIdObject,
                  () -> startHistoricalExtractorAndRealtimeExtractor(exceptionHolder))) {
        rethrowExceptionIfAny(exceptionHolder);
        return;
      }
      rethrowExceptionIfAny(exceptionHolder);
    }
  }

  private void startHistoricalExtractorAndRealtimeExtractor(
      AtomicReference<Exception> exceptionHolder) {
    try {
      // Start realtimeExtractor first to avoid losing data. This may cause some
      // retransmission, yet it is OK according to the idempotency of IoTDB.
      // Note: The order of historical collection is flushing data -> adding all tsFile events.
      // There can still be writing when tsFile events are added. If we start
      // realtimeExtractor after the process, then this part of data will be lost.
      realtimeExtractor.start();
      historicalExtractor.start();
    } catch (Exception e) {
      exceptionHolder.set(e);
      LOGGER.warn(
          String.format(
              "Start historical extractor %s and realtime extractor %s error.",
              historicalExtractor, realtimeExtractor),
          e);
    }
  }

  private void rethrowExceptionIfAny(AtomicReference<Exception> exceptionHolder) {
    if (exceptionHolder.get() != null) {
      throw new PipeException("failed to start extractors.", exceptionHolder.get());
    }
  }

  @Override
  public Event supply() throws Exception {
    Event event =
        historicalExtractor.hasConsumedAll()
            ? realtimeExtractor.supply()
            : historicalExtractor.supply();
    if (Objects.nonNull(event)) {
      if (event instanceof TabletInsertionEvent) {
        PipeExtractorMetrics.getInstance().markTabletEvent(taskID);
      } else if (event instanceof TsFileInsertionEvent) {
        PipeExtractorMetrics.getInstance().markTsFileEvent(taskID);
      } else if (event instanceof PipeHeartbeatEvent) {
        PipeExtractorMetrics.getInstance().markPipeHeartbeatEvent(taskID);
      }
    }
    return event;
  }

  @Override
  public void close() throws Exception {
    historicalExtractor.close();
    realtimeExtractor.close();
    PipeExtractorMetrics.getInstance().deregister(taskID);
  }

  public String getTaskID() {
    return taskID;
  }

  public int getHistoricalTsFileInsertionEventCount() {
    return hasBeenStarted.get() ? historicalExtractor.getPendingQueueSize() : 0;
  }

  public int getTabletInsertionEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getTabletInsertionEventCount() : 0;
  }

  public int getRealtimeTsFileInsertionEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getTsFileInsertionEventCount() : 0;
  }

  public int getPipeHeartbeatEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getPipeHeartbeatEventCount() : 0;
  }
}
