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

package org.apache.iotdb.db.pipe.extractor.dataregion;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.extractor.IoTDBExtractor;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.historical.PipeHistoricalDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.historical.PipeHistoricalDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionFakeExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionLogExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.metric.PipeExtractorMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_WATERMARK_INTERVAL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_WATERMARK_INTERVAL_KEY;

public class IoTDBDataRegionExtractor extends IoTDBExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionExtractor.class);

  private PipeHistoricalDataRegionExtractor historicalExtractor;
  private PipeRealtimeDataRegionExtractor realtimeExtractor;

  private DataRegionWatermarkInjector watermarkInjector;

  private boolean hasNoExtractionNeed = true;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(
            validator.getParameters());
    if (insertionDeletionListeningOptionPair.getLeft().equals(false)
        && insertionDeletionListeningOptionPair.getRight().equals(false)) {
      return;
    }
    hasNoExtractionNeed = false;

    // Validate extractor.pattern.format is within valid range
    validator
        .validateAttributeValueRange(
            EXTRACTOR_PATTERN_FORMAT_KEY,
            true,
            EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE,
            EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE)
        .validateAttributeValueRange(
            SOURCE_PATTERN_FORMAT_KEY,
            true,
            EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE,
            EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE);

    // Get the pattern format to check whether the pattern is legal
    final PipePattern pattern =
        PipePattern.parsePipePatternFromSourceParameters(validator.getParameters());

    // Check whether the pattern is legal
    validatePattern(pattern);

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
                EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)
        || validator.getParameters().hasAnyAttributes(SOURCE_START_TIME_KEY, SOURCE_END_TIME_KEY)) {
      validator.validateAttributeValueRange(
          validator.getParameters().hasAttribute(EXTRACTOR_REALTIME_MODE_KEY)
              ? EXTRACTOR_REALTIME_MODE_KEY
              : SOURCE_REALTIME_MODE_KEY,
          true,
          EXTRACTOR_REALTIME_MODE_FILE_VALUE,
          EXTRACTOR_REALTIME_MODE_HYBRID_VALUE,
          EXTRACTOR_REALTIME_MODE_LOG_VALUE,
          EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE,
          EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE,
          EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE);
    }

    // Validate source.start-time and source.end-time
    if (validator.getParameters().hasAnyAttributes(SOURCE_START_TIME_KEY, SOURCE_END_TIME_KEY)
        && validator
            .getParameters()
            .hasAnyAttributes(
                EXTRACTOR_HISTORY_ENABLE_KEY,
                EXTRACTOR_REALTIME_ENABLE_KEY,
                SOURCE_HISTORY_ENABLE_KEY,
                SOURCE_REALTIME_ENABLE_KEY)) {
      LOGGER.warn(
          "When {}, {}, {} or {} is specified, specifying {}, {}, {} and {} is invalid.",
          SOURCE_START_TIME_KEY,
          EXTRACTOR_START_TIME_KEY,
          SOURCE_END_TIME_KEY,
          EXTRACTOR_END_TIME_KEY,
          SOURCE_HISTORY_START_TIME_KEY,
          EXTRACTOR_HISTORY_START_TIME_KEY,
          SOURCE_HISTORY_END_TIME_KEY,
          EXTRACTOR_HISTORY_END_TIME_KEY);
    }

    constructHistoricalExtractor();
    constructRealtimeExtractor(validator.getParameters());

    historicalExtractor.validate(validator);
    realtimeExtractor.validate(validator);
  }

  private void validatePattern(PipePattern pattern) {
    if (!pattern.isLegal()) {
      throw new IllegalArgumentException(String.format("Pattern \"%s\" is illegal.", pattern));
    }
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
          "Pipe: '{}' is set to false, use fake realtime extractor.",
          EXTRACTOR_REALTIME_ENABLE_KEY);
      return;
    }

    // Use hybrid mode by default
    if (!parameters.hasAnyAttributes(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
      LOGGER.info(
          "Pipe: '{}' is not set, use hybrid mode by default.", EXTRACTOR_REALTIME_MODE_KEY);
      return;
    }

    switch (parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      case EXTRACTOR_REALTIME_MODE_FILE_VALUE:
      case EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionTsFileExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_HYBRID_VALUE:
      case EXTRACTOR_REALTIME_MODE_LOG_VALUE:
      case EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionLogExtractor();
        break;
      default:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Pipe: Unsupported extractor realtime mode: {}, create a hybrid extractor.",
              parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY));
        }
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    if (hasNoExtractionNeed) {
      return;
    }

    super.customize(parameters, configuration);

    historicalExtractor.customize(parameters, configuration);
    realtimeExtractor.customize(parameters, configuration);

    // Set watermark injector
    if (parameters.hasAnyAttributes(
        EXTRACTOR_WATERMARK_INTERVAL_KEY, SOURCE_WATERMARK_INTERVAL_KEY)) {
      final long watermarkIntervalInMs =
          parameters.getLongOrDefault(
              Arrays.asList(EXTRACTOR_WATERMARK_INTERVAL_KEY, SOURCE_WATERMARK_INTERVAL_KEY),
              EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE);
      if (watermarkIntervalInMs > 0) {
        watermarkInjector = new DataRegionWatermarkInjector(regionId, watermarkIntervalInMs);
        LOGGER.info(
            "Pipe {}@{}: Set watermark injector with interval {} ms.",
            pipeName,
            regionId,
            watermarkInjector.getInjectionIntervalInMs());
      }
    }

    // register metric after generating taskID
    PipeExtractorMetrics.getInstance().register(this);
  }

  @Override
  public void start() throws Exception {
    if (hasNoExtractionNeed || hasBeenStarted.get()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    LOGGER.info(
        "Pipe {}@{}: Starting historical extractor {} and realtime extractor {}.",
        pipeName,
        regionId,
        historicalExtractor.getClass().getSimpleName(),
        realtimeExtractor.getClass().getSimpleName());

    super.start();

    final AtomicReference<Exception> exceptionHolder = new AtomicReference<>(null);
    final DataRegionId dataRegionIdObject = new DataRegionId(this.regionId);
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

        LOGGER.info(
            "Pipe {}@{}: Started historical extractor {} and realtime extractor {} successfully within {} ms.",
            pipeName,
            regionId,
            historicalExtractor.getClass().getSimpleName(),
            realtimeExtractor.getClass().getSimpleName(),
            System.currentTimeMillis() - startTime);
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
          "Pipe {}@{}: Start historical extractor {} and realtime extractor {} error.",
          pipeName,
          regionId,
          historicalExtractor.getClass().getSimpleName(),
          realtimeExtractor.getClass().getSimpleName(),
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
    if (hasNoExtractionNeed) {
      return null;
    }

    Event event = null;
    if (!historicalExtractor.hasConsumedAll()) {
      event = historicalExtractor.supply();
    } else {
      if (Objects.nonNull(watermarkInjector)) {
        event = watermarkInjector.inject();
      }
      if (Objects.isNull(event)) {
        event = realtimeExtractor.supply();
      }
    }

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
    if (hasNoExtractionNeed || !hasBeenStarted.get()) {
      return;
    }

    historicalExtractor.close();
    realtimeExtractor.close();
    if (Objects.nonNull(taskID)) {
      PipeExtractorMetrics.getInstance().deregister(taskID);
    }
  }

  //////////////////////////// APIs provided for detecting stuck ////////////////////////////

  public boolean isStreamMode() {
    return realtimeExtractor instanceof PipeRealtimeDataRegionHybridExtractor
        || realtimeExtractor instanceof PipeRealtimeDataRegionLogExtractor;
  }

  public boolean hasConsumedAllHistoricalTsFiles() {
    return historicalExtractor.hasConsumedAll();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getTaskID() {
    return taskID;
  }

  public String getPipeName() {
    return pipeName;
  }

  public int getDataRegionId() {
    return regionId;
  }

  public long getCreationTime() {
    return creationTime;
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
