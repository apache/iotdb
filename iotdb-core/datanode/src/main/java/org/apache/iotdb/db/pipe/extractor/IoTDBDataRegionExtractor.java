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
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.extractor.historical.PipeHistoricalDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.historical.PipeHistoricalDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionFakeExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionLogExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionTsFileExtractor;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_FILE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_HYBRID;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_LOG;

public class IoTDBDataRegionExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionExtractor.class);

  private final AtomicBoolean hasBeenStarted;

  private PipeHistoricalDataRegionExtractor historicalExtractor;
  private PipeRealtimeDataRegionExtractor realtimeExtractor;

  private int dataRegionId;

  public IoTDBDataRegionExtractor() {
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // validate extractor.history.enable and extractor.realtime.enable
    validator
        .validateAttributeValueRange(
            EXTRACTOR_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            EXTRACTOR_REALTIME_ENABLE, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validate(
            args -> (boolean) args[0] || (boolean) args[1],
            String.format(
                "Should not set both %s and %s to false.",
                EXTRACTOR_HISTORY_ENABLE_KEY, EXTRACTOR_REALTIME_ENABLE),
            validator.getParameters().getBooleanOrDefault(EXTRACTOR_HISTORY_ENABLE_KEY, true),
            validator.getParameters().getBooleanOrDefault(EXTRACTOR_REALTIME_ENABLE, true));

    // validate extractor.realtime.mode
    if (validator.getParameters().getBooleanOrDefault(EXTRACTOR_REALTIME_ENABLE, true)) {
      validator.validateAttributeValueRange(
          EXTRACTOR_REALTIME_MODE,
          true,
          EXTRACTOR_REALTIME_MODE_HYBRID,
          EXTRACTOR_REALTIME_MODE_FILE,
          EXTRACTOR_REALTIME_MODE_LOG);
    }

    constructHistoricalExtractor();
    constructRealtimeExtractor(validator.getParameters());

    historicalExtractor.validate(validator);
    realtimeExtractor.validate(validator);
  }

  private void constructHistoricalExtractor() {
    // enable historical extractor by default
    historicalExtractor = new PipeHistoricalDataRegionTsFileExtractor();
  }

  private void constructRealtimeExtractor(PipeParameters parameters) {
    // enable realtime extractor by default
    if (!parameters.getBooleanOrDefault(EXTRACTOR_REALTIME_ENABLE, true)) {
      realtimeExtractor = new PipeRealtimeDataRegionFakeExtractor();
      return;
    }

    // use hybrid mode by default
    if (!parameters.hasAttribute(EXTRACTOR_REALTIME_MODE)) {
      realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
      return;
    }

    switch (parameters.getString(EXTRACTOR_REALTIME_MODE)) {
      case EXTRACTOR_REALTIME_MODE_FILE:
        realtimeExtractor = new PipeRealtimeDataRegionTsFileExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_LOG:
        realtimeExtractor = new PipeRealtimeDataRegionLogExtractor();
        break;
      case EXTRACTOR_REALTIME_MODE_HYBRID:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        break;
      default:
        realtimeExtractor = new PipeRealtimeDataRegionHybridExtractor();
        LOGGER.warn(
            "Unsupported extractor realtime mode: {}, create a hybrid extractor.",
            parameters.getString(EXTRACTOR_REALTIME_MODE));
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    dataRegionId =
        ((PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment()).getRegionId();

    historicalExtractor.customize(parameters, configuration);
    realtimeExtractor.customize(parameters, configuration);
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
      historicalExtractor.start();
      realtimeExtractor.start();
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
    return historicalExtractor.hasConsumedAll()
        ? realtimeExtractor.supply()
        : historicalExtractor.supply();
  }

  @Override
  public void close() throws Exception {
    historicalExtractor.close();
    realtimeExtractor.close();
  }
}
