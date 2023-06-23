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

package org.apache.iotdb.db.pipe.collector;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.pipe.collector.historical.PipeHistoricalDataRegionExtractor;
import org.apache.iotdb.db.pipe.collector.historical.PipeHistoricalDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionFakeExtractor;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionLogExtractor;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionTsFileExtractor;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
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

import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_REALTIME_ENABLE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_REALTIME_MODE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_REALTIME_MODE_FILE;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_REALTIME_MODE_HYBRID;
import static org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant.COLLECTOR_REALTIME_MODE_LOG;

public class IoTDBDataRegionExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionExtractor.class);

  private final AtomicBoolean hasBeenStarted;

  private PipeHistoricalDataRegionExtractor historicalCollector;
  private PipeRealtimeDataRegionExtractor realtimeCollector;

  private int dataRegionId;

  public IoTDBDataRegionExtractor() {
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // validate collector.history.enable and collector.realtime.enable
    validator
        .validateAttributeValueRange(
            COLLECTOR_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            COLLECTOR_REALTIME_ENABLE, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validate(
            args -> (boolean) args[0] || (boolean) args[1],
            String.format(
                "Should not set both %s and %s to false.",
                COLLECTOR_HISTORY_ENABLE_KEY, COLLECTOR_REALTIME_ENABLE),
            validator.getParameters().getBooleanOrDefault(COLLECTOR_HISTORY_ENABLE_KEY, true),
            validator.getParameters().getBooleanOrDefault(COLLECTOR_REALTIME_ENABLE, true));

    // validate collector.realtime.mode
    if (validator.getParameters().getBooleanOrDefault(COLLECTOR_REALTIME_ENABLE, true)) {
      validator.validateAttributeValueRange(
          COLLECTOR_REALTIME_MODE,
          true,
          COLLECTOR_REALTIME_MODE_HYBRID,
          COLLECTOR_REALTIME_MODE_FILE,
          COLLECTOR_REALTIME_MODE_LOG);
    }

    constructHistoricalCollector();
    constructRealtimeCollector(validator.getParameters());

    historicalCollector.validate(validator);
    realtimeCollector.validate(validator);
  }

  private void constructHistoricalCollector() {
    // enable historical collector by default
    historicalCollector = new PipeHistoricalDataRegionTsFileExtractor();
  }

  private void constructRealtimeCollector(PipeParameters parameters) {
    // enable realtime collector by default
    if (!parameters.getBooleanOrDefault(COLLECTOR_REALTIME_ENABLE, true)) {
      realtimeCollector = new PipeRealtimeDataRegionFakeExtractor();
      return;
    }

    // use hybrid mode by default
    if (!parameters.hasAttribute(COLLECTOR_REALTIME_MODE)) {
      realtimeCollector = new PipeRealtimeDataRegionHybridExtractor();
      return;
    }

    switch (parameters.getString(COLLECTOR_REALTIME_MODE)) {
      case COLLECTOR_REALTIME_MODE_FILE:
        realtimeCollector = new PipeRealtimeDataRegionTsFileExtractor();
        break;
      case COLLECTOR_REALTIME_MODE_LOG:
        realtimeCollector = new PipeRealtimeDataRegionLogExtractor();
        break;
      case COLLECTOR_REALTIME_MODE_HYBRID:
        realtimeCollector = new PipeRealtimeDataRegionHybridExtractor();
        break;
      default:
        realtimeCollector = new PipeRealtimeDataRegionHybridExtractor();
        LOGGER.warn(
            "Unsupported collector realtime mode: {}, create a hybrid collector.",
            parameters.getString(COLLECTOR_REALTIME_MODE));
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    dataRegionId =
        ((PipeTaskCollectorRuntimeEnvironment) configuration.getRuntimeEnvironment()).getRegionId();

    historicalCollector.customize(parameters, configuration);
    realtimeCollector.customize(parameters, configuration);
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
      // try to start collectors in the data region ...
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
                      startHistoricalCollectorAndRealtimeCollector(exceptionHolder);
                    } finally {
                      dataRegion.writeUnlock();
                    }
                  }))
          || StorageEngine.getInstance()
              .runIfAbsent(
                  dataRegionIdObject,
                  () -> startHistoricalCollectorAndRealtimeCollector(exceptionHolder))) {
        rethrowExceptionIfAny(exceptionHolder);
        return;
      }
      rethrowExceptionIfAny(exceptionHolder);
    }
  }

  private void startHistoricalCollectorAndRealtimeCollector(
      AtomicReference<Exception> exceptionHolder) {
    try {
      historicalCollector.start();
      realtimeCollector.start();
    } catch (Exception e) {
      exceptionHolder.set(e);
      LOGGER.warn(
          String.format(
              "Start historical collector %s and realtime collector %s error.",
              historicalCollector, realtimeCollector),
          e);
    }
  }

  private void rethrowExceptionIfAny(AtomicReference<Exception> exceptionHolder) {
    if (exceptionHolder.get() != null) {
      throw new PipeException("failed to start collectors.", exceptionHolder.get());
    }
  }

  @Override
  public Event supply() throws Exception {
    return historicalCollector.hasConsumedAll()
        ? realtimeCollector.supply()
        : historicalCollector.supply();
  }

  @Override
  public void close() throws Exception {
    historicalCollector.close();
    realtimeCollector.close();
  }
}
