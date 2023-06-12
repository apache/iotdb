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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.pipe.collector.historical.PipeHistoricalDataRegionCollector;
import org.apache.iotdb.db.pipe.collector.historical.PipeHistoricalDataRegionTsFileCollector;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionFakeCollector;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionHybridCollector;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionLogCollector;
import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionTsFileCollector;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_REALTIME_ENABLE;
import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_REALTIME_MODE;
import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_FILE;
import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_HYBRID;
import static org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_LOG;

public class IoTDBDataRegionCollector implements PipeCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionCollector.class);

  private final AtomicBoolean hasBeenStarted;

  private PipeHistoricalDataRegionCollector historicalCollector;
  private PipeRealtimeDataRegionCollector realtimeCollector;

  private DataRegionId dataRegionId;

  public IoTDBDataRegionCollector() {
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

    constructHistoricalCollector(validator.getParameters());
    constructRealtimeCollector(validator.getParameters());

    historicalCollector.validate(validator);
    realtimeCollector.validate(validator);
  }

  private void constructHistoricalCollector(PipeParameters parameters) {
    // enable historical collector by default
    historicalCollector = new PipeHistoricalDataRegionTsFileCollector();
  }

  private void constructRealtimeCollector(PipeParameters parameters) {
    // enable realtime collector by default
    if (!parameters.getBooleanOrDefault(COLLECTOR_REALTIME_ENABLE, true)) {
      realtimeCollector = new PipeRealtimeDataRegionFakeCollector();
      return;
    }

    // use hybrid mode by default
    if (!parameters.hasAttribute(COLLECTOR_REALTIME_MODE)) {
      realtimeCollector = new PipeRealtimeDataRegionHybridCollector();
      return;
    }

    switch (parameters.getString(COLLECTOR_REALTIME_MODE)) {
      case COLLECTOR_REALTIME_MODE_FILE:
        realtimeCollector = new PipeRealtimeDataRegionTsFileCollector();
        break;
      case COLLECTOR_REALTIME_MODE_LOG:
        realtimeCollector = new PipeRealtimeDataRegionLogCollector();
        break;
      case COLLECTOR_REALTIME_MODE_HYBRID:
        realtimeCollector = new PipeRealtimeDataRegionHybridCollector();
        break;
      default:
        realtimeCollector = new PipeRealtimeDataRegionHybridCollector();
        LOGGER.warn(
            String.format(
                "Unsupported collector realtime mode: %s, create a hybrid collector.",
                parameters.getString(COLLECTOR_REALTIME_MODE)));
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    dataRegionId =
        (DataRegionId)
            ConsensusGroupId.Factory.createFromTConsensusGroupId(
                ((PipeTaskCollectorRuntimeEnvironment) configuration.getRuntimeEnvironment())
                    .getRegionId());

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
    while (true) {
      // try to start collectors in the data region ...
      // first try to run if data region exists, then try to run if data region does not exist.
      // both conditions fail is not common, which means the data region is created during the
      // runIfPresent and runIfAbsent operations. in this case, we need to retry.
      if (StorageEngine.getInstance()
              .runIfPresent(
                  dataRegionId,
                  (dataRegion -> {
                    dataRegion.writeLock(
                        String.format(
                            "Pipe: starting %s", IoTDBDataRegionCollector.class.getName()));
                    try {
                      startHistoricalCollectorAndRealtimeCollector(exceptionHolder);
                    } finally {
                      dataRegion.writeUnlock();
                    }
                  }))
          || StorageEngine.getInstance()
              .runIfAbsent(
                  dataRegionId,
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
