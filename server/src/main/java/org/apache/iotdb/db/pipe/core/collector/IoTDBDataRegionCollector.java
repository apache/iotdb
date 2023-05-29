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

package org.apache.iotdb.db.pipe.core.collector;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.historical.PIpeHistoricalDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.historical.PipeHistoricalDataRegionFakeCollector;
import org.apache.iotdb.db.pipe.core.collector.historical.PipeHistoricalDataRegionTsFileCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionFakeCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionHybridCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionTsFileCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionWalCollector;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_REALTIME_ENABLE;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_REALTIME_MODE;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_FILE;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_HYBRID;
import static org.apache.iotdb.db.pipe.config.PipeCollectorConstant.COLLECTOR_REALTIME_MODE_LOG;

public class IoTDBDataRegionCollector implements PipeCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionCollector.class);

  private final AtomicBoolean hasBeenStarted;

  private PipeRealtimeDataRegionCollector realtimeCollector;
  // TODO: support pattern in historical collector
  private PIpeHistoricalDataRegionCollector historicalCollector;

  private final ListenableUnboundedBlockingPendingQueue<Event> collectorPendingQueue;
  private final PipeTaskMeta pipeTaskMeta;

  private int dataRegionId;

  public IoTDBDataRegionCollector(
      PipeTaskMeta pipeTaskMeta,
      ListenableUnboundedBlockingPendingQueue<Event> collectorPendingQueue) {
    hasBeenStarted = new AtomicBoolean(false);

    this.pipeTaskMeta = pipeTaskMeta;
    this.collectorPendingQueue = collectorPendingQueue;

    realtimeCollector =
        new PipeRealtimeDataRegionHybridCollector(pipeTaskMeta, collectorPendingQueue);
    historicalCollector = new PipeHistoricalDataRegionTsFileCollector(pipeTaskMeta);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeCollectorConstant.DATA_REGION_KEY);

    // validate collector.history.enable and collector.realtime.enable
    validator
        .validateAttributeValueRange(
            COLLECTOR_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            COLLECTOR_REALTIME_ENABLE, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validate(
            args ->
                args[0] == null || args[1] == null || ((boolean) args[0]) || ((boolean) args[1]),
            String.format(
                "Should not set both %s and %s to false.",
                COLLECTOR_HISTORY_ENABLE_KEY, COLLECTOR_REALTIME_ENABLE),
            validator.getParameters().getBoolean(COLLECTOR_HISTORY_ENABLE_KEY),
            validator.getParameters().getBoolean(COLLECTOR_REALTIME_ENABLE));

    // validate collector.realtime.mode
    validator.validateAttributeValueRange(
        COLLECTOR_REALTIME_MODE,
        true,
        COLLECTOR_REALTIME_MODE_HYBRID,
        COLLECTOR_REALTIME_MODE_FILE,
        COLLECTOR_REALTIME_MODE_LOG);

    constructHistoricalCollector(validator.getParameters());
    constructRealtimeCollector(validator.getParameters());

    // TODO: require more attributes
    realtimeCollector.validate(validator);
    historicalCollector.validate(validator);
  }

  private void constructHistoricalCollector(PipeParameters parameters) {
    historicalCollector =
        (Boolean.FALSE.toString().equals(parameters.getString(COLLECTOR_HISTORY_ENABLE_KEY)))
            ? new PipeHistoricalDataRegionFakeCollector()
            : new PipeHistoricalDataRegionTsFileCollector(pipeTaskMeta);
  }

  private void constructRealtimeCollector(PipeParameters parameters) {
    if (Boolean.FALSE.toString().equals(parameters.getString(COLLECTOR_REALTIME_ENABLE))) {
      realtimeCollector = new PipeRealtimeDataRegionFakeCollector(pipeTaskMeta);
      return;
    }

    if (!parameters.hasAttribute(COLLECTOR_REALTIME_MODE)) {
      realtimeCollector =
          new PipeRealtimeDataRegionHybridCollector(pipeTaskMeta, collectorPendingQueue);
      return;
    }

    switch (parameters.getString(COLLECTOR_REALTIME_MODE)) {
      case COLLECTOR_REALTIME_MODE_FILE:
        realtimeCollector =
            new PipeRealtimeDataRegionTsFileCollector(pipeTaskMeta, collectorPendingQueue);
        break;
      case COLLECTOR_REALTIME_MODE_LOG:
        realtimeCollector =
            new PipeRealtimeDataRegionWalCollector(pipeTaskMeta, collectorPendingQueue);
        break;
      case COLLECTOR_REALTIME_MODE_HYBRID:
        realtimeCollector =
            new PipeRealtimeDataRegionHybridCollector(pipeTaskMeta, collectorPendingQueue);
        break;
      default:
        realtimeCollector =
            new PipeRealtimeDataRegionHybridCollector(pipeTaskMeta, collectorPendingQueue);
        LOGGER.warn(
            String.format(
                "Unsupported collector realtime mode: %s, create a hybrid collector.",
                parameters.getString(COLLECTOR_REALTIME_MODE)));
    }
  }

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    dataRegionId = parameters.getInt(PipeCollectorConstant.DATA_REGION_KEY);

    realtimeCollector.customize(parameters, configuration);
    historicalCollector.customize(parameters, configuration);
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);

    while (true) {
      // try to start collectors in the data region ...
      // first try to run if data region exists, then try to run if data region does not exist.
      // both conditions fail is not common, which means the data region is created during the
      // runIfPresent and runIfAbsent operations. in this case, we need to retry.
      if (StorageEngine.getInstance()
              .runIfPresent(
                  new DataRegionId(dataRegionId),
                  (dataRegion -> {
                    dataRegion.writeLock(
                        String.format(
                            "Pipe: starting %s", IoTDBDataRegionCollector.class.getName()));
                    try {
                      startHistoricalCollectorAndRealtimeCollector();
                    } finally {
                      dataRegion.writeUnlock();
                    }
                  }))
          || StorageEngine.getInstance()
              .runIfAbsent(
                  new DataRegionId(dataRegionId),
                  this::startHistoricalCollectorAndRealtimeCollector)) {
        return;
      }
    }
  }

  public void startHistoricalCollectorAndRealtimeCollector() {
    try {
      historicalCollector.start();
      realtimeCollector.start();
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Start historical collector %s and realtime collector %s error.",
              historicalCollector, realtimeCollector),
          e);
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
