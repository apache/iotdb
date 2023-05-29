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
import org.apache.iotdb.db.pipe.core.collector.historical.PipeHistoricalDataRegionTsFileCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionHybridCollector;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBDataRegionCollector implements PipeCollector {

  private final AtomicBoolean hasBeenStarted;

  private final PipeRealtimeDataRegionCollector realtimeCollector;
  // TODO: support pattern in historical collector
  private final PipeHistoricalDataRegionTsFileCollector historicalCollector;

  private int dataRegionId;

  public IoTDBDataRegionCollector(
      PipeTaskMeta pipeTaskMeta,
      ListenableUnboundedBlockingPendingQueue<Event> collectorPendingQueue) {
    hasBeenStarted = new AtomicBoolean(false);
    realtimeCollector =
        new PipeRealtimeDataRegionHybridCollector(pipeTaskMeta, collectorPendingQueue);
    historicalCollector = new PipeHistoricalDataRegionTsFileCollector(pipeTaskMeta);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeCollectorConstant.DATA_REGION_KEY);

    // TODO: require more attributes
    realtimeCollector.validate(validator);
    historicalCollector.validate(validator);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {
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
    historicalCollector.start();
    realtimeCollector.start();
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
