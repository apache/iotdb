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

import org.apache.iotdb.db.pipe.config.PipeConstant;
import org.apache.iotdb.db.pipe.core.collector.historical.PipeHistoricalTsFileCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeHybridDataRegionCollector;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicBoolean;

public class PipeIoTDBCollector implements PipeCollector {
  private final AtomicBoolean hasBeenStarted;
  private final String dataRegionId;
  private PipeRealtimeDataRegionCollector realtimeCollector;
  private PipeHistoricalTsFileCollector historicalCollector;

  public PipeIoTDBCollector(String dataRegionId) {
    this.hasBeenStarted = new AtomicBoolean(false);
    this.dataRegionId = dataRegionId;
  }

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    String pattern = parameters.getString(PipeConstant.PATTERN_ARGUMENT_NAME);
    this.realtimeCollector = new PipeRealtimeHybridDataRegionCollector(pattern, dataRegionId);
    this.historicalCollector = new PipeHistoricalTsFileCollector(dataRegionId);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeConstant.PATTERN_ARGUMENT_NAME);
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }

    hasBeenStarted.set(true);
    realtimeCollector.start();
    historicalCollector.start();
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
