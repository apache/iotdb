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

package org.apache.iotdb.db.pipe.core.collector.realtime;

import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;

public abstract class PipeRealtimeDataRegionCollector implements PipeCollector {

  protected String pattern;
  protected String dataRegionId;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeCollectorConstant.PATTERN_PATTERN_KEY);
    validator.validateRequiredAttribute(PipeCollectorConstant.PATTERN_DATA_REGION_KEY);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration) {
    pattern = parameters.getString(PipeCollectorConstant.PATTERN_PATTERN_KEY);
    dataRegionId = parameters.getString(PipeCollectorConstant.PATTERN_DATA_REGION_KEY);
  }

  @Override
  public void start() {
    PipeInsertionDataNodeListener.getInstance().startListenAndAssign(dataRegionId, this);
  }

  @Override
  public void close() {
    PipeInsertionDataNodeListener.getInstance().stopListenAndAssign(dataRegionId, this);
  }

  /** @param event the event from the storage engine */
  public abstract void collect(PipeRealtimeCollectEvent event);

  public final String getPattern() {
    return pattern;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionCollector{"
        + "pattern='"
        + pattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }
}
