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

import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;

public abstract class PipeRealtimeDataRegionCollector implements PipeCollector {

  protected final PipeTaskMeta pipeTaskMeta;

  protected String pattern;
  protected String dataRegionId;

  public PipeRealtimeDataRegionCollector(PipeTaskMeta pipeTaskMeta) {
    this.pipeTaskMeta = pipeTaskMeta;
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PipeCollectorConstant.DATA_REGION_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    pattern =
        parameters.getStringOrDefault(
            PipeCollectorConstant.COLLECTOR_PATTERN_KEY,
            PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE);
    dataRegionId = parameters.getString(PipeCollectorConstant.DATA_REGION_KEY);
  }

  @Override
  public void start() throws Exception {
    PipeInsertionDataNodeListener.getInstance().startListenAndAssign(dataRegionId, this);
  }

  @Override
  public void close() throws Exception {
    PipeInsertionDataNodeListener.getInstance().stopListenAndAssign(dataRegionId, this);
  }

  /** @param event the event from the storage engine */
  public abstract void collect(PipeRealtimeCollectEvent event);

  public final String getPattern() {
    return pattern;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
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
