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

package org.apache.iotdb.db.pipe.collector.realtime;

import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.collector.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

public abstract class PipeRealtimeDataRegionExtractor implements PipeExtractor {

  protected String pattern;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected PipeRealtimeDataRegionExtractor() {}

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    pattern =
        parameters.getStringOrDefault(
            PipeExtractorConstant.COLLECTOR_PATTERN_KEY,
            PipeExtractorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE);

    final PipeTaskCollectorRuntimeEnvironment environment =
        (PipeTaskCollectorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    dataRegionId = String.valueOf(environment.getRegionId());
    pipeTaskMeta = environment.getPipeTaskMeta();
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

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

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
