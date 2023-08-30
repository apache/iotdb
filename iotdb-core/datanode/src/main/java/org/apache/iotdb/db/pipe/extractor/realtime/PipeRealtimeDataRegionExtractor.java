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

package org.apache.iotdb.db.pipe.extractor.realtime;

import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

public abstract class PipeRealtimeDataRegionExtractor implements PipeExtractor {

  protected String pattern;
  protected boolean isForwardingPipeRequests;

  protected String pipeName;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected PipeRealtimeDataRegionExtractor() {
    // Do nothing
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    pattern =
        parameters.getStringOrDefault(
            PipeExtractorConstant.EXTRACTOR_PATTERN_KEY,
            PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE);
    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);

    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
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
  public abstract void extract(PipeRealtimeEvent event);

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final String getPattern() {
    return pattern;
  }

  public final boolean isForwardingPipeRequests() {
    return isForwardingPipeRequests;
  }

  public final String getPipeName() {
    return pipeName;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  @Override
  public String toString() {
    return "PipeRealtimeDataRegionExtractor{"
        + "pattern='"
        + pattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + '}';
  }
}
