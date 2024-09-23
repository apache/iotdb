/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.extractor;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.hasAtLeastOneOption;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.optionsAreAllLegal;

public abstract class IoTDBExtractor implements PipeExtractor {

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  protected String taskID;
  protected String pipeName;
  protected long creationTime;
  protected int regionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected boolean isForwardingPipeRequests;

  // The value is always true after the first start even the extractor is closed
  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE))
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion.exclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE))
        .validate(
            args -> hasAtLeastOneOption((String) args[0], (String) args[1]),
            "The pipe inclusion content can't be empty.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE));
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        ((PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment());
    regionId = environment.getRegionId();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    taskID = pipeName + "_" + regionId + "_" + creationTime;
    pipeTaskMeta = environment.getPipeTaskMeta();

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                PipeExtractorConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getTaskID() {
    return taskID;
  }

  public String getPipeName() {
    return pipeName;
  }

  public int getRegionId() {
    return regionId;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
