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

package org.apache.iotdb.confignode.manager.pipe.transfer.extractor;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.iotdb.IoTDBCommonExtractor;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class IoTDBConfigRegionExtractor extends IoTDBCommonExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigRegionExtractor.class);

  private PipeTaskMeta pipeTaskMeta;
  private Set<ConfigPhysicalPlanType> listenTypes = new HashSet<>();

  private ConcurrentIterableLinkedQueue<ConfigPhysicalPlan>.DynamicIterator itr;

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    pipeTaskMeta =
        ((PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment())
            .getPipeTaskMeta();
    listenTypes =
        PipeConfigPlanFilter.getPipeListenSet(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                EXTRACTOR_EXCLUSION_DEFAULT_VALUE),
            parameters.getBooleanOrDefault(
                EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE));
  }

  @Override
  public void start() throws Exception {
    ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    if (progressIndex instanceof MinimumProgressIndex) {
      // TODO: Listen to snapshot
    } else {
      itr =
          ConfigPlanListeningQueue.getInstance()
              .newIterator(((MetaProgressIndex) pipeTaskMeta.getProgressIndex()).getIndex());
    }
  }

  @Override
  public Event supply() {
    ConfigPhysicalPlan plan;
    do {
      plan = itr.next(1000);
    } while (plan != null && !listenTypes.contains(plan.getType()));
    // TODO: convert plan to event and configure timeout
    return null;
  }

  @Override
  public void close() throws Exception {
    ConfigPlanListeningQueue.getInstance().returnIterator(itr);
  }
}
