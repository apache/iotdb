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
import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.iotdb.IoTDBCommonExtractor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.manager.pipe.event.PipeWriteConfigPlanEvent;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

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
  private Set<ConfigPhysicalPlanType> listenTypes = new HashSet<>();

  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr;

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
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
    long index;
    if (progressIndex instanceof MinimumProgressIndex) {
      // TODO: Trigger snapshot if not exists and return nearest snapshots' first index
      index = 0;
    } else {
      index = ((MetaProgressIndex) progressIndex).getIndex();
    }
    itr = ConfigPlanListeningQueue.getInstance().newIterator(index);
  }

  @Override
  public EnrichedEvent supply() {
    EnrichedEvent event;
    do {
      // Return immediately
      event = (EnrichedEvent) itr.next(0);
    } while (event instanceof PipeWriteConfigPlanEvent
        && !listenTypes.contains(((PipeWriteConfigPlanEvent) event).getPhysicalPlan().getType()));

    EnrichedEvent targetEvent =
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
    targetEvent.bindProgressIndex(new MetaProgressIndex(itr.getNextIndex() - 1));
    targetEvent.increaseReferenceCount(IoTDBConfigRegionExtractor.class.getName());
    return targetEvent;
  }

  @Override
  public void close() throws Exception {
    ConfigPlanListeningQueue.getInstance().returnIterator(itr);
  }
}
