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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.extractor.IoTDBNonDataRegionExtractor;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigRegionExtractorMetrics;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class IoTDBConfigRegionExtractor extends IoTDBNonDataRegionExtractor {

  public static final PipeConfigPhysicalPlanPatternParseVisitor PATTERN_PARSE_VISITOR =
      new PipeConfigPhysicalPlanPatternParseVisitor();

  private Set<ConfigPhysicalPlanType> listenedTypeSet = new HashSet<>();

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    // TODO: Delete this
    if (ConfigNodeDescriptor.getInstance()
        .getConf()
        .getConfigNodeConsensusProtocolClass()
        .equals(ConsensusFactory.SIMPLE_CONSENSUS)) {
      throw new PipeException(
          "IoTDBConfigRegionExtractor does not transferring events under simple consensus");
    }

    super.customize(parameters, configuration);
    listenedTypeSet = ConfigRegionListeningFilter.parseListeningPlanTypeSet(parameters);

    PipeConfigRegionExtractorMetrics.getInstance().register(this);
    PipeConfigNodeRemainingTimeMetrics.getInstance().register(this);
  }

  @Override
  protected AbstractPipeListeningQueue getListeningQueue() {
    return PipeConfigNodeAgent.runtime().listener();
  }

  @Override
  protected boolean needTransferSnapshot() {
    return PipeConfigRegionSnapshotEvent.needTransferSnapshot(listenedTypeSet);
  }

  @Override
  protected void triggerSnapshot() {
    try {
      ConfigNode.getInstance()
          .getConfigManager()
          .getConsensusManager()
          .getConsensusImpl()
          .triggerSnapshot(
              new ConfigRegionId(ConfigNodeDescriptor.getInstance().getConf().getConfigRegionId()),
              true);
    } catch (final ConsensusException e) {
      throw new PipeException("Exception encountered when triggering schema region snapshot.", e);
    }
  }

  @Override
  public synchronized EnrichedEvent supply() throws Exception {
    final EnrichedEvent event = super.supply();
    PipeEventCommitManager.getInstance()
        .enrichWithCommitterKeyAndCommitId(event, creationTime, regionId);
    return event;
  }

  @Override
  protected long getMaxBlockingTimeMs() {
    // The connector continues to submit and relies on the queue to sleep if empty
    // Here we return with block to be consistent with the dataNode connector
    return PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
  }

  @Override
  protected Optional<PipeWritePlanEvent> trimRealtimeEventByPipePattern(
      final PipeWritePlanEvent event) {
    return PATTERN_PARSE_VISITOR
        .process(((PipeConfigRegionWritePlanEvent) event).getConfigPhysicalPlan(), pipePattern)
        .map(
            configPhysicalPlan ->
                new PipeConfigRegionWritePlanEvent(configPhysicalPlan, event.isGeneratedByPipe()));
  }

  @Override
  protected boolean isTypeListened(final PipeWritePlanEvent event) {
    return listenedTypeSet.contains(
        ((PipeConfigRegionWritePlanEvent) event).getConfigPhysicalPlan().getType());
  }

  @Override
  protected void confineHistoricalEventTransferTypes(final PipeSnapshotEvent event) {
    ((PipeConfigRegionSnapshotEvent) event).confineTransferredTypes(listenedTypeSet);
  }

  @Override
  public synchronized void close() throws Exception {
    if (hasBeenClosed.get()) {
      return;
    }
    hasBeenClosed.set(true);

    if (!hasBeenStarted.get()) {
      return;
    }
    super.close();

    if (Objects.nonNull(taskID)) {
      PipeConfigRegionExtractorMetrics.getInstance().deregister(taskID);
      PipeConfigNodeRemainingTimeMetrics.getInstance().deregister(pipeName + "_" + creationTime);
    }
  }
}
