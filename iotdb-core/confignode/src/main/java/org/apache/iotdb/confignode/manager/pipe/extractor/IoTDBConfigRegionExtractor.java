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
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.extractor.IoTDBNonDataRegionExtractor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigRegionExtractorMetrics;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@TreeModel
@TableModel
public class IoTDBConfigRegionExtractor extends IoTDBNonDataRegionExtractor {

  public static final PipeConfigPhysicalPlanTreePatternParseVisitor TREE_PATTERN_PARSE_VISITOR =
      new PipeConfigPhysicalPlanTreePatternParseVisitor();
  public static final PipeConfigPhysicalPlanTablePatternParseVisitor TABLE_PATTERN_PARSE_VISITOR =
      new PipeConfigPhysicalPlanTablePatternParseVisitor();
  public static final PipeConfigPhysicalPlanTreeScopeParseVisitor TREE_SCOPE_PARSE_VISITOR =
      new PipeConfigPhysicalPlanTreeScopeParseVisitor();
  public static final PipeConfigPhysicalPlanTableScopeParseVisitor TABLE_SCOPE_PARSE_VISITOR =
      new PipeConfigPhysicalPlanTableScopeParseVisitor();

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
    return parseConfigPlan(
            ((PipeConfigRegionWritePlanEvent) event).getConfigPhysicalPlan(),
            treePattern,
            tablePattern)
        .map(
            configPhysicalPlan ->
                new PipeConfigRegionWritePlanEvent(configPhysicalPlan, event.isGeneratedByPipe()));
  }

  public static Optional<ConfigPhysicalPlan> parseConfigPlan(
      final ConfigPhysicalPlan plan,
      final IoTDBTreePattern treePattern,
      final TablePattern tablePattern) {
    Optional<ConfigPhysicalPlan> result = Optional.empty();
    final Boolean isTableDatabasePlan = isTableDatabasePlan(plan);
    if (!Boolean.TRUE.equals(isTableDatabasePlan)) {
      result = TREE_PATTERN_PARSE_VISITOR.process(plan, treePattern);
      if (!result.isPresent()) {
        return result;
      }
    }
    if (!Boolean.FALSE.equals(isTableDatabasePlan) && result.isPresent()) {
      result = TABLE_PATTERN_PARSE_VISITOR.process(result.get(), tablePattern);
      if (!result.isPresent()) {
        return result;
      }
    }
    if (!treePattern.isTreeModelDataAllowedToBeCaptured() && result.isPresent()) {
      result = TREE_SCOPE_PARSE_VISITOR.process(result.get(), null);
      if (!result.isPresent()) {
        return result;
      }
    }
    if (!tablePattern.isTableModelDataAllowedToBeCaptured() && result.isPresent()) {
      result = TABLE_SCOPE_PARSE_VISITOR.process(result.get(), null);
      if (!result.isPresent()) {
        return result;
      }
    }
    return result;
  }

  @Override
  protected boolean isTypeListened(final PipeWritePlanEvent event) {
    return isTypeListened(
        ((PipeConfigRegionWritePlanEvent) event).getConfigPhysicalPlan(),
        listenedTypeSet,
        treePattern,
        tablePattern);
  }

  public static boolean isTypeListened(
      final ConfigPhysicalPlan plan,
      final Set<ConfigPhysicalPlanType> listenedTypeSet,
      final IoTDBTreePattern treePattern,
      final TablePattern tablePattern) {
    final Boolean isTableDatabasePlan = isTableDatabasePlan(plan);
    return listenedTypeSet.contains(plan.getType())
        && (Objects.isNull(isTableDatabasePlan)
            || Boolean.TRUE.equals(isTableDatabasePlan)
                && tablePattern.isTableModelDataAllowedToBeCaptured()
            || Boolean.FALSE.equals(isTableDatabasePlan)
                && treePattern.isTreeModelDataAllowedToBeCaptured());
  }

  private static Boolean isTableDatabasePlan(final ConfigPhysicalPlan plan) {
    if (plan instanceof DatabaseSchemaPlan) {
      return ((DatabaseSchemaPlan) plan).getSchema().isIsTableModel()
          ? Boolean.TRUE
          : Boolean.FALSE;
    } else if (plan instanceof DeleteDatabasePlan) {
      return PathUtils.isTableModelDatabase(((DeleteDatabasePlan) plan).getName())
          ? Boolean.TRUE
          : Boolean.FALSE;
    }
    return null;
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
