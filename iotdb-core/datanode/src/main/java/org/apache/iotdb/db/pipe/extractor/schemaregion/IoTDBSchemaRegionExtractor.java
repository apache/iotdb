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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.extractor.IoTDBNonDataRegionExtractor;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionExtractorMetrics;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@TreeModel
@TableModel
public class IoTDBSchemaRegionExtractor extends IoTDBNonDataRegionExtractor {
  public static final PipePlanTreePatternParseVisitor TREE_PATTERN_PARSE_VISITOR =
      new PipePlanTreePatternParseVisitor();
  public static final PipePlanTablePatternParseVisitor TABLE_PATTERN_PARSE_VISITOR =
      new PipePlanTablePatternParseVisitor();
  public static final PipePlanTablePrivilegeParseVisitor TABLE_PRIVILEGE_PARSE_VISITOR =
      new PipePlanTablePrivilegeParseVisitor();
  private static final PipeStatementToPlanVisitor STATEMENT_TO_PLAN_VISITOR =
      new PipeStatementToPlanVisitor();

  private SchemaRegionId schemaRegionId;

  private Set<PlanNodeType> listenedTypeSet = new HashSet<>();
  private String database;
  private SRStatementGenerator generator;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    // TODO: Delete this
    if (IoTDBDescriptor.getInstance()
        .getConfig()
        .getSchemaRegionConsensusProtocolClass()
        .equals(ConsensusFactory.SIMPLE_CONSENSUS)) {
      throw new PipeException(
          "IoTDBSchemaRegionExtractor does not transferring events under simple consensus");
    }

    super.customize(parameters, configuration);

    schemaRegionId = new SchemaRegionId(regionId);
    listenedTypeSet = SchemaRegionListeningFilter.parseListeningPlanTypeSet(parameters);

    PipeSchemaRegionExtractorMetrics.getInstance().register(this);
    PipeDataNodeRemainingEventAndTimeMetrics.getInstance().register(this);
  }

  @Override
  public void start() throws Exception {
    // Delay the start process to schema region leader ready
    if (!PipeDataNodeAgent.runtime().isSchemaLeaderReady(schemaRegionId)
        || hasBeenStarted.get()
        || hasBeenClosed.get()) {
      return;
    }

    // Try open the queue if it is the first task
    if (PipeDataNodeAgent.runtime().increaseAndGetSchemaListenerReferenceCount(schemaRegionId)
        == 1) {
      SchemaRegionConsensusImpl.getInstance()
          .write(schemaRegionId, new PipeOperateSchemaQueueNode(new PlanNodeId(""), true));
      database = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId).getDatabaseFullPath();
    }

    super.start();
  }

  @Override
  protected boolean needTransferSnapshot() {
    // Note: the schema region will transfer snapshot if there are table or tree planNode captured.
    // However, the schema region can be only tree model or table model, thus the snapshot can be
    // omitted if the schema region and transferred type's model are mismatched. Actually, the
    // mismatched subtask is supposed to be trimmed on configNode and will not be created here,
    // hence there's no need to optimize it here.
    return PipeSchemaRegionSnapshotEvent.needTransferSnapshot(listenedTypeSet);
  }

  @Override
  protected void triggerSnapshot() {
    try {
      SchemaRegionConsensusImpl.getInstance().triggerSnapshot(schemaRegionId, true);
    } catch (final ConsensusException e) {
      throw new PipeException("Exception encountered when triggering schema region snapshot.", e);
    }
  }

  // This method will return events only after schema region leader gets ready
  @Override
  public synchronized EnrichedEvent supply() throws Exception {
    return PipeDataNodeAgent.runtime().isSchemaLeaderReady(schemaRegionId) ? super.supply() : null;
  }

  @Override
  protected long getMaxBlockingTimeMs() {
    // The dataNode processor can sleep if it supplies null
    // Here we return immediately to be consistent with the data region extractor
    return 0;
  }

  @Override
  protected boolean canSkipSnapshotPrivilegeCheck(final PipeSnapshotEvent event) {
    try {
      if (PathUtils.isTableModelDatabase(database)) {
        Coordinator.getInstance()
            .getAccessControl()
            .checkCanSelectFromDatabase4Pipe(userName, database);
      }
      return true;
    } catch (final AccessDeniedException e) {
      return false;
    }
  }

  @Override
  protected void initSnapshotGenerator(final PipeSnapshotEvent event)
      throws IOException, IllegalPathException {
    final PipeSchemaRegionSnapshotEvent snapshotEvent = (PipeSchemaRegionSnapshotEvent) event;
    generator =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(snapshotEvent.getMTreeSnapshotFile().getPath()),
            Objects.nonNull(snapshotEvent.getTagLogSnapshotFile())
                ? Paths.get(snapshotEvent.getTagLogSnapshotFile().getPath())
                : null,
            Objects.nonNull(snapshotEvent.getAttributeSnapshotFile())
                ? Paths.get(snapshotEvent.getAttributeSnapshotFile().getPath())
                : null,
            PartialPath.getQualifiedDatabasePartialPath(database));
  }

  @Override
  protected boolean hasNextEventInCurrentSnapshot() {
    return Objects.nonNull(generator) && generator.hasNext();
  }

  @Override
  protected PipeWritePlanEvent getNextEventInCurrentSnapshot() {
    // Currently only support table model event
    return new PipeSchemaRegionWritePlanEvent(
        STATEMENT_TO_PLAN_VISITOR.process((Node) generator.next()), false);
  }

  @Override
  protected Optional<PipeWritePlanEvent> trimRealtimeEventByPrivilege(
      final PipeWritePlanEvent event) throws AccessDeniedException {
    final Optional<PlanNode> result =
        TABLE_PRIVILEGE_PARSE_VISITOR.process(
            ((PipeSchemaRegionWritePlanEvent) event).getPlanNode(), userName);
    if (result.isPresent()) {
      return Optional.of(
          new PipeSchemaRegionWritePlanEvent(result.get(), event.isGeneratedByPipe()));
    }
    if (skipIfNoPrivileges) {
      return Optional.empty();
    }
    throw new AccessDeniedException(
        "Not has privilege to transfer event: "
            + ((PipeSchemaRegionWritePlanEvent) event).getPlanNode());
  }

  @Override
  protected Optional<PipeWritePlanEvent> trimRealtimeEventByPipePattern(
      final PipeWritePlanEvent event) {
    return TREE_PATTERN_PARSE_VISITOR
        .process(((PipeSchemaRegionWritePlanEvent) event).getPlanNode(), treePattern)
        .flatMap(
            planNode ->
                TABLE_PATTERN_PARSE_VISITOR
                    .process(((PipeSchemaRegionWritePlanEvent) event).getPlanNode(), tablePattern)
                    .map(
                        planNode1 ->
                            new PipeSchemaRegionWritePlanEvent(
                                planNode1, event.isGeneratedByPipe())));
  }

  @Override
  protected AbstractPipeListeningQueue getListeningQueue() {
    return PipeDataNodeAgent.runtime().schemaListener(schemaRegionId);
  }

  @Override
  protected boolean isTypeListened(final PipeWritePlanEvent event) {
    final PlanNode planNode = ((PipeSchemaRegionWritePlanEvent) event).getPlanNode();
    return listenedTypeSet.contains(
        (planNode.getType() == PlanNodeType.ALTER_TIME_SERIES
                && ((AlterTimeSeriesNode) planNode).isAlterView())
            ? PlanNodeType.ALTER_LOGICAL_VIEW
            : planNode.getType());
  }

  @Override
  protected void confineHistoricalEventTransferTypes(final PipeSnapshotEvent event) {
    ((PipeSchemaRegionSnapshotEvent) event).confineTransferredTypes(listenedTypeSet);
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

    if (!listenedTypeSet.isEmpty()) {
      // The queue is not closed here, and is closed iff the PipeMetaKeeper
      // has no schema pipe after one successful sync
      PipeDataNodeAgent.runtime().decreaseAndGetSchemaListenerReferenceCount(schemaRegionId);
    }
    if (Objects.nonNull(taskID)) {
      PipeSchemaRegionExtractorMetrics.getInstance().deregister(taskID);
    }
  }
}
