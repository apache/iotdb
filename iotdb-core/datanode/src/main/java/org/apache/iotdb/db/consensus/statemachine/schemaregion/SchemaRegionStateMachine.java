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

package org.apache.iotdb.db.consensus.statemachine.schemaregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.ratis.utils.Utils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.BaseStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.GeneralRegionAttributeSecurityService;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

public class SchemaRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionStateMachine.class);

  private final ISchemaRegion schemaRegion;
  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  public SchemaRegionStateMachine(final ISchemaRegion schemaRegion) {
    this.schemaRegion = schemaRegion;
  }

  @Override
  public void start() {
    // Do nothing
  }

  @Override
  public void stop() {
    // Stop leader related service for schema pipe
    PipeDataNodeAgent.runtime().notifySchemaLeaderUnavailable(schemaRegion.getSchemaRegionId());
    GeneralRegionAttributeSecurityService.getInstance().stopBroadcast(schemaRegion);
  }

  @Override
  public void notifyLeaderChanged(final ConsensusGroupId groupId, final int newLeaderId) {
    if (newLeaderId != IoTDBDescriptor.getInstance().getConfig().getDataNodeId()) {
      logger.info(
          "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], "
              + "the new leader is [nodeId:{}]",
          IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
          schemaRegion.getSchemaRegionId(),
          newLeaderId);
    }
  }

  @Override
  public void notifyNotLeader() {
    final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    logger.info(
        "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], "
            + "start cleaning up related services.",
        dataNodeId,
        schemaRegion.getSchemaRegionId());

    // Shutdown leader related service for schema pipe
    PipeDataNodeAgent.runtime().notifySchemaLeaderUnavailable(schemaRegion.getSchemaRegionId());
    GeneralRegionAttributeSecurityService.getInstance().stopBroadcast(schemaRegion);

    logger.info(
        "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], "
            + "all services on old leader are unavailable now.",
        dataNodeId,
        schemaRegion.getSchemaRegionId());
  }

  @Override
  public void notifyLeaderReady() {
    logger.info(
        "Current node [nodeId: {}] becomes schema region leader [regionId: {}]",
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        schemaRegion.getSchemaRegionId());

    // Activate leader related service for schema pipe
    PipeDataNodeAgent.runtime().notifySchemaLeaderReady(schemaRegion.getSchemaRegionId());
    GeneralRegionAttributeSecurityService.getInstance().startBroadcast(schemaRegion);

    logger.info(
        "Current node [nodeId: {}] as schema region leader [regionId: {}] is ready to work",
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
        schemaRegion.getSchemaRegionId());
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  @Override
  public boolean takeSnapshot(final File snapshotDir) {
    if (schemaRegion.createSnapshot(snapshotDir)
        && PipeDataNodeAgent.runtime()
            .schemaListener(schemaRegion.getSchemaRegionId())
            .createSnapshot(snapshotDir)) {
      listen2Snapshot4PipeListener(true);
      return true;
    }
    return false;
  }

  @Override
  public void loadSnapshot(final File latestSnapshotRootDir) {
    schemaRegion.loadSnapshot(latestSnapshotRootDir);
    PipeDataNodeAgent.runtime()
        .schemaListener(schemaRegion.getSchemaRegionId())
        .loadSnapshot(latestSnapshotRootDir);
    // We recompute the snapshot for pipe listener when loading snapshot
    // to recover the newest snapshot in cache
    listen2Snapshot4PipeListener(false);
  }

  public void listen2Snapshot4PipeListener(final boolean isTmp) {
    final Pair<Path, Path> snapshotPaths =
        SchemaRegionSnapshotParser.getSnapshotPaths(
            Utils.fromConsensusGroupIdToRaftGroupId(schemaRegion.getSchemaRegionId())
                .getUuid()
                .toString(),
            isTmp);
    final SchemaRegionListeningQueue listener =
        PipeDataNodeAgent.runtime().schemaListener(schemaRegion.getSchemaRegionId());
    if (Objects.isNull(snapshotPaths) || Objects.isNull(snapshotPaths.getLeft())) {
      if (listener.isOpened()) {
        logger.warn(
            "Schema Region Listening Queue Listen to snapshot failed, the historical data may not be transferred. snapshotPaths:{}",
            snapshotPaths);
      }
      return;
    }
    listener.tryListenToSnapshot(
        snapshotPaths.getLeft().toString(),
        // Transfer tLogSnapshot iff it exists and is non-empty
        Objects.nonNull(snapshotPaths.getRight()) && snapshotPaths.getRight().toFile().length() > 0
            ? snapshotPaths.getRight().toString()
            : null,
        schemaRegion.getDatabaseFullPath());
  }

  @Override
  public TSStatus write(final IConsensusRequest request) {
    try {
      final TSStatus result =
          ((PlanNode) request).accept(new SchemaExecutionVisitor(), schemaRegion);
      if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        PipeDataNodeAgent.runtime()
            .schemaListener(schemaRegion.getSchemaRegionId())
            .tryListenToNode((PlanNode) request);
      }
      return result;
    } catch (final IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public DataSet read(final IConsensusRequest request) {
    final FragmentInstance fragmentInstance;
    try {
      fragmentInstance = getFragmentInstance(request);
    } catch (final IllegalArgumentException e) {
      logger.error(e.getMessage());
      return null;
    }
    logger.debug(
        "SchemaRegionStateMachine[{}]: Execute read plan: FragmentInstance-{}",
        schemaRegion.getSchemaRegionId(),
        fragmentInstance.getId());
    return QUERY_INSTANCE_MANAGER.execSchemaQueryFragmentInstance(fragmentInstance, schemaRegion);
  }
}
