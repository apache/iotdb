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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AliasTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.mpp.rpc.thrift.TAliasTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TAliasTimeSeriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TCreateAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TEnablePhysicalSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TLockAndGetSchemaInfoForAliasReq;
import org.apache.iotdb.mpp.rpc.thrift.TMarkSeriesDisabledReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdatePhysicalAliasRefReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AliasTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AliasTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AliasTimeSeriesProcedure.class);

  private String queryId;
  private PartialPath oldPath;
  private PartialPath newPath;

  private transient ByteBuffer oldPathBytes;
  private transient ByteBuffer newPathBytes;
  private transient String requestMessage;

  // Schema info from phase 1
  private transient TAliasTimeSeriesResp schemaInfoResp;
  private transient boolean isRenamed = false;
  private transient PartialPath physicalPath;

  public AliasTimeSeriesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AliasTimeSeriesProcedure(
      final String queryId,
      final PartialPath oldPath,
      final PartialPath newPath,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.oldPath = oldPath;
    this.newPath = newPath;
    generateRequestBytes();
  }

  private void generateRequestBytes() {
    requestMessage = String.format("Alias %s to %s", oldPath.getFullPath(), newPath.getFullPath());
    final ByteArrayOutputStream oldPathStream = new ByteArrayOutputStream();
    final ByteArrayOutputStream newPathStream = new ByteArrayOutputStream();
    final DataOutputStream oldPathDataStream = new DataOutputStream(oldPathStream);
    final DataOutputStream newPathDataStream = new DataOutputStream(newPathStream);
    try {
      oldPath.serialize(oldPathDataStream);
      newPath.serialize(newPathDataStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    oldPathBytes = ByteBuffer.wrap(oldPathStream.toByteArray());
    newPathBytes = ByteBuffer.wrap(newPathStream.toByteArray());
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AliasTimeSeriesState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case LOCK_AND_GET_SCHEMA_INFO:
          LOGGER.info("Lock and get schema info for alias time series {}", requestMessage);
          lockAndGetSchemaInfo(env);
          setNextState(AliasTimeSeriesState.TRANSFORM_METADATA);
          break;
        case TRANSFORM_METADATA:
          LOGGER.info("Transform metadata for alias time series {}", requestMessage);
          transformMetadata(env);
          setNextState(AliasTimeSeriesState.UNLOCK);
          break;
        case UNLOCK:
          LOGGER.info("Unlock for alias time series {}", requestMessage);
          unlock(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "AliasTimeSeries-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
      final ConfigNodeProcedureEnv env, final PartialPath path) {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(path);
    patternTree.constructTree();
    return env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, false);
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getOrCreateRelatedSchemaRegionGroup(
      final ConfigNodeProcedureEnv env, final PartialPath path) {
    // Step 1: Extract and check/create database for the path
    try {
      final String databaseName = extractDatabaseName(path);
      if (databaseName != null && !databaseName.isEmpty()) {
        final List<String> databases =
            env.getConfigManager().getClusterSchemaManager().getDatabaseNames(false);
        if (!databases.contains(databaseName)) {
          // Database doesn't exist, create it with default settings
          final TDatabaseSchema databaseSchema = new TDatabaseSchema(databaseName);
          ClusterSchemaManager.enrichDatabaseSchemaWithDefaultProperties(databaseSchema);
          final DatabaseSchemaPlan databaseSchemaPlan =
              new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, databaseSchema);
          final TSStatus status = env.getConfigManager().setDatabase(databaseSchemaPlan);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                "Failed to create database {} for path {}: {}",
                databaseName,
                path.getFullPath(),
                status.getMessage());
            return Collections.emptyMap();
          }
          LOGGER.info("Auto-created database {} for path {}", databaseName, path.getFullPath());
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to extract or create database for path {}: {}",
          path.getFullPath(),
          e.getMessage());
      // Continue to try getOrCreateSchemaPartition anyway
    }

    // Step 2: Use getOrCreateSchemaPartition to automatically create SchemaRegion if it doesn't
    // exist
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(path);
    patternTree.constructTree();
    final TSchemaPartitionTableResp resp =
        env.getConfigManager().getOrCreateSchemaPartition(patternTree);
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return Collections.emptyMap();
    }
    // Convert TSchemaPartitionTableResp to Map<TConsensusGroupId, TRegionReplicaSet>
    final List<TRegionReplicaSet> allRegionReplicaSets =
        env.getConfigManager().getPartitionManager().getAllReplicaSets();
    final Set<TConsensusGroupId> groupIdSet =
        resp.getSchemaPartitionTable().values().stream()
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toSet());
    final Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (final TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  /**
   * Extract database name from a PartialPath. For Tree Model, database is typically at level 2
   * (root.sg). We try to construct the database path from the first two nodes.
   */
  private String extractDatabaseName(final PartialPath path) {
    try {
      // For Tree Model, database is typically root.sg (first two nodes)
      // For multi-level databases, it could be root.level1.level2, but we start with root.level1
      final String[] nodes = path.getNodes();
      if (nodes.length >= 2) {
        // Try root.sg as database name
        final PartialPath candidateDatabase = new PartialPath(new String[] {nodes[0], nodes[1]});
        return candidateDatabase.getFullPath();
      }
    } catch (Exception e) {
      LOGGER.debug(
          "Failed to extract database name from path {}: {}", path.getFullPath(), e.getMessage());
    }
    return null;
  }

  private void lockAndGetSchemaInfo(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env, oldPath);
    if (targetSchemaRegionGroup.isEmpty()) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format("Schema region not found for path: %s", oldPath.getFullPath()))));
      return;
    }

    final LockAndGetSchemaInfoTaskExecutor lockTask =
        new LockAndGetSchemaInfoTaskExecutor(
            env,
            targetSchemaRegionGroup,
            (dataNodeLocation, consensusGroupIdList) ->
                new TLockAndGetSchemaInfoForAliasReq(
                        consensusGroupIdList, oldPathBytes, newPathBytes)
                    .setIsGeneratedByPipe(isGeneratedByPipe));
    lockTask.execute();

    // Store schema info from response
    schemaInfoResp = lockTask.getResponse();
    if (schemaInfoResp != null
        && schemaInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      isRenamed = schemaInfoResp.isSetIsRenamed() && schemaInfoResp.isIsRenamed();
      if (schemaInfoResp.isSetPhysicalPath() && schemaInfoResp.getPhysicalPath() != null) {
        physicalPath =
            (PartialPath)
                PathDeserializeUtil.deserialize(ByteBuffer.wrap(schemaInfoResp.getPhysicalPath()));
      }
    }
  }

  private void transformMetadata(final ConfigNodeProcedureEnv env) {
    // Transform metadata based on schema info from phase 1
    final Map<TConsensusGroupId, TRegionReplicaSet> oldPathSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env, oldPath);
    if (oldPathSchemaRegionGroup.isEmpty()) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format("Schema region not found for path: %s", oldPath.getFullPath()))));
      return;
    }

    // Determine scenario based on phase 1 response
    if (!isRenamed) {
      // Scenario A: OldPath is a physical series
      // Step 1: Create alias series (NewPath -> OldPath) in newPath's SchemaRegion
      // Use getOrCreateRelatedSchemaRegionGroup to auto-create SchemaRegion if it doesn't exist
      final Map<TConsensusGroupId, TRegionReplicaSet> newPathSchemaRegionGroup =
          getOrCreateRelatedSchemaRegionGroup(env, newPath);
      if (newPathSchemaRegionGroup.isEmpty()) {
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Failed to get or create schema region for path: %s",
                        newPath.getFullPath()))));
        return;
      }
      final AliasTimeSeriesRegionTaskExecutor<TCreateAliasSeriesReq> createAliasTask =
          new AliasTimeSeriesRegionTaskExecutor<>(
              "create alias series",
              env,
              newPathSchemaRegionGroup,
              CnToDnAsyncRequestType.CREATE_ALIAS_SERIES,
              (dataNodeLocation, consensusGroupIdList) ->
                  new TCreateAliasSeriesReq(
                          consensusGroupIdList,
                          oldPathBytes,
                          newPathBytes,
                          schemaInfoResp.getTimeSeriesInfo())
                      .setIsGeneratedByPipe(isGeneratedByPipe));
      createAliasTask.execute();

      // Step 2: Mark old series as disabled and set ALIAS_PATH in oldPath's SchemaRegion
      final AliasTimeSeriesRegionTaskExecutor<TMarkSeriesDisabledReq> markDisabledTask =
          new AliasTimeSeriesRegionTaskExecutor<>(
              "mark series disabled",
              env,
              oldPathSchemaRegionGroup,
              CnToDnAsyncRequestType.MARK_SERIES_DISABLED,
              (dataNodeLocation, consensusGroupIdList) ->
                  new TMarkSeriesDisabledReq(consensusGroupIdList, oldPathBytes, newPathBytes)
                      .setIsGeneratedByPipe(isGeneratedByPipe));
      markDisabledTask.execute();
    } else {
      // OldPath is an alias series
      // Get physical path's SchemaRegion
      final Map<TConsensusGroupId, TRegionReplicaSet> physicalPathSchemaRegionGroup =
          getRelatedSchemaRegionGroup(env, physicalPath);
      if (physicalPathSchemaRegionGroup.isEmpty()) {
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Schema region not found for path: %s", physicalPath.getFullPath()))));
        return;
      }

      if (newPath.equals(physicalPath)) {
        // Scenario C: NewPath == physicalPath (revert alias)
        // Step 1: Enable physical series (remove DISABLED, clear ALIAS_PATH) in physicalPath's
        // SchemaRegion
        final AliasTimeSeriesRegionTaskExecutor<TEnablePhysicalSeriesReq> enablePhysicalTask =
            new AliasTimeSeriesRegionTaskExecutor<>(
                "enable physical series",
                env,
                physicalPathSchemaRegionGroup,
                CnToDnAsyncRequestType.ENABLE_PHYSICAL_SERIES,
                (dataNodeLocation, consensusGroupIdList) -> {
                  ByteArrayOutputStream physicalPathStream = new ByteArrayOutputStream();
                  DataOutputStream physicalPathDataStream =
                      new DataOutputStream(physicalPathStream);
                  try {
                    physicalPath.serialize(physicalPathDataStream);
                  } catch (IOException ignored) {
                    // ByteArrayOutputStream won't throw IOException
                  }
                  return new TEnablePhysicalSeriesReq(
                          consensusGroupIdList, ByteBuffer.wrap(physicalPathStream.toByteArray()))
                      .setIsGeneratedByPipe(isGeneratedByPipe);
                });
        enablePhysicalTask.execute();

        // Step 2: Drop old alias series in oldPath's SchemaRegion
        final AliasTimeSeriesRegionTaskExecutor<TDropAliasSeriesReq> dropAliasTask =
            new AliasTimeSeriesRegionTaskExecutor<>(
                "drop alias series",
                env,
                oldPathSchemaRegionGroup,
                CnToDnAsyncRequestType.DROP_ALIAS_SERIES,
                (dataNodeLocation, consensusGroupIdList) ->
                    new TDropAliasSeriesReq(consensusGroupIdList, oldPathBytes)
                        .setIsGeneratedByPipe(isGeneratedByPipe));
        dropAliasTask.execute();
      } else {
        // Scenario B: NewPath != physicalPath (update alias)
        // Step 1: Create new alias series (NewPath -> physicalPath) in newPath's SchemaRegion
        // Use getOrCreateRelatedSchemaRegionGroup to auto-create SchemaRegion if it doesn't exist
        final Map<TConsensusGroupId, TRegionReplicaSet> newPathSchemaRegionGroup =
            getOrCreateRelatedSchemaRegionGroup(env, newPath);
        if (newPathSchemaRegionGroup.isEmpty()) {
          setFailure(
              new ProcedureException(
                  new MetadataException(
                      String.format(
                          "Failed to get or create schema region for path: %s",
                          newPath.getFullPath()))));
          return;
        }
        final AliasTimeSeriesRegionTaskExecutor<TCreateAliasSeriesReq> createAliasTask =
            new AliasTimeSeriesRegionTaskExecutor<>(
                "create alias series",
                env,
                newPathSchemaRegionGroup,
                CnToDnAsyncRequestType.CREATE_ALIAS_SERIES,
                (dataNodeLocation, consensusGroupIdList) -> {
                  ByteArrayOutputStream physicalPathStream = new ByteArrayOutputStream();
                  DataOutputStream physicalPathDataStream =
                      new DataOutputStream(physicalPathStream);
                  try {
                    physicalPath.serialize(physicalPathDataStream);
                  } catch (IOException ignored) {
                    // ByteArrayOutputStream won't throw IOException
                  }
                  return new TCreateAliasSeriesReq(
                          consensusGroupIdList,
                          ByteBuffer.wrap(physicalPathStream.toByteArray()),
                          newPathBytes,
                          schemaInfoResp.getTimeSeriesInfo())
                      .setIsGeneratedByPipe(isGeneratedByPipe);
                });
        createAliasTask.execute();

        // Step 2: Update physical series ALIAS_PATH to NewPath in physicalPath's SchemaRegion
        final AliasTimeSeriesRegionTaskExecutor<TUpdatePhysicalAliasRefReq> updateRefTask =
            new AliasTimeSeriesRegionTaskExecutor<>(
                "update physical alias ref",
                env,
                physicalPathSchemaRegionGroup,
                CnToDnAsyncRequestType.UPDATE_PHYSICAL_ALIAS_REF,
                (dataNodeLocation, consensusGroupIdList) -> {
                  ByteArrayOutputStream physicalPathStream = new ByteArrayOutputStream();
                  DataOutputStream physicalPathDataStream =
                      new DataOutputStream(physicalPathStream);
                  try {
                    physicalPath.serialize(physicalPathDataStream);
                  } catch (IOException ignored) {
                    // ByteArrayOutputStream won't throw IOException
                  }
                  return new TUpdatePhysicalAliasRefReq(
                          consensusGroupIdList,
                          ByteBuffer.wrap(physicalPathStream.toByteArray()),
                          newPathBytes)
                      .setIsGeneratedByPipe(isGeneratedByPipe);
                });
        updateRefTask.execute();

        // Step 3: Drop old alias series in oldPath's SchemaRegion
        final AliasTimeSeriesRegionTaskExecutor<TDropAliasSeriesReq> dropAliasTask =
            new AliasTimeSeriesRegionTaskExecutor<>(
                "drop alias series",
                env,
                oldPathSchemaRegionGroup,
                CnToDnAsyncRequestType.DROP_ALIAS_SERIES,
                (dataNodeLocation, consensusGroupIdList) ->
                    new TDropAliasSeriesReq(consensusGroupIdList, oldPathBytes)
                        .setIsGeneratedByPipe(isGeneratedByPipe));
        dropAliasTask.execute();
      }
    }
  }

  private void unlock(final ConfigNodeProcedureEnv env) {
    // Unlock in oldPath's SchemaRegion (where the lock was acquired)
    final Map<TConsensusGroupId, TRegionReplicaSet> oldPathSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env, oldPath);
    if (oldPathSchemaRegionGroup.isEmpty()) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format("Schema region not found for path: %s", oldPath.getFullPath()))));
      return;
    }

    final AliasTimeSeriesRegionTaskExecutor<TAliasTimeSeriesReq> unlockTask =
        new AliasTimeSeriesRegionTaskExecutor<>(
            "unlock",
            env,
            oldPathSchemaRegionGroup,
            CnToDnAsyncRequestType.UNLOCK_FOR_ALIAS,
            (dataNodeLocation, consensusGroupIdList) ->
                new TAliasTimeSeriesReq(consensusGroupIdList, oldPathBytes, newPathBytes)
                    .setIsGeneratedByPipe(isGeneratedByPipe));
    unlockTask.execute();
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final AliasTimeSeriesState aliasTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {
    // TODO: Implement rollback logic
    // Rollback should unlock and restore the original state
    if (aliasTimeSeriesState == AliasTimeSeriesState.LOCK_AND_GET_SCHEMA_INFO) {
      // Rollback: unlock if we locked
      unlock(env);
    } else if (aliasTimeSeriesState == AliasTimeSeriesState.TRANSFORM_METADATA) {
      // Rollback: restore original metadata
      // TODO: Implement rollback of metadata transformation
    }
  }

  @Override
  protected boolean isRollbackSupported(final AliasTimeSeriesState aliasTimeSeriesState) {
    return true;
  }

  @Override
  protected AliasTimeSeriesState getState(final int stateId) {
    return AliasTimeSeriesState.values()[stateId];
  }

  @Override
  protected int getStateId(final AliasTimeSeriesState aliasTimeSeriesState) {
    return aliasTimeSeriesState.ordinal();
  }

  @Override
  protected AliasTimeSeriesState getInitialState() {
    return AliasTimeSeriesState.LOCK_AND_GET_SCHEMA_INFO;
  }

  public String getQueryId() {
    return queryId;
  }

  public PartialPath getOldPath() {
    return oldPath;
  }

  public PartialPath getNewPath() {
    return newPath;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALIAS_TIMESERIES_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    oldPath.serialize(stream);
    newPath.serialize(stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    oldPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    newPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    generateRequestBytes();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AliasTimeSeriesProcedure that = (AliasTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && this.getCurrentState().equals(that.getCurrentState())
        && this.getCycles() == that.getCycles()
        && this.isGeneratedByPipe == that.isGeneratedByPipe
        && Objects.equals(this.oldPath, that.oldPath)
        && Objects.equals(this.newPath, that.newPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, oldPath, newPath);
  }

  /** Task executor for lock and get schema info phase, which returns TAliasTimeSeriesResp. */
  private class LockAndGetSchemaInfoTaskExecutor
      extends DataNodeRegionTaskExecutor<TLockAndGetSchemaInfoForAliasReq, TAliasTimeSeriesResp> {

    private TAliasTimeSeriesResp response;

    LockAndGetSchemaInfoTaskExecutor(
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final BiFunction<
                TDataNodeLocation, List<TConsensusGroupId>, TLockAndGetSchemaInfoForAliasReq>
            dataNodeRequestGenerator) {
      super(
          env,
          targetSchemaRegionGroup,
          false,
          CnToDnAsyncRequestType.LOCK_ALIAS,
          dataNodeRequestGenerator);
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        final TAliasTimeSeriesResp resp) {
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // Store the first successful response
        if (response == null) {
          response = resp;
        }
        return failedRegionList;
      }

      // Handle errors
      if (resp.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = resp.getStatus().getSubStatus();
        for (int i = 0; i < subStatus.size(); i++) {
          if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            failedRegionList.add(consensusGroupIdList.get(i));
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Alias time series %s failed when [lock and get schema info] because failed to execute in all replicaset of %s %s. Failure nodes: %s",
                      requestMessage,
                      consensusGroupId.type,
                      consensusGroupId.id,
                      dataNodeLocationSet))));
      interruptTask();
    }

    public TAliasTimeSeriesResp getResponse() {
      return response;
    }
  }

  private class AliasTimeSeriesRegionTaskExecutor<Q> extends DataNodeTSStatusTaskExecutor<Q> {

    private final String taskName;

    AliasTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final CnToDnAsyncRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetSchemaRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Alias time series %s failed when [%s] because failed to execute in all replicaset of %s %s. Failure nodes: %s",
                      requestMessage,
                      taskName,
                      consensusGroupId.type,
                      consensusGroupId.id,
                      dataNodeLocationSet))));
      interruptTask();
    }
  }
}
