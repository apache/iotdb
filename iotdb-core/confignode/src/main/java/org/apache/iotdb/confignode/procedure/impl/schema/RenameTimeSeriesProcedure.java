/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeRenameTimeSeriesPlan;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.MetadataProcedureConflictCheckable;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TCreateAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropAliasSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TLockAndGetSchemaInfoForAliasReq;
import org.apache.iotdb.mpp.rpc.thrift.TMarkSeriesEnabledReq;
import org.apache.iotdb.mpp.rpc.thrift.TMarkSeriesInvalidReq;
import org.apache.iotdb.mpp.rpc.thrift.TRenameTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TRenameTimeSeriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TUpdatePhysicalAliasRefReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Industrial-grade RenameTimeSeriesProcedure with "Register-Before-Execute" Rollback Pattern. */
public class RenameTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, RenameTimeSeriesState>
    implements MetadataProcedureConflictCheckable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RenameTimeSeriesProcedure.class);

  // Persisted Fields
  private String queryId;
  private PartialPath oldPath;
  private PartialPath newPath;

  // Transient: Request context
  private transient ByteBuffer oldPathBytes;
  private transient ByteBuffer newPathBytes;
  private transient String requestMessage;

  // Persisted execution context for resuming transform and rollback after restart
  private TRenameTimeSeriesResp analysisContext;

  // Persisted rollback actions. The newest action is at the front of the deque.
  private final Deque<RollbackAction> rollbackActions = new ArrayDeque<>();

  protected enum RollbackAction {
    ROLLBACK_CREATE_ALIAS,
    ROLLBACK_MARK_SERIES_INVALID,
    ROLLBACK_MARK_SERIES_ENABLED,
    ROLLBACK_DROP_ALIAS_SERIES,
    ROLLBACK_UPDATE_PHYSICAL_ALIAS_REF
  }

  public RenameTimeSeriesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public RenameTimeSeriesProcedure(
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
    this.requestMessage =
        String.format("Rename %s to %s", oldPath.getFullPath(), newPath.getFullPath());
    this.oldPathBytes = oldPath.serialize();
    this.newPathBytes = newPath.serialize();
  }

  protected final void setAnalysisContext(final TRenameTimeSeriesResp analysisContext) {
    this.analysisContext = analysisContext;
  }

  protected final TRenameTimeSeriesResp getAnalysisContext() {
    return analysisContext;
  }

  protected final void registerRollbackAction(final RollbackAction rollbackAction) {
    rollbackActions.push(rollbackAction);
  }

  protected final List<RollbackAction> getRollbackActionsSnapshot() {
    return new ArrayList<>(rollbackActions);
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final RenameTimeSeriesState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case LOCK_AND_GET_SCHEMA_INFO:
          LOGGER.info("Phase 1: Validate and get schema info for {}", requestMessage);
          fetchAndValidateSchemaInfo(env);

          if (isFailed()) {
            return Flow.NO_MORE_STATE;
          }
          if (Objects.equals(oldPath, newPath)) {
            LOGGER.info("Skip rename as paths are identical: {}", requestMessage);
            return Flow.NO_MORE_STATE;
          }
          setNextState(RenameTimeSeriesState.TRANSFORM_METADATA);
          break;

        case TRANSFORM_METADATA:
          LOGGER.info("Phase 2: Transform metadata for {}", requestMessage);

          transformMetadata(env);

          if (isFailed()) {
            // Execution failed, framework triggers rollbackState.
            // rollbackActions already contains the rollback op for the failed step (and previous
            // steps).
            return Flow.NO_MORE_STATE;
          }

          invalidateCache(env);
          setNextState(RenameTimeSeriesState.UNLOCK);
          break;

        case UNLOCK:
          LOGGER.info("Phase 3: Unlock for {}", requestMessage);
          unlock(env);
          collectPayload4Pipe(env);
          return Flow.NO_MORE_STATE;

        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "RenameTimeSeries-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  // ==================== Context Management ====================

  private void fetchAndValidateSchemaInfo(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        getRelatedSchemaRegionGroup(env, oldPath);

    if (targetSchemaRegionGroup.isEmpty()) {
      setFailure(
          new ProcedureException(
              new MetadataException("Schema region not found for path: " + oldPath.getFullPath())));
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

    if (isFailed()) {
      return;
    }

    TRenameTimeSeriesResp resp = lockTask.getResponse();
    if (resp == null || resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      handleLockFailure(resp);
    } else {
      setAnalysisContext(resp);
    }
  }

  private void handleLockFailure(TRenameTimeSeriesResp resp) {
    TSStatus status = resp != null ? resp.getStatus() : null;
    int code =
        status != null ? status.getCode() : TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode();
    String msg = status != null ? status.getMessage() : "Unknown error during lock";

    if (code == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      msg = String.format("Path %s does not exist", oldPath.getFullPath());
    } else if (isConstraintError(code)) {
      msg = String.format("Cannot rename %s: %s", oldPath.getFullPath(), msg);
    }
    setFailure(new ProcedureException(new MetadataException(msg, code)));
  }

  private boolean isConstraintError(int code) {
    return code == TSStatusCode.TEMPLATE_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode()
        || code == TSStatusCode.VIEW_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode()
        || code == TSStatusCode.INVALID_TIMESERIES_CANNOT_CREATE_ALIAS.getStatusCode()
        || code == TSStatusCode.TIMESERIES_ALREADY_RENAMING.getStatusCode();
  }

  // ==================== Core Logic: Transformation (Updated Pattern) ====================

  private void transformMetadata(final ConfigNodeProcedureEnv env) {
    if (analysisContext == null) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  "Missing schema info when transforming rename metadata for "
                      + oldPath.getFullPath())));
      return;
    }

    boolean isRenamed = analysisContext.isIsRenamed();
    PartialPath physicalPath = null;
    if (analysisContext.isSetPhysicalPath()) {
      physicalPath =
          (PartialPath)
              PathDeserializeUtil.deserialize(ByteBuffer.wrap(analysisContext.getPhysicalPath()));
    }

    if (!isRenamed) {
      processPhysicalToAlias(env);
    } else {
      if (newPath.equals(physicalPath)) {
        processAliasToPhysical(env, physicalPath);
      } else {
        processAliasToAlias(env, physicalPath);
      }
    }
  }

  /** Scenario A: Physical Path -> Alias Path. */
  private void processPhysicalToAlias(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> newRegion =
        getOrCreateRelatedSchemaRegionGroup(env, newPath);
    if (checkAndSetFailure(newRegion, newPath)) {
      return;
    }

    Map<TConsensusGroupId, TRegionReplicaSet> oldRegion = getRelatedSchemaRegionGroup(env, oldPath);
    if (checkAndSetFailure(oldRegion, oldPath)) {
      return;
    }

    // --- Step 1: Create Alias ---
    registerRollbackAction(RollbackAction.ROLLBACK_CREATE_ALIAS);

    executeRegionTask(
        "create alias",
        env,
        newRegion,
        CnToDnAsyncRequestType.CREATE_ALIAS_SERIES,
        (loc, ids) ->
            new TCreateAliasSeriesReq(
                    ids, oldPathBytes, newPathBytes, analysisContext.getTimeSeriesInfo())
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
    if (isFailed()) {
      return;
    }

    // --- Step 2: Mark Disabled ---
    registerRollbackAction(RollbackAction.ROLLBACK_MARK_SERIES_INVALID);
    executeRegionTask(
        "mark disabled",
        env,
        oldRegion,
        CnToDnAsyncRequestType.MARK_SERIES_INVALID,
        (loc, ids) ->
            new TMarkSeriesInvalidReq(ids, oldPathBytes, newPathBytes)
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
  }

  /**
   * Scenario C: Alias -> Physical.
   *
   * <p>Processes the rename operation from an alias path to its physical path. This scenario occurs
   * when renaming an alias back to its original physical path.
   *
   * <p>Execution steps:
   *
   * <ol>
   *   <li>Mark the series as enabled in the physical region, restoring it from disabled state
   *   <li>Drop the alias from the alias region, removing the alias mapping
   * </ol>
   *
   * <p>Each step is registered with rollback operations before execution to ensure atomicity.
   *
   * @param env the procedure environment
   * @param physicalPath the physical path that the alias points to
   */
  private void processAliasToPhysical(ConfigNodeProcedureEnv env, PartialPath physicalPath) {
    if (physicalPath == null) {
      setFailure(
          new ProcedureException(
              new MetadataException("Physical path is null when processing alias to physical")));
      return;
    }
    Map<TConsensusGroupId, TRegionReplicaSet> physRegion =
        getRelatedSchemaRegionGroup(env, physicalPath);
    if (checkAndSetFailure(physRegion, physicalPath)) {
      return;
    }

    Map<TConsensusGroupId, TRegionReplicaSet> aliasRegion =
        getRelatedSchemaRegionGroup(env, oldPath);
    if (checkAndSetFailure(aliasRegion, oldPath)) {
      return;
    }

    ByteBuffer physBytes = physicalPath.serialize();

    // --- Step 1: Mark Series Enabled ---
    registerRollbackAction(RollbackAction.ROLLBACK_MARK_SERIES_ENABLED);

    executeRegionTask(
        "mark series enabled",
        env,
        physRegion,
        CnToDnAsyncRequestType.MARK_SERIES_ENABLED,
        (loc, ids) ->
            new TMarkSeriesEnabledReq(ids, physBytes)
                .setAliasPath(oldPathBytes)
                .setTimeSeriesInfo(analysisContext.getTimeSeriesInfo())
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
    if (isFailed()) {
      return;
    }

    // --- Step 2: Drop Alias ---
    // Register Rollback 2 BEFORE Execute: Re-create the alias we are about to drop
    registerRollbackAction(RollbackAction.ROLLBACK_DROP_ALIAS_SERIES);

    // Execute 2: Drop Alias
    executeRegionTask(
        "drop alias",
        env,
        aliasRegion,
        CnToDnAsyncRequestType.DROP_ALIAS_SERIES,
        (loc, ids) ->
            new TDropAliasSeriesReq(ids, oldPathBytes)
                .setPhysicalPath(physBytes)
                .setTimeSeriesInfo(analysisContext.getTimeSeriesInfo())
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
  }

  /** Scenario B: Alias -> Alias. */
  private void processAliasToAlias(ConfigNodeProcedureEnv env, PartialPath physicalPath) {
    if (physicalPath == null) {
      setFailure(
          new ProcedureException(
              new MetadataException("Physical path is null when processing alias to alias")));
      return;
    }
    Map<TConsensusGroupId, TRegionReplicaSet> newRegion =
        getOrCreateRelatedSchemaRegionGroup(env, newPath);
    if (checkAndSetFailure(newRegion, newPath)) {
      return;
    }

    Map<TConsensusGroupId, TRegionReplicaSet> physRegion =
        getRelatedSchemaRegionGroup(env, physicalPath);
    if (checkAndSetFailure(physRegion, physicalPath)) {
      return;
    }

    Map<TConsensusGroupId, TRegionReplicaSet> oldRegion = getRelatedSchemaRegionGroup(env, oldPath);
    if (checkAndSetFailure(oldRegion, oldPath)) {
      return;
    }

    ByteBuffer physBytes = physicalPath.serialize();

    // --- Step 1: Create New Alias ---
    registerRollbackAction(RollbackAction.ROLLBACK_CREATE_ALIAS);
    executeRegionTask(
        "create new alias",
        env,
        newRegion,
        CnToDnAsyncRequestType.CREATE_ALIAS_SERIES,
        (loc, ids) ->
            new TCreateAliasSeriesReq(
                    ids, physBytes, newPathBytes, analysisContext.getTimeSeriesInfo())
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
    if (isFailed()) {
      return;
    }

    // --- Step 2: Update Ref ---
    // Register Rollback 2 BEFORE Execute: Revert ref to old alias
    registerRollbackAction(RollbackAction.ROLLBACK_UPDATE_PHYSICAL_ALIAS_REF);

    // Execute 2: Update Ref
    executeRegionTask(
        "update physical ref",
        env,
        physRegion,
        CnToDnAsyncRequestType.UPDATE_PHYSICAL_ALIAS_REF,
        (loc, ids) ->
            new TUpdatePhysicalAliasRefReq(ids, physBytes, newPathBytes)
                .setOldAliasPath(oldPathBytes)
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
    if (isFailed()) {
      return;
    }

    // --- Step 3: Drop Old Alias ---
    registerRollbackAction(RollbackAction.ROLLBACK_DROP_ALIAS_SERIES);

    executeRegionTask(
        "drop old alias",
        env,
        oldRegion,
        CnToDnAsyncRequestType.DROP_ALIAS_SERIES,
        (loc, ids) ->
            new TDropAliasSeriesReq(ids, oldPathBytes)
                .setPhysicalPath(physBytes)
                .setTimeSeriesInfo(analysisContext.getTimeSeriesInfo())
                .setIsGeneratedByPipe(isGeneratedByPipe)
                .setIsRollback(false));
  }

  // ==================== Hybrid Rollback Logic ====================

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final RenameTimeSeriesState state)
      throws IOException, InterruptedException, ProcedureException {

    LOGGER.info("Rolling back rename time series {} from state {}", requestMessage, state);

    // Runtime failure: execute fine-grained rollback actions in LIFO order.
    // Because we register BEFORE execute, the top action matches the current
    // (potentially failed) step, or the last successful step. Each action is
    // removed before execution to avoid retrying the same failing action forever.
    if (state == RenameTimeSeriesState.TRANSFORM_METADATA && !rollbackActions.isEmpty()) {
      LOGGER.info("Executing precise stack-based rollback for {}", requestMessage);
      while (!rollbackActions.isEmpty()) {
        final RollbackAction rollbackAction = rollbackActions.pop();
        try {
          executeRollbackAction(env, rollbackAction);
        } catch (Exception e) {
          LOGGER.error(
              "Failed to execute rollback step {} for {}", rollbackAction, requestMessage, e);
        }
      }
    }

    unlock(env);
  }

  // ==================== Utilities & Boilerplate ====================

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    SchemaUtils.invalidateCache(env, true, oldPath, newPath);
  }

  protected void unlock(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> group =
        getRelatedSchemaRegionGroup(env, oldPath);
    if (!group.isEmpty()) {
      final RenameTimeSeriesRegionTaskExecutor<TRenameTimeSeriesReq> unlockTask =
          new RenameTimeSeriesRegionTaskExecutor<>(
              "unlock",
              env,
              group,
              CnToDnAsyncRequestType.UNLOCK_FOR_ALIAS,
              (loc, ids) ->
                  new TRenameTimeSeriesReq(ids, oldPathBytes, newPathBytes)
                      .setIsGeneratedByPipe(isGeneratedByPipe));
      unlockTask.execute();
    }
  }

  private void collectPayload4Pipe(final ConfigNodeProcedureEnv env) {
    try {
      PipeRenameTimeSeriesPlan plan = new PipeRenameTimeSeriesPlan(oldPathBytes, newPathBytes);
      TSStatus result =
          env.getConfigManager()
              .getConsensusManager()
              .write(isGeneratedByPipe ? new PipeEnrichedPlan(plan) : plan);
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(result.getMessage());
      }
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Failed to write pipe rename time series plan to consensus layer for {}: {}",
          requestMessage,
          e.getMessage(),
          e);
      throw new PipeException(
          String.format(
              "Failed to write pipe rename time series plan to consensus layer for %s: %s",
              requestMessage, e.getMessage()),
          e);
    }
  }

  protected <Q> void executeRegionTask(
      String taskName,
      ConfigNodeProcedureEnv env,
      Map<TConsensusGroupId, TRegionReplicaSet> regionGroup,
      CnToDnAsyncRequestType type,
      BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> requestGen) {

    if (regionGroup == null || regionGroup.isEmpty()) {
      return;
    }

    RenameTimeSeriesRegionTaskExecutor<Q> task =
        new RenameTimeSeriesRegionTaskExecutor<>(taskName, env, regionGroup, type, requestGen);
    task.execute();
  }

  private void executeRollbackAction(
      final ConfigNodeProcedureEnv env, final RollbackAction rollbackAction) {
    if (analysisContext == null) {
      throw new IllegalStateException("Missing rollback context for " + requestMessage);
    }

    final PartialPath physicalPath = getPhysicalPathFromAnalysisContext();
    switch (rollbackAction) {
      case ROLLBACK_CREATE_ALIAS:
        final PartialPath sourcePath = getRollbackCreateAliasSourcePath(physicalPath);
        final ByteBuffer sourcePathBytes = sourcePath.serialize();
        executeRegionTask(
            "rollback: create alias",
            env,
            getRelatedSchemaRegionGroup(env, newPath),
            CnToDnAsyncRequestType.CREATE_ALIAS_SERIES,
            (loc, ids) ->
                new TCreateAliasSeriesReq(
                        ids, sourcePathBytes, newPathBytes, analysisContext.getTimeSeriesInfo())
                    .setIsGeneratedByPipe(isGeneratedByPipe)
                    .setIsRollback(true));
        return;
      case ROLLBACK_MARK_SERIES_INVALID:
        executeRegionTask(
            "rollback: mark disabled",
            env,
            getRelatedSchemaRegionGroup(env, oldPath),
            CnToDnAsyncRequestType.MARK_SERIES_INVALID,
            (loc, ids) ->
                new TMarkSeriesInvalidReq(ids, oldPathBytes, newPathBytes)
                    .setIsGeneratedByPipe(isGeneratedByPipe)
                    .setIsRollback(true));
        return;
      case ROLLBACK_MARK_SERIES_ENABLED:
        if (physicalPath == null) {
          throw new IllegalStateException("Missing physical path for rollback mark enabled");
        }
        final ByteBuffer physicalPathBytesForEnable = physicalPath.serialize();
        executeRegionTask(
            "rollback: mark series enabled",
            env,
            getRelatedSchemaRegionGroup(env, physicalPath),
            CnToDnAsyncRequestType.MARK_SERIES_ENABLED,
            (loc, ids) ->
                new TMarkSeriesEnabledReq(ids, physicalPathBytesForEnable)
                    .setAliasPath(oldPathBytes)
                    .setTimeSeriesInfo(analysisContext.getTimeSeriesInfo())
                    .setIsGeneratedByPipe(isGeneratedByPipe)
                    .setIsRollback(true));
        return;
      case ROLLBACK_DROP_ALIAS_SERIES:
        if (physicalPath == null) {
          throw new IllegalStateException("Missing physical path for rollback drop alias");
        }
        final ByteBuffer physicalPathBytesForDrop = physicalPath.serialize();
        executeRegionTask(
            "rollback: drop alias",
            env,
            getRelatedSchemaRegionGroup(env, oldPath),
            CnToDnAsyncRequestType.DROP_ALIAS_SERIES,
            (loc, ids) ->
                new TDropAliasSeriesReq(ids, oldPathBytes)
                    .setPhysicalPath(physicalPathBytesForDrop)
                    .setTimeSeriesInfo(analysisContext.getTimeSeriesInfo())
                    .setIsGeneratedByPipe(isGeneratedByPipe)
                    .setIsRollback(true));
        return;
      case ROLLBACK_UPDATE_PHYSICAL_ALIAS_REF:
        if (physicalPath == null) {
          throw new IllegalStateException("Missing physical path for rollback update alias ref");
        }
        final ByteBuffer physicalPathBytesForRef = physicalPath.serialize();
        executeRegionTask(
            "rollback: update physical ref",
            env,
            getRelatedSchemaRegionGroup(env, physicalPath),
            CnToDnAsyncRequestType.UPDATE_PHYSICAL_ALIAS_REF,
            (loc, ids) ->
                new TUpdatePhysicalAliasRefReq(ids, physicalPathBytesForRef, newPathBytes)
                    .setOldAliasPath(oldPathBytes)
                    .setIsGeneratedByPipe(isGeneratedByPipe)
                    .setIsRollback(true));
        return;
      default:
        throw new IllegalArgumentException("Unknown rollback action: " + rollbackAction);
    }
  }

  private PartialPath getPhysicalPathFromAnalysisContext() {
    if (analysisContext == null || !analysisContext.isSetPhysicalPath()) {
      return null;
    }
    return (PartialPath)
        PathDeserializeUtil.deserialize(ByteBuffer.wrap(analysisContext.getPhysicalPath()));
  }

  private PartialPath getRollbackCreateAliasSourcePath(final PartialPath physicalPath) {
    if (analysisContext != null
        && analysisContext.isIsRenamed()
        && physicalPath != null
        && !newPath.equals(physicalPath)) {
      return physicalPath;
    }
    return oldPath;
  }

  // ==================== Serialization ====================

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_RENAME_TIMESERIES_PROCEDURE.getTypeCode()
            : ProcedureType.RENAME_TIMESERIES_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    oldPath.serialize(stream);
    newPath.serialize(stream);
    serializeAnalysisContext(stream);
    serializeRollbackActions(stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    oldPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    newPath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    generateRequestBytes();

    analysisContext = null;
    rollbackActions.clear();
    if (!byteBuffer.hasRemaining()) {
      return;
    }

    analysisContext = deserializeAnalysisContext(byteBuffer);
    if (!byteBuffer.hasRemaining()) {
      return;
    }
    deserializeRollbackActions(byteBuffer);
  }

  private void serializeAnalysisContext(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(analysisContext != null, stream);
    if (analysisContext == null) {
      return;
    }

    try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      analysisContext.write(new TBinaryProtocol(new TIOStreamTransport(byteArrayOutputStream)));
      final byte[] bytes = byteArrayOutputStream.toByteArray();
      ReadWriteIOUtils.write(bytes.length, stream);
      stream.write(bytes);
    } catch (TException e) {
      throw new IOException("Failed to serialize rename analysis context", e);
    }
  }

  private TRenameTimeSeriesResp deserializeAnalysisContext(final ByteBuffer byteBuffer) {
    if (!ReadWriteIOUtils.readBool(byteBuffer)) {
      return null;
    }

    final int length = ReadWriteIOUtils.readInt(byteBuffer);
    final byte[] bytes = new byte[length];
    byteBuffer.get(bytes);

    final TRenameTimeSeriesResp resp = new TRenameTimeSeriesResp();
    try {
      resp.read(new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(bytes))));
      return resp;
    } catch (TException e) {
      throw new IllegalArgumentException("Failed to deserialize rename analysis context", e);
    }
  }

  private void serializeRollbackActions(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(rollbackActions.size(), stream);
    for (final RollbackAction rollbackAction : rollbackActions) {
      ReadWriteIOUtils.write(rollbackAction.ordinal(), stream);
    }
  }

  private void deserializeRollbackActions(final ByteBuffer byteBuffer) {
    final int rollbackActionSize = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < rollbackActionSize; i++) {
      rollbackActions.addLast(RollbackAction.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    }
  }

  protected Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
      final ConfigNodeProcedureEnv env, final PartialPath path) {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(path);
    patternTree.constructTree();
    return env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, false);
  }

  protected Map<TConsensusGroupId, TRegionReplicaSet> getOrCreateRelatedSchemaRegionGroup(
      final ConfigNodeProcedureEnv env, final PartialPath path) {
    // Step 1: Find or create database for the path
    try {
      // First, try to find matching database from existing databases
      final List<String> databases =
          env.getConfigManager().getClusterSchemaManager().getDatabaseNames(false);
      IDeviceID deviceID = path.getIDeviceIDAsFullDevice();
      String matchedDatabase = null;

      // Search for matching database
      for (final String database : databases) {
        if (PathUtils.isStartWith(deviceID, database)) {
          matchedDatabase = database;
          break;
        }
      }

      // If no matching database found, extract database name and create it
      if (matchedDatabase == null) {
        final PartialPath databaseName =
            MetaUtils.getDatabasePathByLevel(
                path, IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel());
        // Database doesn't exist, create it with default settings
        final TDatabaseSchema databaseSchema = new TDatabaseSchema(databaseName.toString());
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
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to find or create database for path {}: {}", path.getFullPath(), e.getMessage());
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

  private boolean checkAndSetFailure(
      Map<TConsensusGroupId, TRegionReplicaSet> map, PartialPath path) {
    if (map == null || map.isEmpty()) {
      setFailure(
          new ProcedureException(
              new MetadataException("No schema region for " + path.getFullPath())));
      return true;
    }
    return false;
  }

  // ==================== Inner Classes (Executors) ====================

  private class LockAndGetSchemaInfoTaskExecutor
      extends DataNodeRegionTaskExecutor<TLockAndGetSchemaInfoForAliasReq, TRenameTimeSeriesResp> {

    private TRenameTimeSeriesResp response;

    LockAndGetSchemaInfoTaskExecutor(
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final BiFunction<
                TDataNodeLocation, List<TConsensusGroupId>, TLockAndGetSchemaInfoForAliasReq>
            gen) {
      super(env, targetSchemaRegionGroup, false, CnToDnAsyncRequestType.LOCK_ALIAS, gen);
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation loc,
        final List<TConsensusGroupId> ids,
        final TRenameTimeSeriesResp resp) {
      if (response == null) {
        response = resp;
      }
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return Collections.emptyList();
      }
      return ids;
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      interruptTask();
    }

    public TRenameTimeSeriesResp getResponse() {
      return response;
    }
  }

  private class RenameTimeSeriesRegionTaskExecutor<Q> extends DataNodeTSStatusTaskExecutor<Q> {
    private final String taskName;
    private TSStatus response;

    RenameTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final CnToDnAsyncRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetSchemaRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        final TSStatus response) {
      List<TConsensusGroupId> list =
          super.processResponseOfOneDataNode(dataNodeLocation, consensusGroupIdList, response);
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && this.response == null) {
        this.response = response;
        return list;
      }
      this.response = response;
      return list;
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      if (this.response != null) {
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format("Rename failed at [%s]: %s", taskName, response.message))));
      } else {
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Rename failed at [%s] on group %s", taskName, consensusGroupId))));
      }
      interruptTask();
    }
  }

  @Override
  protected boolean isRollbackSupported(final RenameTimeSeriesState state) {
    return true;
  }

  @Override
  protected RenameTimeSeriesState getState(final int stateId) {
    return RenameTimeSeriesState.values()[stateId];
  }

  @Override
  protected int getStateId(final RenameTimeSeriesState state) {
    return state.ordinal();
  }

  @Override
  protected RenameTimeSeriesState getInitialState() {
    return RenameTimeSeriesState.LOCK_AND_GET_SCHEMA_INFO;
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
  public void applyPathPatterns(PathPatternTree patternTree) {
    if (oldPath != null) {
      patternTree.appendPathPattern(oldPath);
    }
    if (newPath != null) {
      patternTree.appendPathPattern(newPath);
    }
  }

  @Override
  public boolean shouldCheckConflict() {
    return !isFinished();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RenameTimeSeriesProcedure that = (RenameTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && Objects.equals(this.oldPath, that.oldPath)
        && Objects.equals(this.newPath, that.newPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), oldPath, newPath);
  }
}
