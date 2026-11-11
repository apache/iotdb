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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private String queryId;

  private PathPatternTree patternTree;
  private PathPatternTree originalPatternTree;
  private final List<TDatabaseSchema> deleteDatabaseSchemas = new ArrayList<>();
  private String databaseDeletionFailure;
  private transient ByteBuffer patternTreeBytes;
  private boolean mayDeleteAudit;

  private transient String requestMessage;

  // Do not serialize it for compatibility concerns.
  // If the procedure is restored, add mods to the data regions anyway.
  private transient boolean isAllLogicalView;

  private static final String CONSENSUS_WRITE_ERROR =
      ProcedureMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE;

  public DeleteTimeSeriesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteTimeSeriesProcedure(
      final String queryId,
      final PathPatternTree patternTree,
      final boolean isGeneratedByPipe,
      boolean mayDeleteAudit) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    setPatternTree(patternTree);
    this.mayDeleteAudit = mayDeleteAudit;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DeleteTimeSeriesState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PREPARE_DELETE:
          prepareDelete(env);
          break;
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info(
              ProcedureMessages.CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TIMESERIES, requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            setFailure(
                new ProcedureException(
                    new PathNotExistException(
                        patternTree.getAllPathPatterns().stream()
                            .map(PartialPath::getFullPath)
                            .collect(Collectors.toList()),
                        false)));
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info(ProcedureMessages.INVALIDATE_CACHE_OF_TIMESERIES, requestMessage);
          invalidateCache(env, patternTreeBytes, requestMessage, this::setFailure, true);
          setNextState(DeleteTimeSeriesState.DELETE_DATA);
          break;
        case DELETE_DATA:
          LOGGER.info(ProcedureMessages.DELETE_DATA_OF_TIMESERIES, requestMessage);
          deleteData(env);
          break;
        case DELETE_TIMESERIES_SCHEMA:
          LOGGER.info(ProcedureMessages.DELETE_TIMESERIES_SCHEMAENGINE_OF, requestMessage);
          deleteTimeSeriesSchema(env);
          collectPayload4Pipe(env);
          if (!isFailed() && !deleteDatabaseSchemas.isEmpty()) {
            setNextState(DeleteTimeSeriesState.DELETE_DATABASES);
            break;
          }
          return Flow.NO_MORE_STATE;
        case DELETE_DATABASES:
          // This is a background task: an RPC wait timeout must not allow recreation while a
          // database-deletion procedure is still running.
          final TSStatus deleteStatus =
              env.getConfigManager()
                  .getProcedureManager()
                  .deleteDatabases(
                      deleteDatabaseSchemas, isGeneratedByPipe, Long.MAX_VALUE, queryId);
          if (deleteStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            databaseDeletionFailure = deleteStatus.toString();
          }
          // Persist deletion's result before recreating any database. Recovery must never delete
          // databases again once their schemas have been recreated and accept new writes.
          setNextState(DeleteTimeSeriesState.RECREATE_DATABASES);
          break;
        case RECREATE_DATABASES:
          recreateDatabases(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException(ProcedureMessages.UNRECOGNIZED_STATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.DELETETIMESERIES_COSTS_MS,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  /** Keep the full-database optimization inside the durable asynchronous deletion task. */
  private void prepareDelete(final ConfigNodeProcedureEnv env) {
    final ConfigManager manager = env.getConfigManager();
    final Set<TDatabaseSchema> databases = new HashSet<>();
    final List<PartialPath> databasePatterns = new ArrayList<>();
    final PathPatternTree remaining = new PathPatternTree();
    for (PartialPath path : patternTree.getAllPathPatterns()) {
      if (PathPatternUtil.isMultiLevelMatchWildcard(path.getMeasurement())
          && path.getNodeLength() > 1) {
        // Pattern-tree paths are PartialPaths, whose getDevicePath() includes the final **.
        final PartialPath prefix =
            new PartialPath(Arrays.copyOf(path.getNodes(), path.getNodeLength() - 1));
        if (prefix.hasWildcard()) {
          remaining.appendPathPattern(path);
          continue;
        }
        final Map<String, TDatabaseSchema> matched =
            manager.getClusterSchemaManager().getMatchedDatabaseSchemasByPrefix(prefix);
        matched.remove(TREE_MODEL_AUDIT_DATABASE);
        if (!matched.isEmpty()) {
          databases.addAll(matched.values());
          databasePatterns.add(path);
          continue;
        }
      }
      remaining.appendPathPattern(path);
    }
    if (!databases.isEmpty()) {
      try {
        SchemaUtils.checkSchemaRegionUsingTemplate(manager, databasePatterns);
      } catch (MetadataException e) {
        setFailure(new ProcedureException(e));
        return;
      }
      remaining.constructTree();
      setPatternTree(remaining);
      deleteDatabaseSchemas.addAll(databases);
    }
    setNextState(
        patternTree.isEmpty() && !deleteDatabaseSchemas.isEmpty()
            ? DeleteTimeSeriesState.DELETE_DATABASES
            : DeleteTimeSeriesState.CONSTRUCT_BLACK_LIST);
  }

  private void recreateDatabases(final ConfigNodeProcedureEnv env) {
    final ConfigManager manager = env.getConfigManager();
    final List<TSStatus> failures = new ArrayList<>();
    if (databaseDeletionFailure != null) {
      failures.add(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, databaseDeletionFailure));
    }
    // Recreate the original database schemas even when deletion reports a failure, as before.
    for (TDatabaseSchema database : deleteDatabaseSchemas) {
      final TSStatus status =
          manager
              .getClusterSchemaManager()
              .setDatabase(
                  new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, database),
                  isGeneratedByPipe);
      if (status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
        try {
          if (database.equals(
              manager.getClusterSchemaManager().getDatabaseSchemaByName(database.getName()))) {
            // A previous execution may have recreated this database before the state was stored.
            continue;
          }
        } catch (DatabaseNotExistsException e) {
          failures.add(RpcUtils.getStatus(TSStatusCode.DATABASE_NOT_EXIST, e.getMessage()));
        }
      }
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failures.add(status);
      }
    }
    if (!failures.isEmpty()) {
      setFailure(
          new ProcedureException(new IoTDBException(RpcUtils.squashResponseStatusList(failures))));
    }
  }

  // Return the total num of timeSeries in schemaEngine black list
  private long constructBlackList(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, mayDeleteAudit);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    isAllLogicalView = true;
    final DeleteTimeSeriesRegionTaskExecutor<TConstructSchemaBlackListReq> constructBlackListTask =
        new DeleteTimeSeriesRegionTaskExecutor<TConstructSchemaBlackListReq>(
            ProcedureMessages.MESSAGE_CONSTRUCT_SCHEMA_ENGINE_BLACK_LIST_FCE4CBE0,
            env,
            targetSchemaRegionGroup,
            CnToDnAsyncRequestType.CONSTRUCT_SCHEMA_BLACK_LIST,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TConstructSchemaBlackListReq(consensusGroupIdList, patternTreeBytes))) {
          @Override
          protected List<TConsensusGroupId> processResponseOfOneDataNode(
              final TDataNodeLocation dataNodeLocation,
              final List<TConsensusGroupId> consensusGroupIdList,
              final TSStatus response) {
            if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              isAllLogicalView = false;
            } else if (response.getCode() == TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()) {
              successResult.add(response);
              return Collections.emptyList();
            }
            return processResponseOfOneDataNodeWithSuccessResult(
                dataNodeLocation, consensusGroupIdList, response);
          }
        };
    constructBlackListTask.execute();

    return !isFailed()
        ? constructBlackListTask.getSuccessResult().stream()
            .mapToLong(resp -> Long.parseLong(resp.getMessage()))
            .reduce(Long::sum)
            .orElse(0L)
        : 0;
  }

  public static void invalidateCache(
      final ConfigNodeProcedureEnv env,
      final ByteBuffer patternTreeBytes,
      final String requestMessage,
      final Consumer<ProcedureException> setFailure,
      final boolean needLock) {
    // Proceed once every unreachable DataNode is provably self-fenced (it fails closed on its
    // schema cache and resyncs on recovery, so it cannot serve the to-be-deleted/altered series),
    // instead of hard-failing on the first unreachable DataNode. This runs before the physical
    // delete in the state machine, so the "delete only after PROCEED" ordering holds.
    if (!SchemaUtils.invalidateMatchedSchemaCache(
        env.getConfigManager(), patternTreeBytes, needLock)) {
      // All dataNodes must clear the related schemaEngine cache
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TIMESERIES, requestMessage);
      setFailure.accept(
          new ProcedureException(
              new MetadataException(ProcedureMessages.INVALIDATE_SCHEMAENGINE_CACHE_FAILED)));
    }
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    deleteDataWithRawPathPattern(env);
  }

  private void deleteDataWithRawPathPattern(final ConfigNodeProcedureEnv env) {
    executeDeleteData(env, patternTree);
    if (isFailed()) {
      return;
    }
    setNextState(DeleteTimeSeriesState.DELETE_TIMESERIES_SCHEMA);
  }

  private void executeDeleteData(
      final ConfigNodeProcedureEnv env, final PathPatternTree patternTree) {
    if (isAllLogicalView) {
      return;
    }

    final Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(patternTree, mayDeleteAudit);

    // Target timeSeries has no data
    if (relatedDataRegionGroup.isEmpty()) {
      return;
    }

    final DeleteTimeSeriesRegionTaskExecutor<TDeleteDataForDeleteSchemaReq> deleteDataTask =
        new DeleteTimeSeriesRegionTaskExecutor<>(
            ProcedureMessages.MESSAGE_DELETE_DATA_1896591B,
            env,
            relatedDataRegionGroup,
            false,
            CnToDnAsyncRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteDataForDeleteSchemaReq(
                        new ArrayList<>(consensusGroupIdList),
                        preparePatternTreeBytesData(patternTree))
                    .setIsGeneratedByPipe(isGeneratedByPipe)));
    deleteDataTask.execute();
  }

  private void deleteTimeSeriesSchema(final ConfigNodeProcedureEnv env) {
    final DeleteTimeSeriesRegionTaskExecutor<TDeleteTimeSeriesReq> deleteTimeSeriesTask =
        new DeleteTimeSeriesRegionTaskExecutor<>(
            ProcedureMessages.MESSAGE_DELETE_TIME_SERIES_IN_SCHEMA_ENGINE_9B3DA44A,
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, mayDeleteAudit),
            CnToDnAsyncRequestType.DELETE_TIMESERIES,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteTimeSeriesReq(consensusGroupIdList, patternTreeBytes)
                    .setIsGeneratedByPipe(isGeneratedByPipe)));
    deleteTimeSeriesTask.execute();
  }

  private void collectPayload4Pipe(final ConfigNodeProcedureEnv env) {
    TSStatus result;
    try {
      result =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(new PipeDeleteTimeSeriesPlan(patternTreeBytes))
                      : new PipeDeleteTimeSeriesPlan(patternTreeBytes));
    } catch (final ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      result = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      result.setMessage(e.getMessage());
    }
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(result.getMessage());
    }
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final DeleteTimeSeriesState deleteTimeSeriesState)
      throws IOException, InterruptedException, ProcedureException {
    if (deleteTimeSeriesState == DeleteTimeSeriesState.CONSTRUCT_BLACK_LIST) {
      final DeleteTimeSeriesRegionTaskExecutor<TRollbackSchemaBlackListReq> rollbackStateTask =
          new DeleteTimeSeriesRegionTaskExecutor<>(
              ProcedureMessages.MESSAGE_ROLL_BACK_SCHEMA_ENGINE_BLACK_LIST_39BA8E03,
              env,
              env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
              CnToDnAsyncRequestType.ROLLBACK_SCHEMA_BLACK_LIST,
              (dataNodeLocation, consensusGroupIdList) ->
                  new TRollbackSchemaBlackListReq(consensusGroupIdList, patternTreeBytes));
      rollbackStateTask.execute();
    }
  }

  @Override
  protected boolean isRollbackSupported(final DeleteTimeSeriesState deleteTimeSeriesState) {
    return true;
  }

  @Override
  protected DeleteTimeSeriesState getState(final int stateId) {
    return DeleteTimeSeriesState.values()[stateId];
  }

  @Override
  protected int getStateId(final DeleteTimeSeriesState deleteTimeSeriesState) {
    return deleteTimeSeriesState.ordinal();
  }

  @Override
  protected DeleteTimeSeriesState getInitialState() {
    return DeleteTimeSeriesState.PREPARE_DELETE;
  }

  public String getQueryId() {
    return queryId;
  }

  public PathPatternTree getPatternTree() {
    return originalPatternTree == null ? patternTree : originalPatternTree;
  }

  public boolean deletesDatabase(final String database) {
    // Table and tree databases have separate namespaces, even with the same qualified path.
    if (PathUtils.isTableModelDatabase(database)) {
      return false;
    }
    final PartialPath databasePath;
    try {
      databasePath = PartialPath.getQualifiedDatabasePartialPath(database);
    } catch (IllegalPathException e) {
      return false;
    }
    final String[] databaseNodes = databasePath.getNodes();
    for (PartialPath path : getPatternTree().getAllPathPatterns()) {
      if (!PathPatternUtil.isMultiLevelMatchWildcard(path.getMeasurement())
          || path.getNodeLength() <= 1) {
        continue;
      }
      final PartialPath prefix =
          new PartialPath(Arrays.copyOf(path.getNodes(), path.getNodeLength() - 1));
      if (!prefix.hasWildcard()
          && prefix.getNodeLength() <= databaseNodes.length
          && Arrays.equals(
              prefix.getNodes(), Arrays.copyOf(databaseNodes, prefix.getNodeLength()))) {
        return true;
      }
    }
    return false;
  }

  public String getDeleteProgress() {
    final DeleteTimeSeriesState state = getCurrentState();
    return isFinished() || state == null ? getState().name() : state.name();
  }

  public void setPatternTree(final PathPatternTree patternTree) {
    if (originalPatternTree == null) {
      originalPatternTree = patternTree;
    }
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    patternTreeBytes = preparePatternTreeBytesData(patternTree);
  }

  public static ByteBuffer preparePatternTreeBytesData(final PathPatternTree patternTree) {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DELETE_TIMESERIES_PROCEDURE.getTypeCode()
            : ProcedureType.DELETE_TIMESERIES_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    patternTree.serialize(stream);
    ReadWriteIOUtils.write(mayDeleteAudit, stream);
    ReadWriteIOUtils.write(originalPatternTree != patternTree, stream);
    if (originalPatternTree != patternTree) {
      originalPatternTree.serialize(stream);
    }
    ReadWriteIOUtils.write(deleteDatabaseSchemas.size(), stream);
    for (TDatabaseSchema database : deleteDatabaseSchemas) {
      ThriftConfigNodeSerDeUtils.serializeTDatabaseSchema(database, stream);
    }
    ReadWriteIOUtils.write(databaseDeletionFailure, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    originalPatternTree = null;
    deleteDatabaseSchemas.clear();
    databaseDeletionFailure = null;
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
    if (getCurrentState() == DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE
        || getCurrentState() == DeleteTimeSeriesState.DELETE_DATA) {
      LOGGER.info(ProcedureMessages.SUCCESSFULLY_RESTORED_WILL_SET_MODS_TO_THE_DATA_REGIONS_ANYWAY);
    }
    if (byteBuffer.hasRemaining()) {
      mayDeleteAudit = ReadWriteIOUtils.readBoolean(byteBuffer);
    }
    if (byteBuffer.hasRemaining()) {
      originalPatternTree =
          ReadWriteIOUtils.readBoolean(byteBuffer)
              ? PathPatternTree.deserialize(byteBuffer)
              : patternTree;
      final int size = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < size; i++) {
        deleteDatabaseSchemas.add(
            ThriftConfigNodeSerDeUtils.deserializeTDatabaseSchema(byteBuffer));
      }
      if (byteBuffer.hasRemaining()) {
        databaseDeletionFailure = ReadWriteIOUtils.readString(byteBuffer);
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DeleteTimeSeriesProcedure that = (DeleteTimeSeriesProcedure) o;
    return this.getProcId() == that.getProcId()
        && Objects.equals(this.getCurrentState(), that.getCurrentState())
        && this.getCycles() == getCycles()
        && this.isGeneratedByPipe == that.isGeneratedByPipe
        && this.patternTree.equals(that.patternTree)
        && Objects.equals(originalPatternTree, that.originalPatternTree)
        && deleteDatabaseSchemas.equals(that.deleteDatabaseSchemas)
        && Objects.equals(databaseDeletionFailure, that.databaseDeletionFailure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        patternTree,
        originalPatternTree,
        deleteDatabaseSchemas,
        databaseDeletionFailure);
  }

  private class DeleteTimeSeriesRegionTaskExecutor<Q> extends DataNodeTSStatusTaskExecutor<Q> {

    private final String taskName;

    DeleteTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final CnToDnAsyncRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetSchemaRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    DeleteTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetDataRegionGroup,
        final boolean executeOnAllReplicaset,
        final CnToDnAsyncRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(
          env,
          targetDataRegionGroup,
          executeOnAllReplicaset,
          dataNodeRequestType,
          dataNodeRequestGenerator);
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
                      ProcedureMessages.DELETE_TIME_SERIES_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN,
                      requestMessage,
                      taskName,
                      consensusGroupId.type,
                      consensusGroupId.id,
                      printFailureMap()))));
      interruptTask();
    }
  }
}
