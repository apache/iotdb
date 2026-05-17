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
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.MetadataProcedureConflictCheckable;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TAliasSeriesInfo;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListResp;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState>
    implements MetadataProcedureConflictCheckable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private String queryId;

  private PathPatternTree patternTree;
  private transient ByteBuffer patternTreeBytes;
  private boolean mayDeleteAudit;
  private final AtomicBoolean isAllInvalidSeries = new AtomicBoolean(true);
  private final AtomicBoolean hasInvalidSeries = new AtomicBoolean(false);

  private transient String requestMessage;

  // Do not serialize it for compatibility concerns.
  // If the procedure is restored, add mods to the data regions anyway.
  private transient boolean isAllLogicalView;

  // Expanded pattern tree for deletion (includes pre-deleted paths and referenced invalid paths)
  private transient PathPatternTree expandedPatternTreeForDeletion;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

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
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info(
              ProcedureMessages.CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TIMESERIES, requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            if (hasInvalidSeries.get() && isAllInvalidSeries.get()) {
              return Flow.NO_MORE_STATE;
            }
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
          SchemaUtils.invalidateCache(
              env,
              expandedPatternTreeForDeletion.serialize(),
              requestMessage,
              this::setFailure,
              true);
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

  // Return the total num of timeSeries in schemaEngine black list
  private long constructBlackList(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree, mayDeleteAudit);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    isAllLogicalView = true;
    final AtomicLong preDeletedNum = new AtomicLong(0);
    final List<TAliasSeriesInfo> allReferencedDisabledPaths =
        Collections.synchronizedList(new ArrayList<>());
    final List<ByteBuffer> allPreDeletedPaths = Collections.synchronizedList(new ArrayList<>());
    final DataNodeRegionTaskExecutor<TConstructSchemaBlackListReq, TConstructSchemaBlackListResp>
        constructBlackListTask =
            new DataNodeRegionTaskExecutor<
                TConstructSchemaBlackListReq, TConstructSchemaBlackListResp>(
                env,
                targetSchemaRegionGroup,
                false,
                CnToDnAsyncRequestType.CONSTRUCT_SCHEMA_BLACK_LIST_WITH_ALIAS_INFO,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TConstructSchemaBlackListReq(consensusGroupIdList, patternTreeBytes))) {
              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  final TDataNodeLocation dataNodeLocation,
                  final List<TConsensusGroupId> consensusGroupIdList,
                  final TConstructSchemaBlackListResp response) {
                final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                final TSStatus status = response.getStatus();
                if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    || status.getCode() == TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()) {
                  if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    isAllLogicalView = false;
                  }
                  // Accumulate pre-deleted number
                  preDeletedNum.addAndGet(response.getPreDeletedNum());

                  // Collect referenced invalid paths
                  if (response.getAliasSeriesInfoList() != null) {
                    allReferencedDisabledPaths.addAll(response.getAliasSeriesInfoList());
                  }

                  // Check if there are invalid series
                  if (response.isHasInvalidSeries()) {
                    hasInvalidSeries.set(true);
                  }
                  // Check if all matched series are invalid
                  if (!response.isIsAllInvalidSeries()) {
                    isAllInvalidSeries.set(false);
                  }

                  // Collect pre-deleted paths (active deletion paths)
                  if (response.getPreDeletedPaths() != null) {
                    allPreDeletedPaths.addAll(response.getPreDeletedPaths());
                  }

                } else if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  // Handle multiple errors if needed
                  failedRegionList.addAll(consensusGroupIdList);
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
                                "Delete time series %s failed when constructing black list "
                                    + "because failed to execute in all replicaset of %s %s. "
                                    + "Failure nodes: %s",
                                requestMessage,
                                consensusGroupId.type,
                                consensusGroupId.id,
                                dataNodeLocationSet))));
                interruptTask();
              }
            };
    constructBlackListTask.execute();

    if (isFailed()) {
      return 0;
    }

    // If all matched series are invalid, throw an exception
    if (hasInvalidSeries.get() && isAllInvalidSeries.get()) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  "Cannot delete time series: all matched series are invalid. "
                      + "Please delete the alias series instead.")));
      return 0;
    }

    // Reconstruct PathPatternTree based on different scenarios
    final PathPatternTree expandedPatternTree = new PathPatternTree();

    if (hasInvalidSeries.get()) {
      // If there are invalid paths, use preDeletedPaths (active deletion paths) and
      // referencedInvalidPaths
      // Add pre-deleted paths (active deletion paths)
      for (final ByteBuffer pathBuffer : allPreDeletedPaths) {
        try {
          final PartialPath path = (PartialPath) PathDeserializeUtil.deserialize(pathBuffer);
          expandedPatternTree.appendFullPath(path);
        } catch (final Exception e) {
          LOGGER.warn("Failed to deserialize pre-deleted path: {}", e.getMessage());
        }
      }

      // Add referenced invalid paths and their associated paths
      for (final TAliasSeriesInfo aliasSeriesInfo : allReferencedDisabledPaths) {
        try {
          // If this is an alias series, also delete its original physical series (if invalid)
          if (aliasSeriesInfo.isSetOriginalPath() && aliasSeriesInfo.getOriginalPath() != null) {
            final PartialPath originalPath =
                (PartialPath)
                    PathDeserializeUtil.deserialize(
                        ByteBuffer.wrap(aliasSeriesInfo.getOriginalPath()));
            expandedPatternTree.appendFullPath(originalPath);
          }
        } catch (final Exception e) {
          LOGGER.warn("Failed to deserialize referenced invalid path: {}", e.getMessage());
        }
      }
    } else {
      // If there are no invalid paths, use the original patternTree and referencedInvalidPaths
      // (if any)
      // Add all paths from the original patternTree
      for (final PartialPath path : patternTree.getAllPathPatterns()) {
        expandedPatternTree.appendPathPattern(path);
      }

      // Add referenced invalid paths if any (should be empty in this case, but for safety)
      for (final TAliasSeriesInfo aliasSeriesInfo : allReferencedDisabledPaths) {
        try {
          // If this is an alias series, also delete its original physical series (if invalid)
          if (aliasSeriesInfo.isSetOriginalPath() && aliasSeriesInfo.getOriginalPath() != null) {
            final PartialPath originalPath =
                (PartialPath)
                    PathDeserializeUtil.deserialize(
                        ByteBuffer.wrap(aliasSeriesInfo.getOriginalPath()));
            expandedPatternTree.appendFullPath(originalPath);
          }
        } catch (final Exception e) {
          LOGGER.warn("Failed to deserialize referenced invalid path: {}", e.getMessage());
        }
      }
    }

    expandedPatternTree.constructTree();
    expandedPatternTreeForDeletion = expandedPatternTree;

    // Return the total pre-deleted number (including normal pre-deleted series,
    // but excluding referenced invalid paths as they don't need pre-delete)
    return preDeletedNum.get();
  }

  private void deleteData(final ConfigNodeProcedureEnv env) {
    deleteDataWithRawPathPattern(env);
  }

  private void deleteDataWithRawPathPattern(final ConfigNodeProcedureEnv env) {
    executeDeleteData(env, expandedPatternTreeForDeletion);
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
            "delete data",
            env,
            relatedDataRegionGroup,
            false,
            CnToDnAsyncRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteDataForDeleteSchemaReq(
                        new ArrayList<>(consensusGroupIdList), patternTree.serialize())
                    .setIsGeneratedByPipe(isGeneratedByPipe)));
    deleteDataTask.execute();
  }

  private void deleteTimeSeriesSchema(final ConfigNodeProcedureEnv env) {
    final DeleteTimeSeriesRegionTaskExecutor<TDeleteTimeSeriesReq> deleteTimeSeriesTask =
        new DeleteTimeSeriesRegionTaskExecutor<>(
            "delete time series in schema engine",
            env,
            env.getConfigManager()
                .getRelatedSchemaRegionGroup(expandedPatternTreeForDeletion, mayDeleteAudit),
            CnToDnAsyncRequestType.DELETE_TIMESERIES,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TDeleteTimeSeriesReq(
                        consensusGroupIdList, expandedPatternTreeForDeletion.serialize())
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
              "roll back schema engine black list",
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
    return DeleteTimeSeriesState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  @Override
  public void applyPathPatterns(PathPatternTree patternTree) {
    if (this.patternTree != null && !this.patternTree.isEmpty()) {
      // Merge all patterns from the procedure's pattern tree
      for (PartialPath pattern : this.patternTree.getAllPathPatterns()) {
        patternTree.appendPathPattern(pattern);
      }
    }
  }

  @Override
  public boolean shouldCheckConflict() {
    return !isFinished();
  }

  public void setPatternTree(final PathPatternTree patternTree) {
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    patternTreeBytes = patternTree.serialize();
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
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
    if (getCurrentState() == DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE
        || getCurrentState() == DeleteTimeSeriesState.DELETE_DATA) {
      LOGGER.info(ProcedureMessages.SUCCESSFULLY_RESTORED_WILL_SET_MODS_TO_THE_DATA_REGIONS_ANYWAY);
    }
    if (byteBuffer.hasRemaining()) {
      mayDeleteAudit = ReadWriteIOUtils.readBoolean(byteBuffer);
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
        && this.getCurrentState().equals(that.getCurrentState())
        && this.getCycles() == getCycles()
        && this.isGeneratedByPipe == that.isGeneratedByPipe
        && this.patternTree.equals(that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(), getCurrentState(), getCycles(), isGeneratedByPipe, patternTree);
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
