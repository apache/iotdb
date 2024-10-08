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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteTimeSeriesState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListReq;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DeleteTimeSeriesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteTimeSeriesState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeSeriesProcedure.class);

  private String queryId;

  private PathPatternTree patternTree;
  private transient ByteBuffer patternTreeBytes;

  private transient String requestMessage;

  // Do not serialize it for compatibility concerns.
  // If the procedure is restored, add mods to the data regions anyway.
  private transient boolean isAllLogicalView;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public DeleteTimeSeriesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DeleteTimeSeriesProcedure(
      final String queryId, final PathPatternTree patternTree, final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    setPatternTree(patternTree);
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final DeleteTimeSeriesState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schemaEngine black list of timeSeries {}", requestMessage);
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
          LOGGER.info("Invalidate cache of timeSeries {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of timeSeries {}", requestMessage);
          deleteData(env);
          break;
        case DELETE_TIMESERIES_SCHEMA:
          LOGGER.info("Delete timeSeries schemaEngine of {}", requestMessage);
          deleteTimeSeriesSchema(env);
          collectPayload4Pipe(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DeleteTimeSeries-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  // Return the total num of timeSeries in schemaEngine black list
  private long constructBlackList(final ConfigNodeProcedureEnv env) {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    isAllLogicalView = true;
    final List<TSStatus> successResult = new ArrayList<>();
    final DeleteTimeSeriesRegionTaskExecutor<TConstructSchemaBlackListReq> constructBlackListTask =
        new DeleteTimeSeriesRegionTaskExecutor<TConstructSchemaBlackListReq>(
            "construct schema engine black list",
            env,
            targetSchemaRegionGroup,
            CnToDnRequestType.CONSTRUCT_SCHEMA_BLACK_LIST,
            ((dataNodeLocation, consensusGroupIdList) ->
                new TConstructSchemaBlackListReq(consensusGroupIdList, patternTreeBytes))) {
          @Override
          protected List<TConsensusGroupId> processResponseOfOneDataNode(
              final TDataNodeLocation dataNodeLocation,
              final List<TConsensusGroupId> consensusGroupIdList,
              final TSStatus response) {
            final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
            if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
              isAllLogicalView = false;
              successResult.add(response);
            } else if (response.getCode() == TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()) {
              successResult.add(response);
            } else if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
              final List<TSStatus> subStatusList = response.getSubStatus();
              for (int i = 0; i < subStatusList.size(); i++) {
                if (subStatusList.get(i).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  successResult.add(subStatusList.get(i));
                } else {
                  failedRegionList.add(consensusGroupIdList.get(i));
                }
              }
            } else {
              failedRegionList.addAll(consensusGroupIdList);
            }
            return failedRegionList;
          }
        };
    constructBlackListTask.execute();

    if (isFailed()) {
      return 0;
    }

    return successResult.stream()
        .mapToLong(resp -> Long.parseLong(resp.getMessage()))
        .reduce(Long::sum)
        .orElse(0L);
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(patternTreeBytes),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // All dataNodes must clear the related schemaEngine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schemaEngine cache of timeSeries {}", requestMessage);
        setFailure(
            new ProcedureException(new MetadataException("Invalidate schemaEngine cache failed")));
        return;
      }
    }

    setNextState(DeleteTimeSeriesState.DELETE_DATA);
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
        env.getConfigManager().getRelatedDataRegionGroup(patternTree);

    // Target timeSeries has no data
    if (relatedDataRegionGroup.isEmpty()) {
      return;
    }

    final DeleteTimeSeriesRegionTaskExecutor<TDeleteDataForDeleteSchemaReq> deleteDataTask =
        new DeleteTimeSeriesRegionTaskExecutor<>(
            "delete data",
            env,
            relatedDataRegionGroup,
            true,
            CnToDnRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
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
            "delete time series in schema engine",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
            CnToDnRequestType.DELETE_TIMESERIES,
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
              "roll back schema engine black list",
              env,
              env.getConfigManager().getRelatedSchemaRegionGroup(patternTree),
              CnToDnRequestType.ROLLBACK_SCHEMA_BLACK_LIST,
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

  public void setPatternTree(final PathPatternTree patternTree) {
    this.patternTree = patternTree;
    requestMessage = patternTree.getAllPathPatterns().toString();
    patternTreeBytes = preparePatternTreeBytesData(patternTree);
  }

  private ByteBuffer preparePatternTreeBytesData(final PathPatternTree patternTree) {
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
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    setPatternTree(PathPatternTree.deserialize(byteBuffer));
    if (getCurrentState() == DeleteTimeSeriesState.CLEAN_DATANODE_SCHEMA_CACHE
        || getCurrentState() == DeleteTimeSeriesState.DELETE_DATA) {
      LOGGER.info("Successfully restored, will set mods to the data regions anyway");
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
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

  private class DeleteTimeSeriesRegionTaskExecutor<Q>
      extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    DeleteTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        final CnToDnRequestType dataNodeRequestType,
        final BiFunction<TDataNodeLocation, List<TConsensusGroupId>, Q> dataNodeRequestGenerator) {
      super(env, targetSchemaRegionGroup, false, dataNodeRequestType, dataNodeRequestGenerator);
      this.taskName = taskName;
    }

    DeleteTimeSeriesRegionTaskExecutor(
        final String taskName,
        final ConfigNodeProcedureEnv env,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetDataRegionGroup,
        final boolean executeOnAllReplicaset,
        final CnToDnRequestType dataNodeRequestType,
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
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        final TSStatus response) {
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = response.getSubStatus();
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
                      "Delete time series %s failed when [%s] because failed to execute in all replicaset of %s %s. Failure nodes: %s",
                      requestMessage,
                      taskName,
                      consensusGroupId.type,
                      consensusGroupId.id,
                      dataNodeLocationSet))));
      interruptTask();
    }
  }
}
