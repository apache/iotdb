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
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterLogicalViewState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.metadata.view.ViewNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TAlterViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AlterLogicalViewProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterLogicalViewState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AlterLogicalViewProcedure.class);

  private String queryId;

  private Map<PartialPath, ViewExpression> viewPathToSourceMap;

  private transient PathPatternTree pathPatternTree;
  private transient ByteBuffer patternTreeBytes;

  protected final Map<TDataNodeLocation, TSStatus> failureMap = new HashMap<>();

  public AlterLogicalViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AlterLogicalViewProcedure(
      final String queryId,
      final Map<PartialPath, ViewExpression> viewPathToSourceMap,
      final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.viewPathToSourceMap = viewPathToSourceMap;
    generatePathPatternTree();
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterLogicalViewState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of view {}", viewPathToSourceMap.keySet());
          invalidateCache(env);
          setNextState(AlterLogicalViewState.ALTER_LOGICAL_VIEW);
          return Flow.HAS_MORE_STATE;
        case ALTER_LOGICAL_VIEW:
          LOGGER.info("Alter view {}", viewPathToSourceMap.keySet());
          try {
            alterLogicalView(env);
          } catch (final ProcedureException e) {
            setFailure(e);
          }
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state));
          return Flow.NO_MORE_STATE;
      }
    } finally {
      LOGGER.info(
          "AlterLogicalView-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void invalidateCache(final ConfigNodeProcedureEnv env) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(patternTreeBytes),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    final Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (final TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schemaengine cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to invalidate schemaengine cache of view {}", viewPathToSourceMap.keySet());
        setFailure(
            new ProcedureException(
                new MetadataException("Invalidate view schemaengine cache failed")));
        return;
      }
    }
  }

  private void alterLogicalView(final ConfigNodeProcedureEnv env) throws ProcedureException {
    final Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(pathPatternTree);
    final Map<TConsensusGroupId, Map<PartialPath, ViewExpression>> schemaRegionRequestMap =
        new HashMap<>();
    for (final Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      schemaRegionRequestMap
          .computeIfAbsent(getBelongedSchemaRegion(env, entry.getKey()), k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }
    final AlterLogicalViewRegionTaskExecutor<TAlterViewReq> regionTaskExecutor =
        new AlterLogicalViewRegionTaskExecutor<>(
            "Alter view",
            env,
            targetSchemaRegionGroup,
            CnToDnAsyncRequestType.ALTER_VIEW,
            (dataNodeLocation, consensusGroupIdList) -> {
              TAlterViewReq req = new TAlterViewReq().setIsGeneratedByPipe(isGeneratedByPipe);
              req.setSchemaRegionIdList(consensusGroupIdList);
              List<ByteBuffer> viewMapBinaryList = new ArrayList<>();
              for (TConsensusGroupId consensusGroupId : consensusGroupIdList) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                Map<PartialPath, ViewExpression> viewMap =
                    schemaRegionRequestMap.get(consensusGroupId);
                try {
                  ReadWriteIOUtils.write(viewMap.size(), stream);
                  for (Map.Entry<PartialPath, ViewExpression> viewEntry : viewMap.entrySet()) {
                    viewEntry.getKey().serialize(stream);
                    ViewExpression.serialize(viewEntry.getValue(), stream);
                  }
                } catch (IOException e) {
                  // won't reach here
                }
                viewMapBinaryList.add(ByteBuffer.wrap(stream.toByteArray()));
              }
              req.setViewBinaryList(viewMapBinaryList);
              return req;
            });
    regionTaskExecutor.execute();
    if (isFailed()) {
      return;
    }

    invalidateCache(env);
  }

  private TConsensusGroupId getBelongedSchemaRegion(
      final ConfigNodeProcedureEnv env, final PartialPath viewPath) throws ProcedureException {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendFullPath(viewPath);
    patternTree.constructTree();
    final Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        env.getConfigManager().getSchemaPartition(patternTree, false).schemaPartitionTable;
    if (schemaPartitionTable.isEmpty()) {
      throw new ProcedureException(new ViewNotExistException(viewPath.getFullPath()));
    } else {
      final Map<TSeriesPartitionSlot, TConsensusGroupId> slotMap =
          schemaPartitionTable.values().iterator().next();
      if (slotMap.isEmpty()) {
        throw new ProcedureException(new ViewNotExistException(viewPath.getFullPath()));
      } else {
        return slotMap.values().iterator().next();
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(final AlterLogicalViewState alterLogicalViewState) {
    return true;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final AlterLogicalViewState alterLogicalViewState)
      throws IOException, InterruptedException, ProcedureException {
    if (alterLogicalViewState == AlterLogicalViewState.CLEAN_DATANODE_SCHEMA_CACHE) {
      invalidateCache(env);
    }
  }

  @Override
  protected AlterLogicalViewState getState(final int stateId) {
    return AlterLogicalViewState.values()[stateId];
  }

  @Override
  protected int getStateId(final AlterLogicalViewState alterLogicalViewState) {
    return alterLogicalViewState.ordinal();
  }

  @Override
  protected AlterLogicalViewState getInitialState() {
    return AlterLogicalViewState.CLEAN_DATANODE_SCHEMA_CACHE;
  }

  public String getQueryId() {
    return queryId;
  }

  private void generatePathPatternTree() {
    final PathPatternTree patternTree = new PathPatternTree();
    for (final PartialPath path : viewPathToSourceMap.keySet()) {
      patternTree.appendFullPath(path);
    }
    patternTree.constructTree();
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (final IOException ignored) {
      // won't reach here
    }

    this.pathPatternTree = patternTree;
    this.patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(this.viewPathToSourceMap.size(), stream);
    for (final Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceMap.entrySet()) {
      entry.getKey().serialize(stream);
      ViewExpression.serialize(entry.getValue(), stream);
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);

    final Map<PartialPath, ViewExpression> viewPathToSourceMap = new HashMap<>();
    final int size = byteBuffer.getInt();
    PartialPath path;
    ViewExpression viewExpression;
    for (int i = 0; i < size; i++) {
      path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      viewExpression = ViewExpression.deserialize(byteBuffer);
      viewPathToSourceMap.put(path, viewExpression);
    }
    this.viewPathToSourceMap = viewPathToSourceMap;
    generatePathPatternTree();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlterLogicalViewProcedure)) {
      return false;
    }
    final AlterLogicalViewProcedure that = (AlterLogicalViewProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && Objects.equals(getCycles(), that.getCycles())
        && Objects.equals(isGeneratedByPipe, that.isGeneratedByPipe)
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(viewPathToSourceMap, that.viewPathToSourceMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        queryId,
        viewPathToSourceMap);
  }

  private class AlterLogicalViewRegionTaskExecutor<Q>
      extends DataNodeRegionTaskExecutor<Q, TSStatus> {

    private final String taskName;

    private final List<TSStatus> failureStatusList = new ArrayList<>();

    AlterLogicalViewRegionTaskExecutor(
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
      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return failedRegionList;
      }

      if (response.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatusList = response.getSubStatus();
        TSStatus subStatus;
        for (int i = 0; i < subStatusList.size(); i++) {
          subStatus = subStatusList.get(i);
          if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            if (subStatus.getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
              failedRegionList.add(consensusGroupIdList.get(i));
            } else {
              collectFailure(subStatus);
              interruptTask();
            }
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    private void collectFailure(final TSStatus failureStatus) {
      if (failureStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        failureStatusList.addAll(failureStatus.getSubStatus());
      } else {
        failureStatusList.add(failureStatus);
      }
      if (failureStatusList.size() == 1) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    failureStatusList.get(0).getMessage(), failureStatusList.get(0).getCode())));
      } else {
        setFailure(
            new ProcedureException(
                new BatchProcessException(failureStatusList.toArray(new TSStatus[0]))));
      }
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Alter view %s failed when [%s] because failed to execute in all replicaset of schemaRegion %s. Failure nodes: %s, statuses: %s",
                      viewPathToSourceMap.keySet(),
                      taskName,
                      consensusGroupId.id,
                      dataNodeLocationSet.stream()
                          .map(TDataNodeLocation::getDataNodeId)
                          .collect(Collectors.toSet()),
                      failureStatusList))));
      interruptTask();
    }
  }
}
