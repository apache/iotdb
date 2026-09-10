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
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator;
import org.apache.iotdb.confignode.manager.lease.DataNodeContactTracker;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaUtils {
  /**
   * Check whether the specific template is activated on the given pattern tree.
   *
   * @return {@code true} if the template is activated on the given pattern tree, {@code false}
   *     otherwise.
   * @throws MetadataException if any error occurs when checking the activation.
   */
  public static boolean checkDataNodeTemplateActivation(
      ConfigManager configManager, PathPatternTree patternTree, Template template)
      throws MetadataException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    ByteBuffer patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        configManager.getRelatedSchemaRegionGroup(patternTree);

    List<TCountPathsUsingTemplateResp> respList = new ArrayList<>();
    final MetadataException[] exception = {null};
    DataNodeRegionTaskExecutor<TCountPathsUsingTemplateReq, TCountPathsUsingTemplateResp>
        regionTask =
            new DataNodeRegionTaskExecutor<
                TCountPathsUsingTemplateReq, TCountPathsUsingTemplateResp>(
                configManager,
                relatedSchemaRegionGroup,
                false,
                CnToDnAsyncRequestType.COUNT_PATHS_USING_TEMPLATE,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TCountPathsUsingTemplateReq(
                        template.getId(), patternTreeBytes, consensusGroupIdList))) {

              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  TDataNodeLocation dataNodeLocation,
                  List<TConsensusGroupId> consensusGroupIdList,
                  TCountPathsUsingTemplateResp response) {
                respList.add(response);
                List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  return failedRegionList;
                }

                if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  List<TSStatus> subStatus = response.getStatus().getSubStatus();
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
                  TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet) {
                exception[0] =
                    new MetadataException(
                        String.format(
                            ProcedureMessages
                                .FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING_2,
                            consensusGroupId.id,
                            template,
                            patternTree,
                            dataNodeLocationSet));
                interruptTask();
              }
            };
    regionTask.execute();
    if (exception[0] != null) {
      throw exception[0];
    }
    for (TCountPathsUsingTemplateResp resp : respList) {
      if (resp.count > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whether any template is activated on the given schema regions.
   *
   * @throws MetadataException if any error occurs when checking the activation, or there are
   *     templates under the databases.
   */
  public static void checkSchemaRegionUsingTemplate(
      ConfigManager configManager, List<PartialPath> deleteDatabasePatternPaths)
      throws MetadataException {
    PathPatternTree deleteDatabasePatternTree = new PathPatternTree();
    for (PartialPath path : deleteDatabasePatternPaths) {
      deleteDatabasePatternTree.appendPathPattern(path);
    }
    deleteDatabasePatternTree.constructTree();
    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        configManager.getRelatedSchemaRegionGroup(deleteDatabasePatternTree);
    List<TCheckSchemaRegionUsingTemplateResp> respList = new ArrayList<>();
    final MetadataException[] exception = {null};
    DataNodeRegionTaskExecutor<
            TCheckSchemaRegionUsingTemplateReq, TCheckSchemaRegionUsingTemplateResp>
        regionTask =
            new DataNodeRegionTaskExecutor<
                TCheckSchemaRegionUsingTemplateReq, TCheckSchemaRegionUsingTemplateResp>(
                configManager,
                relatedSchemaRegionGroup,
                false,
                CnToDnAsyncRequestType.CHECK_SCHEMA_REGION_USING_TEMPLATE,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TCheckSchemaRegionUsingTemplateReq(consensusGroupIdList))) {

              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  TDataNodeLocation dataNodeLocation,
                  List<TConsensusGroupId> consensusGroupIdList,
                  TCheckSchemaRegionUsingTemplateResp response) {
                respList.add(response);
                List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  failureMap.remove(dataNodeLocation);
                  return failedRegionList;
                }

                if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  List<TSStatus> subStatus = response.getStatus().getSubStatus();
                  for (int i = 0; i < subStatus.size(); i++) {
                    if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                      failedRegionList.add(consensusGroupIdList.get(i));
                    }
                  }
                } else {
                  failedRegionList.addAll(consensusGroupIdList);
                }
                if (!failedRegionList.isEmpty()) {
                  failureMap.put(
                      dataNodeLocation, RpcUtils.extractFailureStatues(response.getStatus()));
                } else {
                  failureMap.remove(dataNodeLocation);
                }
                return failedRegionList;
              }

              @Override
              protected void onAllReplicasetFailure(
                  TConsensusGroupId consensusGroupId, Set<TDataNodeLocation> dataNodeLocationSet) {
                exception[0] =
                    new MetadataException(
                        String.format(
                            ProcedureMessages
                                .FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING,
                            consensusGroupId.id,
                            deleteDatabasePatternPaths,
                            printFailureMap()));
                interruptTask();
              }
            };
    regionTask.execute();
    if (exception[0] != null) {
      throw exception[0];
    }
    for (TCheckSchemaRegionUsingTemplateResp resp : respList) {
      if (resp.result) {
        throw new PathNotExistException(
            deleteDatabasePatternPaths.stream()
                .map(PartialPath::getFullPath)
                .collect(Collectors.toList()),
            false);
      }
    }
  }

  /** Build the PRE_UPDATE_TABLE request used to pre-release a table change to DataNodes. */
  public static TUpdateTableReq buildPreUpdateTableReq(
      final String database, final TsTable table, final String oldName) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.PRE_UPDATE_TABLE.getOperationType());
    req.setTableInfo(TsTableInternalRPCUtil.serializeSingleTsTableWithDatabase(database, table));
    req.setOldName(oldName);
    return req;
  }

  /**
   * Broadcast a table update to exactly {@code targets} and return the full per-nodeId response map
   * (both successes and failures). Used by {@link
   * org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator}, which needs to know which
   * DataNodes acknowledged in order to decide whether it is safe to proceed past the rest.
   */
  public static Map<Integer, TSStatus> broadcastTableUpdate(
      final TUpdateTableReq req, final Map<Integer, TDataNodeLocation> targets) {
    final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.UPDATE_TABLE, req, targets);
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    return clientHandler.getResponseMap();
  }

  private static Map<Integer, TSStatus> failedOnly(final Map<Integer, TSStatus> responses) {
    return responses.entrySet().stream()
        .filter(entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Map<Integer, TDataNodeLocation> filterFencedDataNode(
      final ConfigManager configManager) {
    return configManager.getNodeManager().getRegisteredDataNodeLocations().entrySet().stream()
        .filter(
            entry ->
                configManager.getLoadManager().getNodeStatus(entry.getKey()) != NodeStatus.Unknown
                    || !DataNodeContactTracker.getInstance().isDataNodeFenced(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Map<Integer, TSStatus> commitReleaseTable(
      final String database,
      final String tableName,
      final ConfigManager configManager,
      final @Nullable String oldName) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.COMMIT_UPDATE_TABLE.getOperationType());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(tableName, outputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream will not throw IOException
    }
    req.setTableInfo(outputStream.toByteArray());
    req.setOldName(oldName);

    final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TABLE, req, filterFencedDataNode(configManager));
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            clientHandler,
            ClusterCachePropagator.BROADCAST_RPC_RETRY,
            ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
    return clientHandler.getResponseMap().entrySet().stream()
        .filter(entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Build the ROLLBACK_UPDATE_TABLE request used to roll back a pre-released table change. */
  public static TUpdateTableReq rollbackUpdateTableReq(
      final String database, final String tableName, final String oldName) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.ROLLBACK_UPDATE_TABLE.getOperationType());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(tableName, outputStream);
    } catch (final IOException ignore) {
      // ByteArrayOutputStream will not throw IOException
    }
    req.setTableInfo(outputStream.toByteArray());
    req.setOldName(oldName);
    return req;
  }

  /**
   * Broadcast an INVALIDATE_MATCHED_SCHEMA_CACHE to all DataNodes through {@link
   * ClusterCachePropagator}: proceed once every unreachable DataNode is provably self-fenced (it
   * fails closed on its schema cache and resyncs on recovery, so it cannot serve the
   * deleted/altered series), instead of hard-failing on the first unreachable DataNode. Returns
   * whether it is safe to proceed; the caller maps {@code false} to its own failure.
   *
   * <p>The propagator may re-broadcast while waiting for unacked DataNodes, so a fresh request with
   * a duplicated buffer is built on each attempt — a consumed buffer can never be re-sent as an
   * empty (and silently-successful) invalidation.
   */
  public static boolean invalidateMatchedSchemaCache(
      final ConfigManager configManager,
      final ByteBuffer patternTreeBytes,
      final boolean needLock) {
    return new ClusterCachePropagator(filterFencedDataNode(configManager))
        .propagate(
            targets -> {
              final DataNodeAsyncRequestContext<TInvalidateMatchedSchemaCacheReq, TSStatus>
                  clientHandler =
                      new DataNodeAsyncRequestContext<>(
                          CnToDnAsyncRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
                          new TInvalidateMatchedSchemaCacheReq(patternTreeBytes.duplicate())
                              .setNeedLock(needLock),
                          targets);
              CnToDnInternalServiceAsyncRequestManager.getInstance()
                  .sendAsyncRequest(
                      clientHandler,
                      ClusterCachePropagator.BROADCAST_RPC_RETRY,
                      ClusterCachePropagator.BROADCAST_RPC_TIMEOUT_MS);
              return clientHandler.getResponseMap();
            });
  }

  public static TSStatus executeInConsensusLayer(
      final ConfigPhysicalPlan plan, final ConfigNodeProcedureEnv env, final Logger logger) {
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (final ConsensusException e) {
      logger.warn(ProcedureMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }
}
