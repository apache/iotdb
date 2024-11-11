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
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCType;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;

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
                            "Failed to execute in all replicaset of schemaRegion %s when checking the template %s on %s. Failure nodes: %s",
                            consensusGroupId.id, template, patternTree, dataNodeLocationSet));
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
                            "Failed to execute in all replicaset of schemaRegion %s when checking templates on path %s. Failure nodes: %s",
                            consensusGroupId.id, deleteDatabasePatternPaths, dataNodeLocationSet));
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

  public static Map<Integer, TSStatus> preReleaseTable(
      final String database, final TsTable table, final ConfigManager configManager) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.PRE_CREATE_OR_ADD_COLUMN.getOperationType());
    req.setTableInfo(TsTableInternalRPCUtil.serializeSingleTsTableWithDatabase(database, table));

    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseMap().entrySet().stream()
        .filter(entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Map<Integer, TSStatus> commitReleaseTable(
      final String database, final String tableName, final ConfigManager configManager) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.COMMIT_CREATE_OR_ADD_COLUMN.getOperationType());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(tableName, outputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream will not throw IOException
    }
    req.setTableInfo(outputStream.toByteArray());

    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseMap().entrySet().stream()
        .filter(entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Map<Integer, TSStatus> rollbackPreRelease(
      final String database, final String tableName, final ConfigManager configManager) {
    final TUpdateTableReq req = new TUpdateTableReq();
    req.setType(TsTableInternalRPCType.ROLLBACK_CREATE_OR_ADD_COLUMN.getOperationType());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      ReadWriteIOUtils.write(tableName, outputStream);
    } catch (final IOException ignore) {
      // ByteArrayOutputStream will not throw IOException
    }
    req.setTableInfo(outputStream.toByteArray());

    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseMap().entrySet().stream()
        .filter(entry -> entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static TSStatus executeInConsensusLayer(
      final ConfigPhysicalPlan plan, final ConfigNodeProcedureEnv env, final Logger logger) {
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(plan);
    } catch (final ConsensusException e) {
      logger.warn("Failed in the write API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return status;
  }
}
