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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckSchemaRegionUsingTemplateResp;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TCountPathsUsingTemplateResp;
import org.apache.iotdb.rpc.TSStatusCode;

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
   * @return true if the template is activated on the given pattern tree, false otherwise.
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
                DataNodeRequestType.COUNT_PATHS_USING_TEMPLATE,
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
                            "all replicaset of schemaRegion %s failed. %s",
                            consensusGroupId.id, dataNodeLocationSet));
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
   * @return true if the template is activated on the given pattern tree, false otherwise.
   * @throws MetadataException if any error occurs when checking the activation.
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
                DataNodeRequestType.CHECK_SCHEMA_REGION_USING_TEMPLATE,
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
                            "all replicaset of schemaRegion %s failed. %s",
                            consensusGroupId.id, dataNodeLocationSet));
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
}
