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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.DeactivateTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.mpp.rpc.thrift.TConstructSchemaBlackListWithTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeactivateTemplateReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteDataForDeleteSchemaReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateMatchedSchemaCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TRollbackSchemaBlackListWithTemplateReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
import java.util.stream.Collectors;

public class DeactivateTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeactivateTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeactivateTemplateProcedure.class);

  private String queryId;
  private Map<PartialPath, List<Template>> templateSetInfo;

  private transient String requestMessage;
  // generate from templateSetInfo by concat pattern and measurement in template
  private transient PathPatternTree timeSeriesPatternTree;
  private transient ByteBuffer timeSeriesPatternTreeBytes;
  private transient Map<String, List<Integer>> dataNodeRequest;

  public DeactivateTemplateProcedure() {
    super();
  }

  public DeactivateTemplateProcedure(
      String queryId, Map<PartialPath, List<Template>> templateSetInfo) {
    super();
    this.queryId = queryId;
    setTemplateSetInfo(templateSetInfo);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, DeactivateTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CONSTRUCT_BLACK_LIST:
          LOGGER.info("Construct schema black list with template {}", requestMessage);
          if (constructBlackList(env) > 0) {
            setNextState(DeactivateTemplateState.CLEAN_DATANODE_SCHEMA_CACHE);
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case CLEAN_DATANODE_SCHEMA_CACHE:
          LOGGER.info("Invalidate cache of template timeseries {}", requestMessage);
          invalidateCache(env);
          break;
        case DELETE_DATA:
          LOGGER.info("Delete data of template timeseries {}", requestMessage);
          deleteData(env);
          break;
        case DEACTIVATE_TEMPLATE:
          LOGGER.info("Deactivate template of {}", requestMessage);
          deactivateTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized state " + state.toString()));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          String.format(
              "DeactivateTemplate-[%s] costs %sms",
              state.toString(), (System.currentTimeMillis() - startTime)));
    }
  }

  // return the total num of timeseries in schema black list
  private int constructBlackList(ConfigNodeProcedureEnv env) {
    Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(timeSeriesPatternTree);
    if (targetSchemaRegionGroup.isEmpty()) {
      return 0;
    }
    DeactivateTemplateRegionTask<TSStatus> constructBlackListTask =
        new DeactivateTemplateRegionTask<TSStatus>(
            "construct schema black list", env, targetSchemaRegionGroup) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            // construct request and send
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);

            AsyncClientHandler<TConstructSchemaBlackListWithTemplateReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE,
                    new TConstructSchemaBlackListWithTemplateReq(
                        consensusGroupIdList, dataNodeRequest),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    constructBlackListTask.execute();

    if (isFailed()) {
      return 0;
    }

    int preDeletedNum = 0;
    for (List<TSStatus> respList : constructBlackListTask.getResponseMap().values()) {
      for (TSStatus resp : respList) {
        preDeletedNum += Integer.parseInt(resp.getMessage());
      }
    }
    return preDeletedNum;
  }

  private void invalidateCache(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    AsyncClientHandler<TInvalidateMatchedSchemaCacheReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
            new TInvalidateMatchedSchemaCacheReq(timeSeriesPatternTreeBytes),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (TSStatus status : statusMap.values()) {
      // all dataNodes must clear the related schema cache
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to invalidate schema cache of template timeseries {}", requestMessage);
        setFailure(new ProcedureException(new MetadataException("Invalidate schema cache failed")));
        return;
      }
    }

    setNextState(DeactivateTemplateState.DELETE_DATA);
  }

  private void deleteData(ConfigNodeProcedureEnv env) {

    Map<TConsensusGroupId, TRegionReplicaSet> relatedDataRegionGroup =
        env.getConfigManager().getRelatedDataRegionGroup(timeSeriesPatternTree);

    // target timeseries has no data
    if (relatedDataRegionGroup.isEmpty()) {
      return;
    }

    DeactivateTemplateRegionTask<TSStatus> deleteDataTask =
        new DeactivateTemplateRegionTask<TSStatus>(
            "delete data", env, relatedDataRegionGroup, true) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TDeleteDataForDeleteSchemaReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
                    new TDeleteDataForDeleteSchemaReq(
                        new ArrayList<>(consensusGroupIdList), timeSeriesPatternTreeBytes),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    deleteDataTask.execute();
  }

  private void deactivateTemplate(ConfigNodeProcedureEnv env) {
    DeactivateTemplateRegionTask<TSStatus> deleteTimeSeriesTask =
        new DeactivateTemplateRegionTask<TSStatus>(
            "deactivate template schema",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(timeSeriesPatternTree)) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TDeactivateTemplateReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.DEACTIVATE_TEMPLATE,
                    new TDeactivateTemplateReq(consensusGroupIdList, dataNodeRequest),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    deleteTimeSeriesTask.execute();
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv env, DeactivateTemplateState deactivateTemplateState)
      throws IOException, InterruptedException, ProcedureException {
    DeactivateTemplateRegionTask<TSStatus> rollbackStateTask =
        new DeactivateTemplateRegionTask<TSStatus>(
            "roll back schema black list",
            env,
            env.getConfigManager().getRelatedSchemaRegionGroup(timeSeriesPatternTree)) {
          @Override
          protected Map<Integer, TSStatus> sendRequest(
              TDataNodeLocation dataNodeLocation, List<TConsensusGroupId> consensusGroupIdList) {
            Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation);
            AsyncClientHandler<TRollbackSchemaBlackListWithTemplateReq, TSStatus> clientHandler =
                new AsyncClientHandler<>(
                    DataNodeRequestType.ROLLBACK_SCHEMA_BLACK_LIST_WITH_TEMPLATE,
                    new TRollbackSchemaBlackListWithTemplateReq(
                        consensusGroupIdList, dataNodeRequest),
                    dataNodeLocationMap);
            AsyncDataNodeClientPool.getInstance()
                .sendAsyncRequestToDataNodeWithRetry(clientHandler);
            clientHandler
                .getResponseMap()
                .forEach(
                    (k, v) -> {
                      if (v.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                        saveDataNodeResponse(k, v);
                      }
                    });
            return clientHandler.getResponseMap();
          }
        };
    rollbackStateTask.execute();
  }

  @Override
  protected boolean isRollbackSupported(DeactivateTemplateState deactivateTemplateState) {
    return true;
  }

  @Override
  protected DeactivateTemplateState getState(int stateId) {
    return DeactivateTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(DeactivateTemplateState deactivateTemplateState) {
    return deactivateTemplateState.ordinal();
  }

  @Override
  protected DeactivateTemplateState getInitialState() {
    return DeactivateTemplateState.CONSTRUCT_BLACK_LIST;
  }

  public String getQueryId() {
    return queryId;
  }

  public Map<PartialPath, List<Template>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  private void setTemplateSetInfo(Map<PartialPath, List<Template>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
    prepareRequestMessage(templateSetInfo);
    prepareTimeSeriesPatternTree(templateSetInfo);
    prepareDataNodeRequest(templateSetInfo);
  }

  private void prepareRequestMessage(Map<PartialPath, List<Template>> templateSetInfo) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{");
    for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      stringBuilder
          .append(entry.getKey())
          .append(":")
          .append(entry.getValue().stream().map(Template::getName).collect(Collectors.toList()))
          .append(";");
    }
    stringBuilder.append("}");
    this.requestMessage = stringBuilder.toString();
  }

  private void prepareTimeSeriesPatternTree(Map<PartialPath, List<Template>> templateSetInfo) {
    PathPatternTree patternTree = new PathPatternTree();
    for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      for (Template template : entry.getValue()) {
        for (String measurement : template.getSchemaMap().keySet()) {
          patternTree.appendPathPattern(entry.getKey().concatNode(measurement));
        }
      }
    }
    patternTree.constructTree();
    this.timeSeriesPatternTree = patternTree;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      patternTree.serialize(dataOutputStream);
    } catch (IOException ignored) {

    }
    this.timeSeriesPatternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  private void prepareDataNodeRequest(Map<PartialPath, List<Template>> templateSetInfo) {
    Map<String, List<Integer>> dataNodeRequest = new HashMap<>();
    templateSetInfo.forEach(
        (k, v) ->
            dataNodeRequest.put(
                k.getFullPath(), v.stream().map(Template::getId).collect(Collectors.toList())));
    this.dataNodeRequest = dataNodeRequest;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(ProcedureFactory.ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE.ordinal());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(templateSetInfo.size(), stream);
    for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Template template : entry.getValue()) {
        template.serialize(stream);
      }
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<PartialPath, List<Template>> templateSetInfo = new HashMap<>();
    for (int i = 0; i < size; i++) {
      PartialPath pattern = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
      int templateNum = ReadWriteIOUtils.readInt(byteBuffer);
      List<Template> templateList = new ArrayList<>(templateNum);
      for (int j = 0; j < templateNum; j++) {
        Template template = new Template();
        template.deserialize(byteBuffer);
        templateList.add(template);
      }
      templateSetInfo.put(pattern, templateList);
    }
    setTemplateSetInfo(templateSetInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeactivateTemplateProcedure that = (DeactivateTemplateProcedure) o;
    return Objects.equals(queryId, that.queryId)
        && Objects.equals(templateSetInfo, that.templateSetInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, templateSetInfo);
  }

  private abstract class DeactivateTemplateRegionTask<T> extends DataNodeRegionTask<T> {

    private final String taskName;

    DeactivateTemplateRegionTask(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup) {
      super(env, targetSchemaRegionGroup, false);
      this.taskName = taskName;
    }

    DeactivateTemplateRegionTask(
        String taskName,
        ConfigNodeProcedureEnv env,
        Map<TConsensusGroupId, TRegionReplicaSet> targetSchemaRegionGroup,
        boolean executeOnAllReplicaset) {
      super(env, targetSchemaRegionGroup, executeOnAllReplicaset);
      this.taskName = taskName;
    }

    @Override
    protected boolean hasFailure() {
      return isFailed();
    }

    @Override
    protected void onExecutionFailure(TDataNodeLocation dataNodeLocation) {
      LOGGER.error(
          "Failed to execute [{}] of deactivate template {} on {}",
          taskName,
          requestMessage,
          dataNodeLocation);
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Deactivate template of %s failed when [%s]", requestMessage, taskName))));
    }

    @Override
    protected void onAllReplicasetFailure(TConsensusGroupId consensusGroupId) {
      setFailure(
          new ProcedureException(
              new MetadataException(
                  String.format(
                      "Deactivate template of %s failed when [%s] because all replicaset of schemaRegion %s failed.",
                      requestMessage, taskName, consensusGroupId.id))));
    }
  }
}
