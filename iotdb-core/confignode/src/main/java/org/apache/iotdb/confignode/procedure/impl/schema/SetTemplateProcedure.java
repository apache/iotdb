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
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.template.TemplateInfoResp;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.SetTemplateState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.exception.metadata.template.TemplateIncompatibleException;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.schemaengine.template.TemplateInternalRPCUtil;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceReq;
import org.apache.iotdb.mpp.rpc.thrift.TCheckTimeSeriesExistenceResp;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class SetTemplateProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, SetTemplateState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SetTemplateProcedure.class);

  private String queryId;
  private String templateName;
  private String templateSetPath;

  private static final String CONSENSUS_WRITE_ERROR =
      "Failed in the write API executing the consensus layer due to: ";

  public SetTemplateProcedure(boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetTemplateProcedure(
      String queryId, String templateName, String templateSetPath, boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
    this.queryId = queryId;
    this.templateName = templateName;
    this.templateSetPath = templateSetPath;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, SetTemplateState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case VALIDATE_TEMPLATE_EXISTENCE:
          LOGGER.info(
              "Check template existence set on path {} when try setting template {}",
              templateSetPath,
              templateName);
          validateTemplateExistence(env);
          break;
        case PRE_SET:
          LOGGER.info("Pre set schemaengine template {} on path {}", templateName, templateSetPath);
          preSetTemplate(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Pre release schemaengine template {} set on path {}", templateName, templateSetPath);
          preReleaseTemplate(env);
          break;
        case VALIDATE_TIMESERIES_EXISTENCE:
          LOGGER.info(
              "Check timeseries existence under path {} when try setting template {}",
              templateSetPath,
              templateName);
          validateTimeSeriesExistence(env);
          break;
        case COMMIT_SET:
          LOGGER.info(
              "Commit set schemaengine template {} on path {}", templateName, templateSetPath);
          commitSetTemplate(env);
          setNextState(SetTemplateState.COMMIT_RELEASE);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              "Commit release schemaengine template {} set on path {}",
              templateName,
              templateSetPath);
          commitReleaseTemplate(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized SetTemplateState " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "SetSchemaTemplate-[{}] costs {}ms", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void validateTemplateExistence(ConfigNodeProcedureEnv env) {
    // check whether the template can be set on given path
    CheckTemplateSettablePlan checkTemplateSettablePlan =
        new CheckTemplateSettablePlan(templateName, templateSetPath);
    TemplateInfoResp resp;
    try {
      resp =
          (TemplateInfoResp)
              env.getConfigManager().getConsensusManager().read(checkTemplateSettablePlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      resp = new TemplateInfoResp();
      resp.setStatus(res);
    }
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(SetTemplateState.PRE_SET);
    } else {
      setFailure(
          new ProcedureException(
              new IoTDBException(resp.getStatus().getMessage(), resp.getStatus().getCode())));
    }
  }

  private void preSetTemplate(ConfigNodeProcedureEnv env) {
    PreSetSchemaTemplatePlan preSetSchemaTemplatePlan =
        new PreSetSchemaTemplatePlan(templateName, templateSetPath);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(preSetSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(SetTemplateState.PRE_RELEASE);
    } else {
      LOGGER.warn(
          "Failed to pre set template {} on path {} due to {}",
          templateName,
          templateSetPath,
          status.getMessage());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void preReleaseTemplate(ConfigNodeProcedureEnv env) {
    Template template = getTemplate(env);
    if (template == null) {
      // already setFailure
      return;
    }

    TUpdateTemplateReq req = new TUpdateTemplateReq();
    req.setType(TemplateInternalRPCUpdateType.ADD_TEMPLATE_PRE_SET_INFO.toByte());
    req.setTemplateInfo(
        TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template, templateSetPath));

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TUpdateTemplateReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.UPDATE_TEMPLATE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync template {} pre-set info on path {} to DataNode {}",
            templateName,
            templateSetPath,
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(new ProcedureException(new MetadataException("Pre set template failed")));
        return;
      }
    }
    setNextState(SetTemplateState.VALIDATE_TIMESERIES_EXISTENCE);
  }

  private Template getTemplate(ConfigNodeProcedureEnv env) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateResp;
    try {
      templateResp =
          (TemplateInfoResp)
              env.getConfigManager().getConsensusManager().read(getSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      templateResp = new TemplateInfoResp();
      templateResp.setStatus(res);
    }
    if (templateResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(
          new ProcedureException(
              new IoTDBException(
                  templateResp.getStatus().getMessage(), templateResp.getStatus().getCode())));
      return null;
    }
    if (templateResp.getTemplateList() == null || templateResp.getTemplateList().isEmpty()) {
      setFailure(new ProcedureException(new UndefinedTemplateException(templateName)));
      return null;
    }
    return templateResp.getTemplateList().get(0);
  }

  private void validateTimeSeriesExistence(ConfigNodeProcedureEnv env) {
    PathPatternTree patternTree = new PathPatternTree();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    PartialPath path = null;
    try {
      path = new PartialPath(templateSetPath);
      patternTree.appendPathPattern(path);
      patternTree.appendPathPattern(path.concatNode(MULTI_LEVEL_PATH_WILDCARD));
      patternTree.serialize(dataOutputStream);
    } catch (IllegalPathException | IOException ignored) {
    }
    ByteBuffer patternTreeBytes = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Map<TConsensusGroupId, TRegionReplicaSet> relatedSchemaRegionGroup =
        env.getConfigManager().getRelatedSchemaRegionGroup(patternTree);

    List<TCheckTimeSeriesExistenceResp> respList = new ArrayList<>();
    DataNodeRegionTaskExecutor<TCheckTimeSeriesExistenceReq, TCheckTimeSeriesExistenceResp>
        regionTask =
            new DataNodeRegionTaskExecutor<
                TCheckTimeSeriesExistenceReq, TCheckTimeSeriesExistenceResp>(
                env,
                relatedSchemaRegionGroup,
                false,
                CnToDnRequestType.CHECK_TIMESERIES_EXISTENCE,
                ((dataNodeLocation, consensusGroupIdList) ->
                    new TCheckTimeSeriesExistenceReq(patternTreeBytes, consensusGroupIdList))) {

              @Override
              protected List<TConsensusGroupId> processResponseOfOneDataNode(
                  TDataNodeLocation dataNodeLocation,
                  List<TConsensusGroupId> consensusGroupIdList,
                  TCheckTimeSeriesExistenceResp response) {
                respList.add(response);
                List<TConsensusGroupId> failedRegionList = new ArrayList<>();
                if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  return failedRegionList;
                }

                if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
                  List<TSStatus> subStatus = response.getStatus().getSubStatus();
                  for (int i = 0; i < subStatus.size(); i++) {
                    if (subStatus.get(i).getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                        && subStatus.get(i).getCode()
                            != TSStatusCode.TIMESERIES_ALREADY_EXIST.getStatusCode()) {
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
                setFailure(
                    new ProcedureException(
                        new MetadataException(
                            String.format(
                                "Set template %s to %s failed when [check time series existence on DataNode] because "
                                    + "failed to check time series existence in all replicaset of schemaRegion %s. Failure nodes: %s",
                                templateName,
                                templateSetPath,
                                consensusGroupId.id,
                                dataNodeLocationSet))));
                interruptTask();
              }
            };
    regionTask.execute();
    if (isFailed()) {
      return;
    }

    for (TCheckTimeSeriesExistenceResp resp : respList) {
      if (resp.isExists()) {
        setFailure(new ProcedureException(new TemplateIncompatibleException(templateName, path)));
      }
    }
    setNextState(SetTemplateState.COMMIT_SET);
  }

  private void commitSetTemplate(ConfigNodeProcedureEnv env) {
    CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan =
        new CommitSetSchemaTemplatePlan(templateName, templateSetPath);
    TSStatus status;
    try {
      status =
          env.getConfigManager()
              .getConsensusManager()
              .write(
                  isGeneratedByPipe
                      ? new PipeEnrichedPlan(commitSetSchemaTemplatePlan)
                      : commitSetSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(SetTemplateState.COMMIT_RELEASE);
    } else {
      LOGGER.warn(
          "Failed to commit set template {} on path {} due to {}",
          templateName,
          templateSetPath,
          status.getMessage());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void commitReleaseTemplate(ConfigNodeProcedureEnv env) {
    Template template = getTemplate(env);
    if (template == null) {
      // already setFailure
      return;
    }

    TUpdateTemplateReq req = new TUpdateTemplateReq();
    req.setType(TemplateInternalRPCUpdateType.COMMIT_TEMPLATE_SET_INFO.toByte());
    req.setTemplateInfo(
        TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template, templateSetPath));

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<TUpdateTemplateReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.UPDATE_TEMPLATE, req, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Failed to sync template {} commit-set info on path {} to DataNode {}",
            templateName,
            templateSetPath,
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(
            new ProcedureException(
                new MetadataException(
                    String.format(
                        "Failed to set schemaengine template %s on path %s because there's failure on DataNode %s",
                        templateName, templateSetPath, dataNodeLocationMap.get(entry.getKey())))));
        return;
      }
    }
  }

  private void submitTemplateMaintainTask(TDataNodeLocation dataNodeLocation) {
    // todo implement async retry

  }

  @Override
  protected boolean isRollbackSupported(SetTemplateState setTemplateState) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, SetTemplateState state)
      throws IOException, InterruptedException, ProcedureException {
    long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case PRE_SET:
          LOGGER.info(
              "Start rollback pre set schemaengine template {} on path {}",
              templateName,
              templateSetPath);
          rollbackPreSet(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              "Start rollback pre release schemaengine template {} on path {}",
              templateName,
              templateSetPath);
          rollbackPreRelease(env);
          break;
        case COMMIT_SET:
          LOGGER.info(
              "Start rollback commit set schemaengine template {} on path {}",
              templateName,
              templateSetPath);
          rollbackCommitSet(env);
          break;
      }
    } finally {
      LOGGER.info(
          "Rollback SetTemplate-{} costs {}ms.", state, (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackPreSet(ConfigNodeProcedureEnv env) {
    PreSetSchemaTemplatePlan preSetSchemaTemplatePlan =
        new PreSetSchemaTemplatePlan(templateName, templateSetPath, true);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(preSetSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to rollback pre set template {} on path {} due to {}",
          templateName,
          templateSetPath,
          status.getMessage());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  private void rollbackPreRelease(ConfigNodeProcedureEnv env) {
    Template template = getTemplate(env);
    if (template == null) {
      // already setFailure
      return;
    }

    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();

    TUpdateTemplateReq invalidateTemplateSetInfoReq = new TUpdateTemplateReq();
    invalidateTemplateSetInfoReq.setType(
        TemplateInternalRPCUpdateType.INVALIDATE_TEMPLATE_SET_INFO.toByte());
    invalidateTemplateSetInfoReq.setTemplateInfo(
        TemplateInternalRPCUtil.generateInvalidateTemplateSetInfoBytes(
            template.getId(), templateSetPath));

    DataNodeAsyncRequestContext<TUpdateTemplateReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnRequestType.UPDATE_TEMPLATE, invalidateTemplateSetInfoReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    Map<Integer, TSStatus> statusMap = clientHandler.getResponseMap();
    for (Map.Entry<Integer, TSStatus> entry : statusMap.entrySet()) {
      // all dataNodes must clear the related template cache
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            "Failed to rollback pre release template info of template {} set on path {} on DataNode {}",
            template.getName(),
            templateSetPath,
            dataNodeLocationMap.get(entry.getKey()));
        setFailure(
            new ProcedureException(new MetadataException("Rollback pre release template failed")));
      }
    }
  }

  private void rollbackCommitSet(ConfigNodeProcedureEnv env) {
    CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan =
        new CommitSetSchemaTemplatePlan(templateName, templateSetPath, true);
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(commitSetSchemaTemplatePlan);
    } catch (ConsensusException e) {
      LOGGER.warn(CONSENSUS_WRITE_ERROR, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "Failed to rollback commit set template {} on path {} due to {}",
          templateName,
          templateSetPath,
          status.getMessage());
      setFailure(new ProcedureException(new IoTDBException(status.getMessage(), status.getCode())));
    }
  }

  @Override
  protected SetTemplateState getState(int stateId) {
    return SetTemplateState.values()[stateId];
  }

  @Override
  protected int getStateId(SetTemplateState state) {
    return state.ordinal();
  }

  @Override
  protected SetTemplateState getInitialState() {
    return SetTemplateState.VALIDATE_TEMPLATE_EXISTENCE;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getTemplateName() {
    return templateName;
  }

  public String getTemplateSetPath() {
    return templateSetPath;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_TEMPLATE_PROCEDURE.getTypeCode()
            : ProcedureType.SET_TEMPLATE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(queryId, stream);
    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(templateSetPath, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    queryId = ReadWriteIOUtils.readString(byteBuffer);
    templateName = ReadWriteIOUtils.readString(byteBuffer);
    templateSetPath = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetTemplateProcedure that = (SetTemplateProcedure) o;
    return Objects.equals(getProcId(), that.getProcId())
        && Objects.equals(getCurrentState(), that.getCurrentState())
        && Objects.equals(getCycles(), that.getCycles())
        && Objects.equals(isGeneratedByPipe, that.isGeneratedByPipe)
        && Objects.equals(templateName, that.templateName)
        && Objects.equals(templateSetPath, that.templateSetPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProcId(),
        getCurrentState(),
        getCycles(),
        isGeneratedByPipe,
        templateName,
        templateSetPath);
  }
}
