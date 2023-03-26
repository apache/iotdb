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

package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.pipe.task.meta.DataRegionPipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePipeOnDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class CreatePipeProcedureV2 extends AbstractOperatePipeProcedureV2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreatePipeProcedureV2.class);

  private TCreatePipeReq req;
  private Map<Integer, Integer> regionGroupToLeaderMap = new HashMap<>();
  // TODO: Will there be a regionGroupToVersionMap?

  public CreatePipeProcedureV2() {
    super();
  }

  public CreatePipeProcedureV2(TCreatePipeReq req) throws PipeException {
    super();
    this.req = req;
  }

  @Override
  boolean validateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to validate PIPE [{}]", req.getPipeName());
    return env.getConfigManager().getPipeManager().getPipeInfo().checkPipeCreateTask(req);
  }

  @Override
  void calculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to calculate PIPE [{}] information on Config Nodes", req.getPipeName());
    // TODO: Get the regionMap after the interface is implemented
    if (this.regionGroupToLeaderMap.isEmpty()) {
      throw new PipeManagementException(
          "Failed to calculate the region map for pipe " + req.getPipeName() + ".");
    }
  }

  @Override
  void operateOnDataNodes(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to broadcast create PIPE [{}] on Data Nodes", req.getPipeName());

    TCreatePipeOnDataNodeReq request =
        new TCreatePipeOnDataNodeReq()
            .setPipeName(req.getPipeName())
            .setCollectorAttributes(req.getCollectorAttributes())
            .setProcessorAttributes(req.getProcessorAttributes())
            .setConnectorAttributes(req.getConnectorAttributes())
            .setRegionGroupToLeaderMap(regionGroupToLeaderMap);
    if (RpcUtils.squashResponseStatusList(env.createPipeOnDataNodes(request)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format("Failed to create pipe instance [%s] on data nodes", req.getPipeName()));
    }
  }

  @Override
  void writeConfigNodeConsensus(ConfigNodeProcedureEnv env) throws PipeManagementException {
    LOGGER.info("Start to create PIPE [{}] on Config Nodes", req.getPipeName());
    final ConfigManager configNodeManager = env.getConfigManager();

    final CreatePipePlanV2 createPipePlanV2 = new CreatePipePlanV2(parseInfoToPipeTaskMeta());

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(createPipePlanV2);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  private PipeTaskMeta parseInfoToPipeTaskMeta() {
    Map<Integer, DataRegionPipeTask> dataRegionPipeTasks = new HashMap<>();
    // TODO: Record the index and version of each newborn DataRegionPipeTask
    regionGroupToLeaderMap.forEach(
        (region, leader) -> {
          dataRegionPipeTasks.put(region, new DataRegionPipeTask(0, 0, region, leader));
        });
    return new PipeTaskMeta(
        req.getPipeName(),
        System.currentTimeMillis(),
        PipeStatus.STOP,
        req.getCollectorAttributes(),
        req.getProcessorAttributes(),
        req.getConnectorAttributes(),
        dataRegionPipeTasks);
  }

  @Override
  SyncOperation getOperation() {
    return SyncOperation.CREATE_PIPE;
  }

  @Override
  protected boolean isRollbackSupported(OperatePipeState state) {
    return true;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws IOException, InterruptedException, ProcedureException {
    LOGGER.info("Roll back CreatePipeProcedure at STATE [{}]", state);
    switch (state) {
      case VALIDATE_TASK:
        rollbackFromValidateTask(env);
        break;
      case CALCULATE_INFO_FOR_TASK:
        rollbackFromCalculateInfoForTask(env);
        break;
      case OPERATE_ON_DATA_NODES:
        rollbackFromOperateOnDataNodes(env);
        break;
      case WRITE_CONFIG_NODE_CONSENSUS:
        rollbackFromWriteConfigNodeConsensus(env);
        break;
      default:
        LOGGER.error("Unsupported roll back STATE [{}]", state);
    }
  }

  private void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to rollback from validate task [{}]", req.getPipeName());
    env.getConfigManager().getPipeManager().unlockPipeTaskInfo();
  }

  private void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to rollback from calculate info for task [{}]", req.getPipeName());
    // Do nothing
  }

  private void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("Start to rollback from operate on data nodes for task [{}]", req.getPipeName());

    TOperatePipeOnDataNodeReq request =
        new TOperatePipeOnDataNodeReq()
            .setPipeName(req.getPipeName())
            .setOperation((byte) SyncOperation.DROP_PIPE.ordinal());
    if (RpcUtils.squashResponseStatusList(env.operatePipeOnDataNodes(request)).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeManagementException(
          String.format(
              "Failed to rollback from operate on data nodes for task [%s]", req.getPipeName()));
    }
  }

  private void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info(
        "Start to rollback from write config node consensus for task [{}]", req.getPipeName());

    // Drop pipe
    final ConfigManager configNodeManager = env.getConfigManager();

    final DropPipePlanV2 dropPipePlanV2 = new DropPipePlanV2(req.getPipeName());

    final ConsensusWriteResponse response =
        configNodeManager.getConsensusManager().write(dropPipePlanV2);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_PIPE_PROCEDURE_V2.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(req.getPipeName(), stream);
    stream.writeInt(req.getCollectorAttributesSize());
    for (Map.Entry<String, String> entry : req.getCollectorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    stream.writeInt(req.getProcessorAttributesSize());
    for (Map.Entry<String, String> entry : req.getProcessorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    stream.writeInt(req.getConnectorAttributesSize());
    for (Map.Entry<String, String> entry : req.getConnectorAttributes().entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
    stream.writeInt(regionGroupToLeaderMap.size());
    for (Map.Entry<Integer, Integer> entry : regionGroupToLeaderMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    req.pipeName = ReadWriteIOUtils.readString(byteBuffer);
    int size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      req.getCollectorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      req.getProcessorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      req.getConnectorAttributes()
          .put(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
    }
    size = byteBuffer.getInt();
    for (int i = 0; i < size; ++i) {
      regionGroupToLeaderMap.put(
          ReadWriteIOUtils.readInt(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreatePipeProcedureV2 that = (CreatePipeProcedureV2) o;
    return req.equals(that.req) && this.regionGroupToLeaderMap.equals(that.regionGroupToLeaderMap);
  }

  @Override
  public int hashCode() {
    return req.getPipeName().hashCode();
  }
}
