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

package org.apache.iotdb.confignode.procedure.impl.pipe.runtime;

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.confignode.consensus.request.write.pipe.coordinator.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeHandleMetaChangeProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeHandleMetaChangeProcedure.class);

  private List<PipeMeta> newPipeMetaList = new ArrayList<>();
  private List<PipeMeta> oldPipeMetaList = new ArrayList<>();

  boolean needPushToDataNodes = false;

  public PipeHandleMetaChangeProcedure() {
    super();
  }

  public PipeHandleMetaChangeProcedure(
      List<PipeMeta> newPipeMetaList, List<PipeMeta> oldPipeMetaList, boolean needPushToDataNodes) {
    super();
    this.newPipeMetaList = newPipeMetaList;
    this.oldPipeMetaList = oldPipeMetaList;
    this.needPushToDataNodes = needPushToDataNodes;
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.HANDLE_PIPE_META_CHANGE;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromValidateTask");

    // do nothing
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) throws PipeException {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromCalculateInfoForTask");

    // do nothing
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env)
      throws PipeException {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromWriteConfigNodeConsensus");

    final PipeHandleMetaChangePlan plan = new PipeHandleMetaChangePlan(newPipeMetaList);
    final ConsensusWriteResponse response =
        env.getConfigManager().getConsensusManager().write(plan);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("PipeHandleMetaChangeProcedure: executeFromHandleOnDataNodes");

    if (needPushToDataNodes) {
      pushPipeMetaToDataNodes(env);
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromValidateTask");

    // do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromCalculateInfoForTask");

    // do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromWriteConfigNodeConsensus");

    final PipeHandleMetaChangePlan plan = new PipeHandleMetaChangePlan(oldPipeMetaList);
    final ConsensusWriteResponse response =
        env.getConfigManager().getConsensusManager().write(plan);
    if (!response.isSuccessful()) {
      throw new PipeManagementException(response.getErrorMessage());
    }
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("PipeHandleMetaChangeProcedure: rollbackFromOperateOnDataNodes");

    if (needPushToDataNodes) {
      // rollback to old pipe meta list
      List<PipeMeta> pipeMetaList =
          (List<PipeMeta>)
              env.getConfigManager()
                  .getPipeManager()
                  .getPipeTaskCoordinator()
                  .getPipeTaskInfo()
                  .getPipeMetaList();
      pipeMetaList.clear();
      pipeMetaList.addAll(oldPipeMetaList);

      pushPipeMetaToDataNodes(env);
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_HANDLE_META_CHANGE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(needPushToDataNodes, stream);
    ReadWriteIOUtils.write(newPipeMetaList.size(), stream);
    for (PipeMeta pipeMeta : newPipeMetaList) {
      pipeMeta.serialize(stream);
    }
    ReadWriteIOUtils.write(oldPipeMetaList.size(), stream);
    for (PipeMeta pipeMeta : oldPipeMetaList) {
      pipeMeta.serialize(stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    needPushToDataNodes = ReadWriteIOUtils.readBool(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(byteBuffer);
      newPipeMetaList.add(pipeMeta);
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final PipeMeta pipeMeta = PipeMeta.deserialize(byteBuffer);
      oldPipeMetaList.add(pipeMeta);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PipeHandleMetaChangeProcedure) {
      PipeHandleMetaChangeProcedure that = (PipeHandleMetaChangeProcedure) o;
      return this.newPipeMetaList.equals(that.newPipeMetaList)
          && this.oldPipeMetaList.equals(that.oldPipeMetaList)
          && this.needPushToDataNodes == that.needPushToDataNodes;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(newPipeMetaList, oldPipeMetaList, needPushToDataNodes);
  }
}
