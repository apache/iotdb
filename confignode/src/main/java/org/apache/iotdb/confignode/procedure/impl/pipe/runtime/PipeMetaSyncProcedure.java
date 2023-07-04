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

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class PipeMetaSyncProcedure extends AbstractOperatePipeProcedureV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMetaSyncProcedure.class);

  public PipeMetaSyncProcedure() {
    super();
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.SYNC_PIPE_META;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: executeFromValidateTask");

    // do nothing
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: executeFromCalculateInfoForTask");

    // do nothing
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: executeFromWriteConfigNodeConsensus");

    // do nothing
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) throws IOException {
    LOGGER.info("PipeMetaSyncProcedure: executeFromOperateOnDataNodes");

    Map<Integer, TPushPipeMetaResp> respMap = pushPipeMetaToDataNodes(env);
    if (env.getConfigManager()
        .getPipeManager()
        .getPipeTaskCoordinator()
        .getPipeTaskInfo()
        .recordPushPipeMetaExceptions(respMap)) {
      throw new PipeException(
          String.format(
              "Failed to push pipe meta to dataNodes, details: %s",
              parsePushPipeMetaExceptionForPipe(null, respMap)));
    }
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: rollbackFromValidateTask");

    // do nothing
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: rollbackFromCalculateInfoForTask");

    // do nothing
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: rollbackFromWriteConfigNodeConsensus");

    // do nothing
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    LOGGER.info("PipeMetaSyncProcedure: rollbackFromOperateOnDataNodes");

    // do nothing
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_META_SYNC_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof PipeMetaSyncProcedure;
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
