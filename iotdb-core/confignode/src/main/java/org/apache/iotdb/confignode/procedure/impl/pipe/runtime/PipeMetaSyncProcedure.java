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

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Empty procedure for older version of pipe, restored only for compatibility.
 *
 * @deprecated use {@link PipeHandleMetaChangeProcedure} instead.
 */
@Deprecated
public class PipeMetaSyncProcedure extends AbstractOperatePipeProcedureV2 {

  public PipeMetaSyncProcedure() {
    super();
  }

  @Override
  protected PipeTaskOperation getOperation() {
    return PipeTaskOperation.SYNC_PIPE_META;
  }

  @Override
  protected void executeFromValidateTask(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void executeFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void executeFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void executeFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void rollbackFromValidateTask(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void rollbackFromCalculateInfoForTask(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void rollbackFromWriteConfigNodeConsensus(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  protected void rollbackFromOperateOnDataNodes(ConfigNodeProcedureEnv env) {
    // Empty method
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.PIPE_META_SYNC_PROCEDURE.getTypeCode());
    super.serialize(stream);
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
