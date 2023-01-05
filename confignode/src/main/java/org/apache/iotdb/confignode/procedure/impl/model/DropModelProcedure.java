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

package org.apache.iotdb.confignode.procedure.impl.model;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.DropModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DropModelProcedure extends AbstractNodeProcedure<DropModelState> {

  private String modelId;

  public DropModelProcedure() {
    super();
  }

  public DropModelProcedure(String modelId) {
    super();
    this.modelId = modelId;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DropModelState dropModelState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, DropModelState dropModelState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected DropModelState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(DropModelState dropModelState) {
    return 0;
  }

  @Override
  protected DropModelState getInitialState() {
    return null;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(modelId, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelId = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof DropModelProcedure) {
      DropModelProcedure thatProc = (DropModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.modelId).equals(this.modelId);
    }
    return false;
  }
}
