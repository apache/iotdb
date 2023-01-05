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

import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.CreateModelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreateModelProcedure extends AbstractNodeProcedure<CreateModelState> {

  private ModelInformation modelInformation;

  public CreateModelProcedure() {
    super();
  }

  public CreateModelProcedure(ModelInformation modelInformation) {
    super();
    this.modelInformation = modelInformation;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, CreateModelState createModelState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, CreateModelState createModelState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected CreateModelState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(CreateModelState createModelState) {
    return 0;
  }

  @Override
  protected CreateModelState getInitialState() {
    return null;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_MODEL_PROCEDURE.getTypeCode());
    super.serialize(stream);
    modelInformation.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    modelInformation = ModelInformation.deserialize(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof CreateModelProcedure) {
      CreateModelProcedure thatProc = (CreateModelProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && thatProc.modelInformation.equals(this.modelInformation);
    }
    return false;
  }
}
