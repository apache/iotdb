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

package org.apache.iotdb.confignode.procedure.impl.testonly;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** This procedure will never finish. */
@TestOnly
public class NeverFinishProcedure extends StateMachineProcedure<ConfigNodeProcedureEnv, Integer> {
  public NeverFinishProcedure() {}

  public NeverFinishProcedure(long procId) {
    this.setProcId(procId);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, Integer state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    setNextState(state + 1);
    Thread.sleep(1000);
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv configNodeProcedureEnv, Integer state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected Integer getState(int stateId) {
    return stateId;
  }

  @Override
  protected int getStateId(Integer integer) {
    return integer;
  }

  @Override
  protected Integer getInitialState() {
    return 0;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.NEVER_FINISH_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return getProcId() == ((NeverFinishProcedure) o).getProcId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId());
  }
}
