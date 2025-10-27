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

package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.AbstractOperatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.state.sync.OperatePipeState;

import java.io.IOException;

/**
 * Empty procedure for old sync, restored only for compatibility.
 *
 * @deprecated use {@link AbstractOperatePipeProcedureV2} instead.
 */
@Deprecated
abstract class AbstractOperatePipeProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, OperatePipeState> {

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, OperatePipeState state)
      throws InterruptedException {
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected OperatePipeState getState(int stateId) {
    return OperatePipeState.values()[stateId];
  }

  @Override
  protected int getStateId(OperatePipeState state) {
    return state.ordinal();
  }

  @Override
  protected OperatePipeState getInitialState() {
    return OperatePipeState.OPERATE_CHECK;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, OperatePipeState operatePipeState)
      throws IOException, InterruptedException, ProcedureException {
    // Do nothing
  }
}
