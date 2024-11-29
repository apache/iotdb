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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterDatabaseState;

import java.io.IOException;

public class AlterDatabaseProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterDatabaseState> {

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final AlterDatabaseState alterDatabaseState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      final ConfigNodeProcedureEnv env, final AlterDatabaseState alterDatabaseState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected AlterDatabaseState getState(final int stateId) {
    return null;
  }

  @Override
  protected int getStateId(final AlterDatabaseState alterDatabaseState) {
    return 0;
  }

  @Override
  protected AlterDatabaseState getInitialState() {
    return null;
  }
}
