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

package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;

public class CreateManyDatabasesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateManyDatabasesProcedure.class);
  public static final int MAX_STATE = 100;
  public static final String DATABASE_NAME_PREFIX = "root.test_";
  public static final int SLEEP_INTERVAL = 1000;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv configNodeProcedureEnv, Integer state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (state < MAX_STATE) {
      createDatabase(configNodeProcedureEnv, state);
      setNextState(state + 1);
      return Flow.HAS_MORE_STATE;
    }
    return Flow.NO_MORE_STATE;
  }

  private void createDatabase(ConfigNodeProcedureEnv env, int id) {
    String databaseName = DATABASE_NAME_PREFIX + id;
    TDatabaseSchema databaseSchema = new TDatabaseSchema(databaseName);
    env.getConfigManager()
        .setDatabase(new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, databaseSchema));
    if (!isDeserialized()) {
      ProcedureManager.sleepWithoutInterrupt(SLEEP_INTERVAL);
    }
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv configNodeProcedureEnv, Integer integer)
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
    return getInitialStateStatic();
  }

  public static Integer getInitialStateStatic() {
    return 0;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_MANY_DATABASES_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }
}
