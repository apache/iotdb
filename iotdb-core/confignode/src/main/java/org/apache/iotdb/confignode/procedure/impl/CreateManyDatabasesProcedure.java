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

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.statemachine.StateMachineProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateManyDatabasesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateManyDatabasesProcedure.class);
  private static int maxState = 10;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv configNodeProcedureEnv, Integer state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (state < maxState) {
      createDatabase(state);
      setNextState(state + 1);
      return Flow.HAS_MORE_STATE;
    }
    return Flow.NO_MORE_STATE;
  }

  private void createDatabase(int id) {
    String databaseName = "test_" + id;
    TDatabaseSchema databaseSchema = new TDatabaseSchema(databaseName);
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      configNodeClient.setDatabase(databaseSchema);
    } catch (ClientManagerException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      LOGGER.info("create database {} fail: ", databaseName, e);
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
    return 0;
  }
}
