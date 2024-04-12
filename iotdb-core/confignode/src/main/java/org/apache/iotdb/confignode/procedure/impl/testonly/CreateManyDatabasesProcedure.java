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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This procedure will create numerous databases (perhaps 100), during which the confignode leader
 * should be externally shutdown to test whether the procedure can be correctly recovered after the
 * leader change. The procedure will never finish until it's recovered from another ConfigNode.
 */
@TestOnly
public class CreateManyDatabasesProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateManyDatabasesProcedure.class);
  public static final int INITIAL_STATE = 0;
  public static final int MAX_STATE = 100;
  public static final String DATABASE_NAME_PREFIX = "root.test_";
  public static final long SLEEP_FOREVER = Long.MAX_VALUE;
  private boolean createFailedOnce = false;
  private boolean isDeserialized = false;

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv configNodeProcedureEnv, Integer state)
      throws InterruptedException {
    if (state < MAX_STATE) {
      if (state == MAX_STATE - 1 && !isDeserialized) {
        Thread.sleep(SLEEP_FOREVER);
      }
      try {
        createDatabase(configNodeProcedureEnv, state);
      } catch (ProcedureException e) {
        setFailure(e);
        return Flow.NO_MORE_STATE;
      }
      setNextState(state + 1);
      return Flow.HAS_MORE_STATE;
    }
    return Flow.NO_MORE_STATE;
  }

  private void createDatabase(ConfigNodeProcedureEnv env, int id) throws ProcedureException {
    String databaseName = DATABASE_NAME_PREFIX + id;
    TSStatus status = ProcedureTestUtils.createDatabase(env.getConfigManager(), databaseName);
    if (TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode() == status.getCode()) {
      // First mistakes are forgivable, but a second signals a problem.
      if (!createFailedOnce) {
        createFailedOnce = true;
      } else {
        throw new ProcedureException("createDatabase fail twice");
      }
    } else if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
      throw new ProcedureException("Unexpected fail, tsStatus is " + status);
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
    return INITIAL_STATE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_MANY_DATABASES_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    isDeserialized = true;
  }
}
