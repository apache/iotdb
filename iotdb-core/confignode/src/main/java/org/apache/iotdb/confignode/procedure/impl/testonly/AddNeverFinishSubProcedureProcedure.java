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
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;

@TestOnly
public class AddNeverFinishSubProcedureProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, Integer> {
  public static final Logger LOGGER =
      LoggerFactory.getLogger(AddNeverFinishSubProcedureProcedure.class);
  public static final String FAIL_DATABASE_NAME = "root.fail";

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, Integer state)
      throws InterruptedException {
    if (state == 0) {
      // the sub procedure will never finish, so the father procedure should never be called again
      addChildProcedure(new NeverFinishProcedure());
      setNextState(1);
      return Flow.HAS_MORE_STATE;
    }
    if (state == 1) {
      // test fail
      LOGGER.error("AddNeverFinishSubProcedureProcedure run again, which should never happen");
      ProcedureTestUtils.createDatabase(env.getConfigManager(), FAIL_DATABASE_NAME);
    }
    return Flow.NO_MORE_STATE;
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

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE.getTypeCode());
    super.serialize(stream);
  }
}
