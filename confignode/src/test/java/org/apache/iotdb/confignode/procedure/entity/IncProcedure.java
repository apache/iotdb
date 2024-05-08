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

package org.apache.iotdb.confignode.procedure.entity;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class IncProcedure extends Procedure<TestProcEnv> {

  public boolean throwEx = false;

  @Override
  protected Procedure<TestProcEnv>[] execute(TestProcEnv testProcEnv)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    AtomicInteger acc = testProcEnv.getAcc();
    if (throwEx) {
      throw new RuntimeException("throw a EXCEPTION");
    }
    acc.getAndIncrement();
    testProcEnv.successCount.getAndIncrement();
    return null;
  }

  @Override
  protected void rollback(TestProcEnv testProcEnv) throws IOException, InterruptedException {
    AtomicInteger acc = testProcEnv.getAcc();
    acc.getAndDecrement();
    testProcEnv.rolledBackCount.getAndIncrement();
  }

  @Override
  protected boolean abort(TestProcEnv testProcEnv) {
    return true;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(TestProcedureFactory.TestProcedureType.INC_PROCEDURE.ordinal());
    super.serialize(stream);
  }
}
