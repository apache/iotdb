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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.entity.SimpleSTMProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestSTMProcedure extends TestProcedureBase {

  @Test
  public void testSubmitProcedure() {
    SimpleSTMProcedure stmProcedure = new SimpleSTMProcedure();
    long procId = this.procExecutor.submitProcedure(stmProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    Assert.assertEquals(acc.get(), 10);
  }

  @Test
  public void testRolledBackProcedure() {
    SimpleSTMProcedure stmProcedure = new SimpleSTMProcedure();
    stmProcedure.throwAtIndex = 4;
    long procId = this.procExecutor.submitProcedure(stmProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    int success = env.successCount.get();
    int rolledback = env.rolledBackCount.get();
    System.out.println(acc.get());
    System.out.println(success);
    System.out.println(rolledback);
    Assert.assertEquals(1 + success - rolledback, acc.get());
  }
}
