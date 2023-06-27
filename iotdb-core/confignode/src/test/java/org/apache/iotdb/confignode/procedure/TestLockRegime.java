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

import org.apache.iotdb.confignode.procedure.entity.SimpleLockProcedure;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestLockRegime extends TestProcedureBase {

  @Test
  public void testAcquireLock() {
    List<Long> procIdList = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      String procName = "[proc" + i + "]";
      SimpleLockProcedure stmProcedure = new SimpleLockProcedure(procName);
      long procId = this.procExecutor.submitProcedure(stmProcedure);
      procIdList.add(procId);
    }
    ProcedureTestUtil.waitForProcedure(
        this.procExecutor, procIdList.stream().mapToLong(Long::longValue).toArray());
    Assert.assertEquals(env.lockAcquireSeq.toString(), env.executeSeq.toString());
  }
}
