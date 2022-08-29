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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.TestProcedureBase;
import org.apache.iotdb.confignode.procedure.entity.IncProcedure;
import org.apache.iotdb.confignode.procedure.entity.StuckSTMProcedure;
import org.apache.iotdb.confignode.procedure.entity.TestProcedureFactory;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TestProcedureStore extends TestProcedureBase {

  private static final String TEST_DIR = "./target/testWAL/";
  private static final int WORK_THREAD = 2;
  private IProcedureFactory factory = new TestProcedureFactory();

  @Override
  protected void initExecutor() {
    this.env = new TestProcEnv();
    this.procStore = new ProcedureStore(TEST_DIR, factory);
    this.procExecutor = new ProcedureExecutor<>(env, procStore);
    this.env.setScheduler(this.procExecutor.getScheduler());
    this.procExecutor.init(WORK_THREAD);
  }

  @Test
  public void testUpdate() {
    ProcedureStore procedureStore = new ProcedureStore(TEST_DIR, factory);
    IncProcedure incProcedure = new IncProcedure();
    procedureStore.update(incProcedure);
    List<Procedure> procedureList = new ArrayList<>();
    procedureStore.load(procedureList);
    assertProc(
        incProcedure,
        procedureList.get(0).getClass(),
        procedureList.get(0).getProcId(),
        procedureList.get(0).getState());
    this.procStore.cleanup();
    try {
      FileUtils.cleanDirectory(new File(TEST_DIR));
    } catch (IOException e) {
      System.out.println("clean dir failed." + e);
    }
  }

  @Test
  public void testChildProcedureLoad() {
    int childCount = 10;
    StuckSTMProcedure STMProcedure = new StuckSTMProcedure(childCount);
    long rootId = procExecutor.submitProcedure(STMProcedure);
    ProcedureTestUtil.sleepWithoutInterrupt(50);
    // stop service
    ProcedureTestUtil.stopService(procExecutor, procExecutor.getScheduler(), procStore);
    ConcurrentHashMap<Long, Procedure> procedures = procExecutor.getProcedures();
    ProcedureStore procedureStore = new ProcedureStore(TEST_DIR, new TestProcedureFactory());
    List<Procedure> procedureList = new ArrayList<>();
    procedureStore.load(procedureList);
    Assert.assertEquals(childCount + 1, procedureList.size());
    for (int i = 0; i < procedureList.size(); i++) {
      Procedure procedure = procedureList.get(i);
      assertProc(
          procedure,
          procedures.get(procedure.getProcId()).getClass(),
          i + 1,
          procedures.get(procedure.getProcId()).getState());
    }
    // restart service
    initExecutor();
    this.procStore.start();
    this.procExecutor.startWorkers();

    ProcedureTestUtil.waitForProcedure(procExecutor, rootId);
    Assert.assertEquals(
        procExecutor.getResultOrProcedure(rootId).getState(), ProcedureState.SUCCESS);
  }

  private void assertProc(Procedure proc, Class clazz, long procId, ProcedureState state) {
    Assert.assertEquals(clazz, proc.getClass());
    Assert.assertEquals(procId, proc.getProcId());
    Assert.assertEquals(state, proc.getState());
  }
}
