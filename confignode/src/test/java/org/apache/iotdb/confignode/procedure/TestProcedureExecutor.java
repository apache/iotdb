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

import org.apache.iotdb.confignode.procedure.entity.IncProcedure;
import org.apache.iotdb.confignode.procedure.entity.NoopProcedure;
import org.apache.iotdb.confignode.procedure.entity.StuckProcedure;
import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.util.ProcedureTestUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestProcedureExecutor extends TestProcedureBase {

  @Override
  protected void initExecutor() {
    this.env = new TestProcEnv();
    this.procStore = new NoopProcedureStore();
    this.procExecutor = new ProcedureExecutor<>(env, procStore);
    this.env.setScheduler(this.procExecutor.getScheduler());
    this.procExecutor.init(2);
  }

  @Test
  public void testSubmitProcedure() {
    IncProcedure incProcedure = new IncProcedure();
    long procId = this.procExecutor.submitProcedure(incProcedure);
    ProcedureTestUtil.waitForProcedure(this.procExecutor, procId);
    TestProcEnv env = this.getEnv();
    AtomicInteger acc = env.getAcc();
    Assert.assertEquals(acc.get(), 1);
  }

  @Test
  public void testWorkerThreadStuck() throws InterruptedException {
    Semaphore latch1 = new Semaphore(2);
    latch1.acquire(2);
    StuckProcedure busyProc1 = new StuckProcedure(latch1);

    Semaphore latch2 = new Semaphore(2);
    latch2.acquire(2);
    StuckProcedure busyProc2 = new StuckProcedure(latch2);

    long busyProcId1 = procExecutor.submitProcedure(busyProc1);
    long busyProcId2 = procExecutor.submitProcedure(busyProc2);
    long otherProcId = procExecutor.submitProcedure(new NoopProcedure());

    // wait until a new worker is being created
    int threads1 = waitThreadCount(3);
    LOG.info("new threads got created: " + (threads1 - 2));
    Assert.assertEquals(3, threads1);

    ProcedureTestUtil.waitForProcedure(procExecutor, otherProcId);
    Assert.assertEquals(true, procExecutor.isFinished(otherProcId));
    Assert.assertEquals(true, procExecutor.isRunning());
    Assert.assertEquals(false, procExecutor.isFinished(busyProcId1));
    Assert.assertEquals(false, procExecutor.isFinished(busyProcId2));

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    LOG.info("set keep alive and wait threads being removed");
    int threads2 = waitThreadCount(2);
    LOG.info("threads got removed: " + (threads1 - threads2));
    Assert.assertEquals(2, threads2);

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    // wait for all procs to complete
    ProcedureTestUtil.waitForProcedure(procExecutor, busyProcId1);
    ProcedureTestUtil.waitForProcedure(procExecutor, busyProcId2);
  }

  private int waitThreadCount(final int expectedThreads) {
    long startTime = System.currentTimeMillis();
    while (procExecutor.isRunning()
        && TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) <= 30) {
      if (procExecutor.getWorkerThreadCount() == expectedThreads) {
        break;
      }
      ProcedureTestUtil.sleepWithoutInterrupt(250);
    }
    return procExecutor.getWorkerThreadCount();
  }
}
