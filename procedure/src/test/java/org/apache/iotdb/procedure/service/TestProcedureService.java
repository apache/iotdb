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

package org.apache.iotdb.procedure.service;

import org.apache.iotdb.procedure.NoopProcedureStore;
import org.apache.iotdb.procedure.ProcedureExecutor;
import org.apache.iotdb.procedure.TestProcEnv;
import org.apache.iotdb.procedure.entity.IncProcedure;
import org.apache.iotdb.procedure.scheduler.ProcedureScheduler;
import org.apache.iotdb.procedure.scheduler.SimpleProcedureScheduler;
import org.apache.iotdb.procedure.store.IProcedureStore;
import org.apache.iotdb.procedure.util.ProcedureTestUtil;
import org.apache.iotdb.service.rpc.thrift.SubmitProcedureReq;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestProcedureService {

  private int bufferSize = 16 * 1024 * 1024;

  ProcedureExecutor executor;
  ProcedureServerProcessor client;
  TestProcEnv env;
  ProcedureScheduler scheduler;
  IProcedureStore store;

  @Before
  public void setUp() {
    env = new TestProcEnv();
    scheduler = new SimpleProcedureScheduler();
    store = new NoopProcedureStore();
    executor = new ProcedureExecutor(env, store, scheduler);
    client = new ProcedureServerProcessor(executor);
    executor.init(4);
    store.start();
    scheduler.start();
    executor.startWorkers();
  }

  @Test
  public void testSubmitAndQuery() throws IOException, TException {
    IncProcedure inc = new IncProcedure();
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    inc.serialize(byteBuffer);
    byteBuffer.flip();
    byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
    byteBuffer.get(bytes);
    SubmitProcedureReq submitProcedureReq = new SubmitProcedureReq();
    submitProcedureReq.setProcedureBody(bytes);
    byteBuffer.flip();
    long procId = client.submit(submitProcedureReq);
    ProcedureTestUtil.waitForProcedure(executor, procId);
    String query = client.query(procId);
    Assert.assertTrue(query.contains("pid=1"));
  }

  @After
  public void tearDown() {
    executor.stop();
    store.stop();
    scheduler.stop();
  }
}
