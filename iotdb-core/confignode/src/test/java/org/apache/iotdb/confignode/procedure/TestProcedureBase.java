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

import org.apache.iotdb.confignode.procedure.env.TestProcEnv;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProcedureBase {

  public static final Logger LOG = LoggerFactory.getLogger(TestProcedureBase.class);

  protected TestProcEnv env;
  protected IProcedureStore procStore;
  protected ProcedureExecutor<TestProcEnv> procExecutor;

  @Before
  public void setUp() {
    initExecutor();
    this.procStore.start();
    this.procExecutor.startWorkers();
  }

  @After
  public void tearDown() {
    this.procStore.stop();
    this.procExecutor.stop();
    this.procExecutor.join();
    this.procStore.cleanup();
  }

  protected void initExecutor() {
    this.env = new TestProcEnv();
    this.procStore = new NoopProcedureStore();
    this.procExecutor = new ProcedureExecutor<>(env, procStore);
    this.env.setScheduler(this.procExecutor.getScheduler());
    this.procExecutor.init(4);
  }

  public TestProcEnv getEnv() {
    return env;
  }

  public void setEnv(TestProcEnv env) {
    this.env = env;
  }

  public IProcedureStore getProcStore() {
    return procStore;
  }

  public void setProcStore(IProcedureStore procStore) {
    this.procStore = procStore;
  }

  public ProcedureExecutor<TestProcEnv> getProcExecutor() {
    return procExecutor;
  }

  public void setProcExecutor(ProcedureExecutor<TestProcEnv> procExecutor) {
    this.procExecutor = procExecutor;
  }
}
