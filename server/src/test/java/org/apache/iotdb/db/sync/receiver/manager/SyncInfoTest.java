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
package org.apache.iotdb.db.sync.receiver.manager;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.sync.PipeException;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.common.SyncInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeMessage;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SyncInfoTest {
  private static final String pipe1 = "pipe1";
  private static final String pipe2 = "pipe2";
  private static final long createdTime1 = System.currentTimeMillis();
  private static final long createdTime2 = System.currentTimeMillis() + 1;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testOperatePipe() throws Exception {
    SyncInfo syncInfo = new SyncInfo();
    try {
      CreatePipeSinkPlan createPipeSinkPlan = new CreatePipeSinkPlan("demo", "iotdb");
      createPipeSinkPlan.addPipeSinkAttribute("ip", "127.0.0.1");
      createPipeSinkPlan.addPipeSinkAttribute("port", "6670");
      try {
        syncInfo.addPipe(new CreatePipePlan(pipe1, "demo"), createdTime1);
        Assert.fail();
      } catch (PipeException e) {
        // throw exception because can not find pipeSink
      }
      syncInfo.addPipeSink(createPipeSinkPlan);
      syncInfo.addPipe(new CreatePipePlan(pipe1, "demo"), createdTime1);
      try {
        syncInfo.addPipe(new CreatePipePlan(pipe2, "demo"), createdTime2);
        Assert.fail();
      } catch (PipeException e) {
        // throw exception because only one pipe is allowed now
      }
      syncInfo.operatePipe(pipe1, StatementType.DROP_PIPE);
      syncInfo.addPipe(new CreatePipePlan(pipe2, "demo"), createdTime2);
      syncInfo.operatePipe(pipe2, StatementType.STOP_PIPE);
      syncInfo.operatePipe(pipe2, StatementType.START_PIPE);
      Assert.assertEquals(2, syncInfo.getAllPipeInfos().size());
      Assert.assertEquals(1, syncInfo.getAllPipeSink().size());
      PipeMessage info = new PipeMessage(PipeMessage.MsgType.INFO, "info");
      PipeMessage warn = new PipeMessage(PipeMessage.MsgType.WARN, "warn");
      PipeMessage error = new PipeMessage(PipeMessage.MsgType.ERROR, "error");
      syncInfo.writePipeMessage(pipe2, createdTime2, info);
      syncInfo.writePipeMessage(pipe2, createdTime2, warn);
      List<PipeMessage> messages = syncInfo.getPipeMessages(pipe2, createdTime2, true);
      Assert.assertEquals(2, messages.size());
      Assert.assertEquals(info, messages.get(0));
      Assert.assertEquals(warn, messages.get(1));
      syncInfo.writePipeMessage(pipe2, createdTime2, error);
      messages = syncInfo.getPipeMessages(pipe2, createdTime2, true);
      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(error, messages.get(0));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      syncInfo.close();
    }
  }
}
