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
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.CreatePipePlan;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.common.AbstractSyncInfo;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AbstractSyncInfoTest {
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
    MockSyncInfo syncInfo = new MockSyncInfo();
    try {
      syncInfo.startServer();
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
      syncInfo.operatePipe(pipe1, Operator.OperatorType.DROP_PIPE);
      syncInfo.addPipe(new CreatePipePlan(pipe2, "demo"), createdTime2);
      syncInfo.operatePipe(pipe2, Operator.OperatorType.STOP_PIPE);
      syncInfo.operatePipe(pipe2, Operator.OperatorType.START_PIPE);
      List<String> actualRecords = syncInfo.getOperateRecord();
      String[] expectedRecords =
          new String[] {
            String.format("%s-%d-%s", pipe1, createdTime1, PipeStatus.DROP.name()),
            String.format("%s-%d-%s", pipe2, createdTime2, PipeStatus.STOP.name()),
            String.format("%s-%d-%s", pipe2, createdTime2, PipeStatus.RUNNING.name())
          };
      Assert.assertEquals(expectedRecords.length, actualRecords.size());
      for (int i = 0; i < expectedRecords.length; i++) {
        Assert.assertEquals(expectedRecords[i], actualRecords.get(i));
      }
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

  private class MockSyncInfo extends AbstractSyncInfo {
    private final List<String> operateRecord = new ArrayList<>();

    public List<String> getOperateRecord() {
      return operateRecord;
    }

    @Override
    protected void afterStartPipe(String pipeName, long createTime) {
      operateRecord.add(String.format("%s-%d-%s", pipeName, createTime, PipeStatus.RUNNING.name()));
    }

    @Override
    protected void afterStopPipe(String pipeName, long createTime) {
      operateRecord.add(String.format("%s-%d-%s", pipeName, createTime, PipeStatus.STOP.name()));
    }

    @Override
    protected void afterDropPipe(String pipeName, long createTime) {
      operateRecord.add(String.format("%s-%d-%s", pipeName, createTime, PipeStatus.DROP.name()));
    }
  }
}
