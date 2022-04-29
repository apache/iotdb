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
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ReceiverManagerTest {
  private static final String pipe1 = "pipe1";
  private static final String pipe2 = "pipe2";
  private static final String ip1 = "192.168.1.11";
  private static final String ip2 = "192.168.2.22";
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
  public void test() {
    try {
      ReceiverManager manager = ReceiverManager.getInstance();
      manager.startServer();
      manager.createPipe(pipe1, ip1, 1);
      manager.createPipe(pipe2, ip2, 2);
      manager.createPipe(pipe1, ip2, 3);
      manager.stopPipe(pipe1, ip1, 1);
      manager.stopPipe(pipe2, ip2, 2);
      manager.dropPipe(pipe1, ip2, 3);
      manager.startPipe(pipe1, ip1, 1);
      List<PipeInfo> allPipeInfos = manager.getAllPipeInfos();
      Assert.assertEquals(3, allPipeInfos.size());
      List<PipeInfo> pipeInfos1 = manager.getPipeInfosByPipeName(pipe1);
      List<PipeInfo> pipeInfos2 = manager.getPipeInfosByPipeName(pipe2);
      Assert.assertEquals(2, pipeInfos1.size());
      Assert.assertEquals(1, pipeInfos2.size());
      for (PipeInfo pipeInfo : pipeInfos2) {
        Assert.assertEquals(new PipeInfo(pipe2, ip2, PipeStatus.STOP, 2), pipeInfo);
      }
      for (PipeInfo pipeInfo : pipeInfos1) {
        if (pipeInfo.getRemoteIp().equals(ip1)) {
          Assert.assertEquals(new PipeInfo(pipe1, ip1, PipeStatus.RUNNING, 1), pipeInfo);
        } else {
          Assert.assertEquals(new PipeInfo(pipe1, ip2, PipeStatus.DROP, 3), pipeInfo);
        }
      }
      PipeMessage info = new PipeMessage(PipeMessage.MsgType.INFO, "info");
      PipeMessage warn = new PipeMessage(PipeMessage.MsgType.WARN, "warn");
      PipeMessage error = new PipeMessage(PipeMessage.MsgType.ERROR, "error");
      manager.writePipeMessage(pipe1, ip1, createdTime1, info);
      manager.writePipeMessage(pipe1, ip1, createdTime1, warn);
      List<PipeMessage> messages = manager.getPipeMessages(pipe1, ip1, createdTime1, true);
      Assert.assertEquals(2, messages.size());
      Assert.assertEquals(info, messages.get(0));
      Assert.assertEquals(warn, messages.get(1));
      manager.writePipeMessage(pipe1, ip1, createdTime1, error);
      messages = manager.getPipeMessages(pipe1, ip1, createdTime1, true);
      Assert.assertEquals(1, messages.size());
      Assert.assertEquals(error, messages.get(0));
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
