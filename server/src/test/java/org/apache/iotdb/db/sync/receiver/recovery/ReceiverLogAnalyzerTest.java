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
package org.apache.iotdb.db.sync.receiver.recovery;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.conf.SyncPathUtil;
import org.apache.iotdb.db.sync.receiver.manager.PipeMessage;
import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** This test is for ReceiverLog and ReceiverLogAnalyzer */
public class ReceiverLogAnalyzerTest {

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
  public void testServiceLog() {
    try {
      ReceiverLog log = new ReceiverLog();
      log.startPipeServer();
      log.createPipe(pipe1, ip1, 1);
      log.createPipe(pipe2, ip2, 2);
      log.createPipe(pipe1, ip2, 3);
      log.stopPipe(pipe1, ip1, 1);
      log.stopPipeServer();
      log.startPipeServer();
      log.stopPipe(pipe2, ip2, 2);
      log.dropPipe(pipe1, ip2, 3);
      log.startPipe(pipe1, ip1, 1);
      log.close();
      ReceiverLogAnalyzer receiverLogAnalyzer = new ReceiverLogAnalyzer();
      receiverLogAnalyzer.scan();
      Map<String, Map<String, Map<Long, PipeStatus>>> map = receiverLogAnalyzer.getPipeInfos();
      Assert.assertTrue(receiverLogAnalyzer.isPipeServerEnable());
      Assert.assertNotNull(map);
      Assert.assertEquals(2, map.get(pipe1).size());
      Assert.assertEquals(1, map.get(pipe2).size());
      Assert.assertEquals(1, map.get(pipe2).size());
      Assert.assertEquals(PipeStatus.STOP, map.get(pipe2).get(ip2).get(2L));
      Assert.assertEquals(PipeStatus.STOP, map.get(pipe1).get(ip1).get(1L));
      Assert.assertEquals(PipeStatus.DROP, map.get(pipe1).get(ip2).get(3L));
    } catch (Exception e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testMessageLog() {
    String pipeIdentifier1 = SyncPathUtil.getReceiverPipeDirName(pipe1, ip1, createdTime1);
    String pipeIdentifier2 = SyncPathUtil.getReceiverPipeDirName(pipe2, ip2, createdTime2);
    try {
      ReceiverLog log = new ReceiverLog();
      PipeMessage info = new PipeMessage(PipeMessage.MsgType.INFO, "info");
      PipeMessage warn = new PipeMessage(PipeMessage.MsgType.WARN, "warn");
      PipeMessage error = new PipeMessage(PipeMessage.MsgType.ERROR, "error");

      log.writePipeMsg(pipeIdentifier1, info);
      log.writePipeMsg(pipeIdentifier1, warn);
      log.comsumePipeMsg(pipeIdentifier1);
      log.writePipeMsg(pipeIdentifier1, error);
      log.writePipeMsg(pipeIdentifier1, info);
      log.writePipeMsg(pipeIdentifier1, warn);

      log.writePipeMsg(pipeIdentifier2, error);
      log.comsumePipeMsg(pipeIdentifier2);
      log.close();

      ReceiverLogAnalyzer receiverLogAnalyzer = new ReceiverLogAnalyzer();
      receiverLogAnalyzer.scan();
      Map<String, List<PipeMessage>> map = receiverLogAnalyzer.getPipeMessageMap();
      Assert.assertNotNull(map);
      Assert.assertEquals(3, map.get(pipeIdentifier1).size());
      Assert.assertNull(map.get(pipeIdentifier2));
      Assert.assertEquals(error, map.get(pipeIdentifier1).get(0));
      Assert.assertEquals(info, map.get(pipeIdentifier1).get(1));
      Assert.assertEquals(warn, map.get(pipeIdentifier1).get(2));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
