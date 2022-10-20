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
package org.apache.iotdb.db.sync.persistence;

import org.apache.iotdb.commons.sync.persistence.SyncLogReader;
import org.apache.iotdb.commons.sync.persistence.SyncLogWriter;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.SyncTestUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** This test is for ReceiverLog and ReceiverLogAnalyzer */
public class SyncLogTest {

  private static final String pipe1 = "pipe1";
  private static final String pipe2 = "pipe2";
  private static final String pipe3 = "pipe3";
  private static final long createdTime1 = System.currentTimeMillis();
  private static final long createdTime2 = System.currentTimeMillis() + 1;
  private static final long createdTime3 = System.currentTimeMillis() + 2;

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
      SyncLogWriter log = new SyncLogWriter(new File(SyncPathUtil.getSysDir()));
      PipeSink pipeSink = new IoTDBPipeSink("demo");
      Map<String, String> attributes = new HashMap<>();
      attributes.put("ip", "192.168.11.11");
      attributes.put("port", "7766");
      pipeSink.setAttribute(attributes);
      log.addPipeSink(pipeSink);
      PipeInfo pipeInfo1 = new TsFilePipeInfo(pipe1, "demo", createdTime1, 0, true);
      PipeInfo pipeInfo2 = new TsFilePipeInfo(pipe2, "demo", createdTime2, 99, false);
      PipeInfo pipeInfo3 = new TsFilePipeInfo(pipe3, "demo", createdTime3, 199, true);
      log.addPipe(pipeInfo1);
      log.operatePipe(pipe1, SyncOperation.DROP_PIPE);

      log.addPipe(pipeInfo2);
      log.operatePipe(pipe2, SyncOperation.STOP_PIPE);
      log.operatePipe(pipe2, SyncOperation.START_PIPE);

      log.addPipe(pipeInfo3);
      log.close();
      SyncLogReader syncLogReader = new SyncLogReader(new File(SyncPathUtil.getSysDir()));

      syncLogReader.recover();

      // check PipeSink
      Map<String, PipeSink> allPipeSinks = syncLogReader.getAllPipeSinks();
      Assert.assertEquals(1, allPipeSinks.size());

      // check Pipe
      Map<String, PipeInfo> pipeInfoMap = syncLogReader.getPipes();
      Assert.assertEquals(2, pipeInfoMap.size());
      PipeInfo pipeInfoRecover1 = pipeInfoMap.get(pipe1);
      Assert.assertNull(pipeInfoRecover1);
      PipeInfo pipeInfoRecover2 = pipeInfoMap.get(pipe2);
      SyncTestUtils.checkPipeInfo(
          pipeInfoRecover2,
          pipe2,
          "demo",
          PipeStatus.RUNNING,
          createdTime2,
          PipeMessage.PipeMessageType.NORMAL);
      PipeInfo pipeInfoRecover3 = pipeInfoMap.get(pipe3);
      SyncTestUtils.checkPipeInfo(
          pipeInfoRecover3,
          pipe3,
          "demo",
          PipeStatus.STOP,
          createdTime3,
          PipeMessage.PipeMessageType.NORMAL);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
