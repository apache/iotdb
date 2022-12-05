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

import org.apache.iotdb.commons.exception.sync.PipeException;
import org.apache.iotdb.commons.exception.sync.PipeSinkNotExistException;
import org.apache.iotdb.commons.sync.pipe.PipeInfo;
import org.apache.iotdb.commons.sync.pipe.PipeMessage;
import org.apache.iotdb.commons.sync.pipe.SyncOperation;
import org.apache.iotdb.commons.sync.pipe.TsFilePipeInfo;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.plan.statement.sys.sync.CreatePipeSinkStatement;
import org.apache.iotdb.db.sync.common.LocalSyncInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalSyncInfoTest {
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
    LocalSyncInfo localSyncInfo = new LocalSyncInfo();
    try {
      CreatePipeSinkStatement createPipeSinkStatement = new CreatePipeSinkStatement();
      createPipeSinkStatement.setPipeSinkName("demo");
      createPipeSinkStatement.setPipeSinkType("IoTDB");
      Map<String, String> attributes = new HashMap<>();
      attributes.put("ip", "127.0.0.1");
      attributes.put("port", "6667");
      createPipeSinkStatement.setAttributes(attributes);
      try {
        localSyncInfo.addPipe(new TsFilePipeInfo(pipe1, "demo", createdTime1, 0, true));
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e instanceof PipeSinkNotExistException);
        // throw exception because can not find pipeSink
      }
      localSyncInfo.addPipeSink(createPipeSinkStatement);
      localSyncInfo.addPipe(new TsFilePipeInfo(pipe1, "demo", createdTime1, 0, true));
      localSyncInfo.addPipe(new TsFilePipeInfo(pipe2, "demo", createdTime2, 0, true));
      try {
        localSyncInfo.addPipe(new TsFilePipeInfo(pipe2, "demo", createdTime2, 0, true));
        Assert.fail();
      } catch (PipeException e) {
        // throw exception because pipe already exists
      }
      localSyncInfo.operatePipe(pipe2, SyncOperation.STOP_PIPE);
      localSyncInfo.operatePipe(pipe2, SyncOperation.START_PIPE);
      Assert.assertEquals(1, localSyncInfo.getAllPipeSink().size());
      Assert.assertEquals(2, localSyncInfo.getAllPipeInfos().size());
      localSyncInfo.changePipeMessage(pipe2, PipeMessage.PipeMessageType.WARN);
      localSyncInfo.changePipeMessage(pipe2, PipeMessage.PipeMessageType.NORMAL);
      PipeInfo pipeInfo1 = localSyncInfo.getPipeInfo(pipe2);
      Assert.assertEquals(PipeMessage.PipeMessageType.WARN, pipeInfo1.getMessageType());
      localSyncInfo.changePipeMessage(pipe2, PipeMessage.PipeMessageType.ERROR);
      PipeInfo pipeInfo2 = localSyncInfo.getPipeInfo(pipe2);
      Assert.assertEquals(PipeMessage.PipeMessageType.ERROR, pipeInfo2.getMessageType());
      localSyncInfo.operatePipe(pipe1, SyncOperation.DROP_PIPE);
      Assert.assertEquals(1, localSyncInfo.getAllPipeInfos().size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      localSyncInfo.close();
    }
  }
}
