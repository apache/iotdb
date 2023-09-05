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
package org.apache.iotdb.confignode.procedure.impl;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.cq.CQManager;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateCQProcedureTest {

  @Test
  public void serializeDeserializeTest() {

    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    String sql = "create cq testCq1 BEGIN select s1 into root.backup.d1(s1) from root.sg.d1 END";

    TCreateCQReq req =
        new TCreateCQReq(
            "testCq1",
            1000,
            0,
            1000,
            0,
            (byte) 0,
            "select s1 into root.backup.d1(s1) from root.sg.d1",
            sql,
            "Asia",
            "root");
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    CreateCQProcedure createCQProcedure1 = new CreateCQProcedure(req, executor);

    CQManager cqManager = Mockito.mock(CQManager.class);
    Mockito.when(cqManager.getExecutor()).thenReturn(executor);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    Mockito.when(configManager.getCQManager()).thenReturn(cqManager);
    ConfigNode configNode = ConfigNode.getInstance();
    configNode.setConfigManager(configManager);

    try {
      createCQProcedure1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      CreateCQProcedure createCQProcedure2 =
          (CreateCQProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(createCQProcedure1, createCQProcedure2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      executor.shutdown();
    }
  }
}
