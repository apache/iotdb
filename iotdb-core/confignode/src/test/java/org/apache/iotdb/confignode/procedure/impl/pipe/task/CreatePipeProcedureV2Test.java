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

package org.apache.iotdb.confignode.procedure.impl.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreatePipeProcedureV2Test {
  @Test
  public void serializeDeserializeTest() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();
    extractorAttributes.put("extractor", "iotdb-extractor");
    processorAttributes.put("processor", "do-nothing-processor");
    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("host", "127.0.0.1");
    connectorAttributes.put("port", "6667");

    CreatePipeProcedureV2 proc =
        new CreatePipeProcedureV2(
            new TCreatePipeReq()
                .setPipeName("testPipe")
                .setExtractorAttributes(extractorAttributes)
                .setProcessorAttributes(processorAttributes)
                .setConnectorAttributes(connectorAttributes));

    try {
      proc.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      CreatePipeProcedureV2 proc2 =
          (CreatePipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

      assertEquals(proc, proc2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testCheckAndEnrichSourceAuthenticationWithEncryptedPassword() {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final PermissionManager permissionManager = Mockito.mock(PermissionManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getPermissionManager()).thenReturn(permissionManager);

    final TPermissionInfoResp loginResp = new TPermissionInfoResp();
    loginResp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Mockito.when(permissionManager.login("user", "encrypted-password", true)).thenReturn(loginResp);

    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put("extractor", "iotdb-source");
    sourceAttributes.put("username", "user");
    sourceAttributes.put("password", "encrypted-password");

    CreatePipeProcedureV2.checkAndEnrichSourceAuthentication(env, sourceAttributes);

    assertEquals(
        "encrypted-password", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
    Mockito.verify(permissionManager).login("user", "encrypted-password", true);
    Mockito.verify(permissionManager, Mockito.never())
        .login4Pipe(Mockito.anyString(), Mockito.any());
  }

  @Test
  public void testCheckAndEnrichSourceAuthenticationFallsBackToRawPassword() {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final PermissionManager permissionManager = Mockito.mock(PermissionManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getPermissionManager()).thenReturn(permissionManager);

    final TPermissionInfoResp loginResp = new TPermissionInfoResp();
    loginResp.setStatus(new TSStatus(TSStatusCode.WRONG_LOGIN_PASSWORD.getStatusCode()));
    Mockito.when(permissionManager.login("user", "raw-password", true)).thenReturn(loginResp);
    Mockito.when(permissionManager.login4Pipe("user", "raw-password"))
        .thenReturn("hashed-password");

    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.SOURCE_KEY, "iotdb-source");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY, "user");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY, "raw-password");

    CreatePipeProcedureV2.checkAndEnrichSourceAuthentication(env, sourceAttributes);

    assertEquals(
        "hashed-password", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY));
    Mockito.verify(permissionManager).login("user", "raw-password", true);
    Mockito.verify(permissionManager).login4Pipe("user", "raw-password");
  }
}
