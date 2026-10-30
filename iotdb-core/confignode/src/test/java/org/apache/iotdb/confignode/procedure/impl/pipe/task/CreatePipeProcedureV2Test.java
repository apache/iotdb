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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreatePipeProcedureV2Test {
  @Test
  public void testDataNodeConstructionFailureIsRecorded() throws Exception {
    final String pipeName = "failedHistoryPipe";
    final TCreatePipeReq request = createPipeRequest(pipeName);
    final PipeTaskInfo taskInfo = new PipeTaskInfo();
    final CreatePipeProcedureV2 procedure =
        new CreatePipeProcedureV2(request, new AtomicReference<>(taskInfo));
    procedure.executeFromCalculateInfoForTask(Mockito.mock(ConfigNodeProcedureEnv.class));
    taskInfo.createPipe(procedure.constructPlan());

    final String error = "Failed to construct PipeConnector";
    final TPushPipeMetaRespExceptionMessage exceptionMessage =
        new TPushPipeMetaRespExceptionMessage(request.getPipeName(), error, 1L)
            .setCreationTime(procedure.getPipeStaticMeta().getCreationTime());
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConsensusManager consensusManager = mockSuccessfulConsensusWrite(env);
    Mockito.when(env.pushSinglePipeMetaToDataNodes(Mockito.any(ByteBuffer.class), Mockito.any()))
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPushPipeMetaResp(
                        new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
                    .setExceptionMessages(Collections.singletonList(exceptionMessage))));

    procedure.executeFromOperateOnDataNodes(env);

    final PipeRuntimeMeta runtimeMeta =
        taskInfo.getPipeMetaByPipeName(request.getPipeName()).getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, runtimeMeta.getStatus().get());
    assertTrue(runtimeMeta.getIsStoppedByRuntimeException());
    assertEquals(error, runtimeMeta.getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
    assertPipeMetaChangeWasWritten(consensusManager, request.getPipeName(), error);
  }

  private static ConsensusManager mockSuccessfulConsensusWrite(final ConfigNodeProcedureEnv env)
      throws Exception {
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.write(Mockito.any(ConfigPhysicalPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    return consensusManager;
  }

  private static void assertPipeMetaChangeWasWritten(
      final ConsensusManager consensusManager, final String pipeName, final String error)
      throws Exception {
    final ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    Mockito.verify(consensusManager).write(planCaptor.capture());
    assertTrue(planCaptor.getValue() instanceof PipeHandleMetaChangePlan);
    final PipeRuntimeMeta persistedRuntimeMeta =
        ((PipeHandleMetaChangePlan) planCaptor.getValue())
            .getPipeMetaList().stream()
                .filter(meta -> pipeName.equals(meta.getStaticMeta().getPipeName()))
                .findFirst()
                .orElseThrow(AssertionError::new)
                .getRuntimeMeta();
    assertEquals(PipeStatus.STOPPED, persistedRuntimeMeta.getStatus().get());
    assertTrue(persistedRuntimeMeta.getIsStoppedByRuntimeException());
    assertEquals(
        error, persistedRuntimeMeta.getNodeId2PipeRuntimeExceptionMap().get(1).getMessage());
  }

  private static TCreatePipeReq createPipeRequest(final String pipeName) {
    final Map<String, String> extractorAttributes = new HashMap<>();
    extractorAttributes.put(PipeSourceConstant.EXTRACTOR_CONSENSUS_GROUP_ID_KEY, "DataRegion[1]");
    extractorAttributes.put(PipeSourceConstant.EXTRACTOR_CONSENSUS_SENDER_DATANODE_ID_KEY, "1");
    return new TCreatePipeReq(PipeStaticMeta.CONSENSUS_PIPE_PREFIX + pipeName, new HashMap<>())
        .setExtractorAttributes(extractorAttributes)
        .setProcessorAttributes(new HashMap<>());
  }

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
  public void serializeDeserializeWithMissingOptionalAttributesTest() throws Exception {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);

    final Map<String, String> connectorAttributes = new HashMap<>();
    connectorAttributes.put("connector", "iotdb-thrift-connector");

    final CreatePipeProcedureV2 proc =
        new CreatePipeProcedureV2(new TCreatePipeReq("testPipe", connectorAttributes));

    proc.serialize(outputStream);
    final ByteBuffer buffer =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    final CreatePipeProcedureV2 proc2 =
        (CreatePipeProcedureV2) ProcedureFactory.getInstance().create(buffer);

    assertEquals(proc, proc2);
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
