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

package org.apache.iotdb.confignode.manager.pipe.coordinator.task;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(Parameterized.class)
public class PipeTaskCoordinatorTest {
  @Parameterized.Parameters(name = "{0}, tableModel={1}")
  public static Collection<Object[]> parameters() {
    final Collection<Object[]> cases = new ArrayList<>();
    for (String protocol :
        new String[] {
          ConsensusFactory.RATIS_CONSENSUS,
          ConsensusFactory.IOT_CONSENSUS,
          ConsensusFactory.IOT_CONSENSUS_V2,
          ConsensusFactory.SIMPLE_CONSENSUS
        }) {
      cases.add(new Object[] {protocol, false});
      cases.add(new Object[] {protocol, true});
    }
    return cases;
  }

  private final String protocol;
  private final boolean isTableModel;
  private String originalProtocol;
  private final ConfigManager configManager = mock(ConfigManager.class);
  private final PipeTaskInfo pipeTaskInfo =
      new PipeTaskInfo() {
        @Override
        public PipeMeta getPipeMetaByPipeName(String name, boolean tableModel) {
          // Schedule a real metadata removal just before lookup, after the RPC's earlier checks.
          Assert.assertTrue(isPipeExisted(name, tableModel));
          Assert.assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              dropPipe(new DropPipePlanV2(name, tableModel)).getCode());
          return super.getPipeMetaByPipeName(name, tableModel);
        }
      };
  private final PipeTaskCoordinator coordinator =
      new PipeTaskCoordinator(configManager, pipeTaskInfo);

  public PipeTaskCoordinatorTest(String protocol, boolean isTableModel) {
    this.protocol = protocol;
    this.isTableModel = isTableModel;
  }

  @Before
  public void setUp() {
    originalProtocol =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance().getConf().setDataRegionConsensusProtocolClass(protocol);
    final Map<String, String> source =
        new HashMap<>(
            Map.of(
                "inclusion",
                "data.insert",
                SystemConstant.SQL_DIALECT_KEY,
                isTableModel
                    ? SystemConstant.SQL_DIALECT_TABLE_VALUE
                    : SystemConstant.SQL_DIALECT_TREE_VALUE));
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(
            new PipeStaticMeta("p", 1L, source, new HashMap<>(), new HashMap<>()),
            new PipeRuntimeMeta()));
    Assert.assertTrue(pipeTaskInfo.isPipeExisted("p", isTableModel));
  }

  @After
  public void tearDown() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(originalProtocol);
  }

  @Test
  public void testAlterHandlesConcurrentDrop() {
    Assert.assertEquals(
        TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
        coordinator.alterPipe(alterRequest(false)).getCode());
    verifyNoMoreInteractions(configManager);
    Assert.assertFalse(pipeTaskInfo.isPipeExisted("p", isTableModel));
  }

  @Test
  public void testAlterIfExistsHandlesConcurrentDrop() {
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        coordinator.alterPipe(alterRequest(true)).getCode());
    verifyNoMoreInteractions(configManager);
    Assert.assertFalse(pipeTaskInfo.isPipeExisted("p", isTableModel));
  }

  @Test
  public void testStartHandlesConcurrentDrop() {
    Assert.assertEquals(
        TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode(),
        coordinator
            .startPipe(new TStartPipeReq().setPipeName("p").setIsTableModel(isTableModel))
            .getCode());
    verifyNoMoreInteractions(configManager);
    Assert.assertFalse(pipeTaskInfo.isPipeExisted("p", isTableModel));
  }

  private TAlterPipeReq alterRequest(boolean ifExists) {
    return new TAlterPipeReq("p", new HashMap<>(), new HashMap<>(), false, false)
        .setExtractorAttributes(new HashMap<>(Map.of("inclusion", "schema")))
        .setIsReplaceAllExtractorAttributes(false)
        .setIfExistsCondition(ifExists)
        .setIsTableModel(isTableModel);
  }
}
