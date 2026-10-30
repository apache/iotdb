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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PipeTaskInfoRatisTest {
  private String originalConsensus;
  private final PipeTaskInfo pipeTaskInfo = new PipeTaskInfo();

  @Before
  public void setUp() {
    originalConsensus =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
  }

  @After
  public void tearDown() {
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(originalConsensus);
  }

  @Test
  public void testCreateAndStartValidateUnderProcedureLock() {
    final TCreatePipeReq insertion = createRequest("insertion", Map.of());
    assertDisabled(() -> pipeTaskInfo.checkBeforeCreatePipe(insertion));
    assertDisabled(
        () ->
            pipeTaskInfo.checkBeforeCreatePipe(
                createRequest("suffix", Map.of("inclusion", "data.insert.extra"))));
    for (final String inclusion : new String[] {"schema", "data.delete", "schema,data.delete"}) {
      final String name = inclusion.replace('.', '_').replace(',', '_');
      final TCreatePipeReq allowed = createRequest(name, Map.of("inclusion", inclusion));
      Assert.assertTrue(pipeTaskInfo.checkBeforeCreatePipe(allowed));
      store(allowed);
      pipeTaskInfo.checkBeforeStartPipe(name);
      pipeTaskInfo.checkBeforeStartPipe(name, false);
    }
    // Persisted metadata can predate the restriction and must be checked on START as well.
    store(insertion);
    assertDisabled(() -> pipeTaskInfo.checkBeforeStartPipe("insertion"));
    assertDisabled(() -> pipeTaskInfo.checkBeforeStartPipe("insertion", false));
    ConfigNodeDescriptor.getInstance()
        .getConf()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2);
    final String consensus = PipeStaticMeta.CONSENSUS_PIPE_PREFIX + "DataRegion[7]_1_2";
    final TCreatePipeReq internal = createRequest(consensus, Map.of());
    Assert.assertTrue(pipeTaskInfo.checkBeforeCreatePipe(internal));
    store(internal);
    pipeTaskInfo.checkBeforeStartPipe(consensus);
  }

  @Test
  public void testAlterRevalidatesAgainstLatestSerializedMetadata() {
    final TCreatePipeReq initial = createRequest("p", Map.of("inclusion", "schema"));
    store(initial);
    final Map<String, String> firstAttributes =
        Map.of("source.inclusion", "all", "source.inclusion.exclusion", "data.insert");
    final Map<String, String> secondAttributes = Map.of("extractor.inclusion.exclusion", "");
    // Both partial updates are allowed against the original RPC-time snapshot.
    Assert.assertTrue(PipeConfigNodeAgent.isPipeDataInsertionEnabled(firstAttributes));
    Assert.assertTrue(
        PipeConfigNodeAgent.isPipeDataInsertionEnabled(
            Map.of("inclusion", "schema", "inclusion.exclusion", "")));
    final TAlterPipeReq first = alterRequest(firstAttributes, false);
    pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(first);
    final PipeStaticMeta oldMeta = pipeTaskInfo.getPipeMetaByPipeName("p").getStaticMeta();
    final PipeStaticMeta newMeta =
        new PipeStaticMeta(
            "p", 2L, first.getExtractorAttributes(), new HashMap<>(), new HashMap<>());
    pipeTaskInfo.alterPipe(new AlterPipePlanV2(oldMeta, newMeta, new PipeRuntimeMeta()));
    assertDisabled(
        () ->
            pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(
                alterRequest(secondAttributes, false)));
    Assert.assertTrue(
        PipeConfigNodeAgent.isPipeDataInsertionEnabled(
            pipeTaskInfo.getPipeMetaByPipeName("p").getStaticMeta().getSourceParameters()));
    assertDisabled(
        () -> pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(alterRequest(Map.of(), true)));
    pipeTaskInfo.checkAndUpdateRequestBeforeAlterPipe(
        alterRequest(Map.of("inclusion", "schema,data.delete"), true));
  }

  private TCreatePipeReq createRequest(String name, Map<String, String> source) {
    return new TCreatePipeReq()
        .setPipeName(name)
        .setExtractorAttributes(new HashMap<>(source))
        .setProcessorAttributes(new HashMap<>())
        .setConnectorAttributes(new HashMap<>());
  }

  private TAlterPipeReq alterRequest(Map<String, String> source, boolean replace) {
    return new TAlterPipeReq("p", new HashMap<>(), new HashMap<>(), false, false)
        .setExtractorAttributes(new HashMap<>(source))
        .setIsReplaceAllExtractorAttributes(replace)
        .setIsTableModel(false);
  }

  private void store(TCreatePipeReq request) {
    pipeTaskInfo.createPipe(
        new CreatePipePlanV2(
            new PipeStaticMeta(
                request.getPipeName(),
                1L,
                request.getExtractorAttributes(),
                request.getProcessorAttributes(),
                request.getConnectorAttributes()),
            new PipeRuntimeMeta()));
  }

  private void assertDisabled(Runnable action) {
    Assert.assertEquals(
        PipeMessages
            .MESSAGE_PIPE_DATA_INSERTION_IS_DISABLED_WHEN_DATA_REGION_CONSENSUS_PROTOCOL_IS_RATIS_7D669ECC,
        Assert.assertThrows(PipeException.class, action::run).getMessage());
  }
}
