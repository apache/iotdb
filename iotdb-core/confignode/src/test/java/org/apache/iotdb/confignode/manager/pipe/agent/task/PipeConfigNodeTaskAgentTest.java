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

package org.apache.iotdb.confignode.manager.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.consensus.ConsensusFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PipeConfigNodeTaskAgentTest {
  @Test
  public void testRecoveryDropsDisallowedPipeAndKeepsMetadataPipes() {
    final String originalConsensus =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    final boolean leaderReady = PipeConfigNodeAgent.runtime().isLeaderReady();
    final TestingTaskAgent agent = new TestingTaskAgent();
    try {
      PipeConfigNodeAgent.runtime().notifyLeaderReady();
      ConfigNodeDescriptor.getInstance()
          .getConf()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
      final PipeMeta insertion = meta("old", "all");
      Assert.assertNull(agent.handleSinglePipeMetaChanges(insertion));
      final PipeTask oldTask = agent.tasks.get("old");
      verify(oldTask).start();
      ConfigNodeDescriptor.getInstance()
          .getConf()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
      Assert.assertNull(agent.handleSinglePipeMetaChanges(insertion));
      verify(oldTask).stop();
      verify(oldTask).drop();
      Assert.assertFalse(agent.contains("old"));
      Assert.assertNull(agent.handleSinglePipeMetaChanges(meta("blocked", "all")));
      Assert.assertFalse(agent.contains("blocked"));
      Assert.assertFalse(agent.tasks.containsKey("blocked"));
      for (String inclusion : new String[] {"schema", "data.delete", "schema,data.delete"}) {
        Assert.assertNull(agent.handleSinglePipeMetaChanges(meta(inclusion, inclusion)));
        verify(agent.tasks.get(inclusion)).start();
        Assert.assertTrue(agent.contains(inclusion));
      }
      ConfigNodeDescriptor.getInstance()
          .getConf()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2);
      final String internal = PipeStaticMeta.CONSENSUS_PIPE_PREFIX + "DataRegion[7]_1_2";
      Assert.assertNull(agent.handleSinglePipeMetaChanges(meta(internal, "data.insert")));
      verify(agent.tasks.get(internal)).start();
      Assert.assertTrue(agent.contains(internal));
    } finally {
      agent.dropAllPipeTasks();
      ConfigNodeDescriptor.getInstance()
          .getConf()
          .setDataRegionConsensusProtocolClass(originalConsensus);
      if (!leaderReady) {
        PipeConfigNodeAgent.runtime().notifyLeaderUnavailable();
      }
    }
  }

  private PipeMeta meta(String name, String inclusion) {
    final PipeMeta meta =
        new PipeMeta(
            new PipeStaticMeta(
                name,
                1L,
                new HashMap<>(Map.of("inclusion", inclusion)),
                new HashMap<>(),
                new HashMap<>()),
            new PipeRuntimeMeta());
    meta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
    return meta;
  }

  private static class TestingTaskAgent extends PipeConfigNodeTaskAgent {
    private final Map<String, PipeTask> tasks = new HashMap<>();

    @Override
    protected Map<Integer, PipeTask> buildPipeTasks(PipeMeta meta) {
      final PipeTask task = mock(PipeTask.class);
      tasks.put(meta.getStaticMeta().getPipeName(), task);
      return Collections.singletonMap(1, task);
    }

    private boolean contains(String name) {
      return pipeMetaKeeper.containsPipeMeta(name);
    }
  }
}
