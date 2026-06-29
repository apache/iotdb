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

package org.apache.iotdb.commons.pipe.agent.task;

import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.mpp.rpc.thrift.TPipeHeartbeatReq;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PipeTaskAgentTest {

  @Test
  public void testFloatingMemoryUsageSurvivesDropUntilLateRelease() throws IllegalPathException {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();

    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe"));

    agent.dropPipeForTest("pipe", 1L);
    Assert.assertEquals(100L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(100L, agent.getFloatingMemoryUsageInByte("pipe"));

    agent.createPipeForTest(generatePipeMeta("pipe", 2L));
    agent.addFloatingMemoryUsageInByte("pipe", 2L, 20L);
    Assert.assertEquals(120L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(120L, agent.getFloatingMemoryUsageInByte("pipe"));

    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);
    Assert.assertEquals(20L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(20L, agent.getFloatingMemoryUsageInByte("pipe"));

    agent.dropPipeForTest("pipe", 2L);
    agent.decreaseFloatingMemoryUsageInByte("pipe", 2L, 20L);
    Assert.assertEquals(0L, agent.getAllFloatingMemoryUsageInByte());
    Assert.assertEquals(0L, agent.getFloatingMemoryUsageInByte("pipe"));
  }

  @Test
  public void testZeroFloatingMemoryUsageCounterIsCleanedAfterDrop() throws Exception {
    final DummyPipeTaskAgent agent = new DummyPipeTaskAgent();

    agent.createPipeForTest(generatePipeMeta("pipe", 1L));
    agent.addFloatingMemoryUsageInByte("pipe", 1L, 100L);
    agent.decreaseFloatingMemoryUsageInByte("pipe", 1L, 100L);
    agent.dropPipeForTest("pipe", 1L);

    Assert.assertTrue(
        getMapField(agent, "pipeNameWithCreationTime2FloatingMemoryUsageInByteMap").isEmpty());
    Assert.assertTrue(getMapField(agent, "pipeName2CreationTimeSetMap").isEmpty());
  }

  @SuppressWarnings("unchecked")
  private static Map<?, ?> getMapField(final PipeTaskAgent agent, final String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    final Field field = PipeTaskAgent.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Map<?, ?>) field.get(agent);
  }

  private static PipeMeta generatePipeMeta(final String pipeName, final long creationTime) {
    return new PipeMeta(
        new PipeStaticMeta(
            pipeName, creationTime, new HashMap<>(), new HashMap<>(), new HashMap<>()),
        new PipeRuntimeMeta(new ConcurrentHashMap<>()));
  }

  private static class DummyPipeTaskAgent extends PipeTaskAgent {

    private boolean createPipeForTest(final PipeMeta pipeMeta) throws IllegalPathException {
      return createPipe(pipeMeta);
    }

    private boolean dropPipeForTest(final String pipeName, final long creationTime) {
      return dropPipe(pipeName, creationTime);
    }

    @Override
    protected boolean isShutdown() {
      return false;
    }

    @Override
    protected void thawRate(final String pipeName, final long creationTime) {
      // Do nothing
    }

    @Override
    protected void freezeRate(final String pipeName, final long creationTime) {
      // Do nothing
    }

    @Override
    protected Map<Integer, PipeTask> buildPipeTasks(final PipeMeta pipeMetaFromCoordinator) {
      return new HashMap<>();
    }

    @Override
    protected void createPipeTask(
        final int consensusGroupId,
        final PipeStaticMeta pipeStaticMeta,
        final org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta pipeTaskMeta) {
      // Do nothing
    }

    @Override
    protected void collectPipeMetaListInternal(
        final TPipeHeartbeatReq req, final TPipeHeartbeatResp resp) throws TException {
      // Do nothing
    }

    @Override
    public void runPipeTasks(
        final Collection<PipeTask> pipeTasks, final Consumer<PipeTask> runSingle) {
      pipeTasks.forEach(runSingle);
    }
  }
}
