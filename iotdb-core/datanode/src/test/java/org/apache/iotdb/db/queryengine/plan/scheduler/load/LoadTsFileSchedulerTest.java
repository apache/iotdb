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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LoadTsFileSchedulerTest {

  @Mock DistributedQueryPlan distributedQueryPlan;
  @Mock SubPlan subPlan;
  @Mock PlanFragment planFragment;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    when(distributedQueryPlan.getRootSubPlan()).thenReturn(subPlan);
    when(subPlan.getPlanFragment()).thenReturn(planFragment);
    when(planFragment.getId()).thenReturn(new PlanFragmentId("test", 0));
  }

  @Test
  public void tt() {
    LoadTsFileScheduler t =
        spy(
            new LoadTsFileScheduler(
                distributedQueryPlan,
                mock(MPPQueryContext.class),
                mock(QueryStateMachine.class),
                mock(IClientManager.class),
                mock(IPartitionFetcher.class),
                false));
    t.start();
    Assert.assertNull(t.getTotalCpuTime());
    Assert.assertNull(t.getFragmentInfo());
  }

  /**
   * The coordinator stops the execution of a query once its state is done, and a load that finished
   * passes no failure to that stop. The load must not answer with a failure it was never given:
   * reporting an explicit stop after a successful load makes the client fail a LOAD whose files are
   * already committed.
   */
  @Test
  public void testStopWithoutAFailureKeepsASuccessfulLoadSuccessful() {
    final QueryStateMachine stateMachine = newStateMachine("load_stop_finished");
    stateMachine.transitionToFinished();
    final String failureMessageBeforeTheStop = stateMachine.getFailureMessage();

    newScheduler(stateMachine).stop(null);

    Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());
    Assert.assertEquals(
        "a stop that was given no failure must not invent one",
        failureMessageBeforeTheStop,
        stateMachine.getFailureMessage());
  }

  /** A stop that carries the failure of the load still fails the task. */
  @Test
  public void testStopWithAFailureFailsTheLoad() {
    final QueryStateMachine stateMachine = newStateMachine("load_stop_failed");
    stateMachine.transitionToRunning();

    newScheduler(stateMachine).stop(new LoadFileException("the staging file is gone"));

    Assert.assertEquals(QueryState.FAILED, stateMachine.getState());
    Assert.assertEquals("the staging file is gone", stateMachine.getFailureMessage());
  }

  private LoadTsFileScheduler newScheduler(final QueryStateMachine stateMachine) {
    return new LoadTsFileScheduler(
        distributedQueryPlan,
        mock(MPPQueryContext.class),
        stateMachine,
        mock(IClientManager.class),
        mock(IPartitionFetcher.class),
        false);
  }

  /**
   * The state machine of these tests dispatches its listeners on the calling thread: a test must
   * not leave a thread pool behind, and the failure a stop records is written before the listeners
   * run.
   */
  private static QueryStateMachine newStateMachine(final String queryId) {
    return new QueryStateMachine(new QueryId(queryId), MoreExecutors.newDirectExecutorService());
  }

  @Test
  public void testGetPartitionQueryDatabaseForPipeGeneratedTreeModelLoad() {
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    when(node.isTableModel()).thenReturn(false);
    when(node.getDatabase()).thenReturn("root.test.sg");

    Assert.assertEquals("root.test.sg", LoadTsFileScheduler.getPartitionQueryDatabase(node, true));
    Assert.assertNull(LoadTsFileScheduler.getPartitionQueryDatabase(node, false));
  }

  @Test
  public void testGetPartitionQueryDatabaseForTableModelLoad() {
    final LoadSingleTsFileNode node = mock(LoadSingleTsFileNode.class);
    when(node.isTableModel()).thenReturn(true);
    when(node.getDatabase()).thenReturn("test");

    Assert.assertEquals("test", LoadTsFileScheduler.getPartitionQueryDatabase(node, false));
  }

  @Test
  public void testBuildRetryTreeLoadStatementUpdatesDatabaseLevel() throws Exception {
    final LoadFallbackHandler fallbackHandler =
        new LoadFallbackHandler(
            mock(MPPQueryContext.class),
            true,
            Collections.emptyList(),
            new ArrayList<>(),
            mock(QueryStateMachine.class));
    final Method method =
        LoadFallbackHandler.class.getDeclaredMethod(
            "buildRetryTreeLoadStatement", String.class, boolean.class, String.class);
    method.setAccessible(true);

    final File tsFile = File.createTempFile("test", ".tsfile");
    tsFile.deleteOnExit();

    final LoadTsFileStatement statement =
        (LoadTsFileStatement)
            method.invoke(fallbackHandler, tsFile.getAbsolutePath(), true, "root.test.sg_0");

    Assert.assertEquals("root.test.sg_0", statement.getDatabase());
    Assert.assertEquals(2, statement.getDatabaseLevel());
    Assert.assertTrue(statement.isGeneratedByPipe());
  }

  @Test
  public void testMemoryBoundedBufferClearReleasesCachedMemory() throws Exception {
    final Constructor<LoadTsFileDataCacheMemoryBlock> memoryBlockConstructor =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredConstructor(long.class);
    memoryBlockConstructor.setAccessible(true);
    final LoadTsFileDataCacheMemoryBlock memoryBlock =
        memoryBlockConstructor.newInstance(1024 * 1024L);

    // Simulate data buffered before split or routing aborts. clear() is the last chance to return
    // this accounting to the shared LOAD memory block.
    final long cachedMemorySize = 128L;
    final MemoryBoundedBuffer memoryBoundedBuffer = new MemoryBoundedBuffer(memoryBlock);
    memoryBoundedBuffer.add(cachedMemorySize);
    memoryBoundedBuffer.clear();

    final Method getMemoryUsageMethod =
        LoadTsFileDataCacheMemoryBlock.class.getDeclaredMethod("getMemoryUsageInBytes");
    getMemoryUsageMethod.setAccessible(true);
    Assert.assertEquals(0L, getMemoryUsageMethod.invoke(memoryBlock));
    Assert.assertEquals(0L, memoryBoundedBuffer.getDataSize());
  }
}
