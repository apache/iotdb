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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.lang.reflect.Method;

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
    final LoadTsFileScheduler scheduler =
        new LoadTsFileScheduler(
            distributedQueryPlan,
            mock(MPPQueryContext.class),
            mock(QueryStateMachine.class),
            mock(IClientManager.class),
            mock(IPartitionFetcher.class),
            true);
    final Method method =
        LoadTsFileScheduler.class.getDeclaredMethod(
            "buildRetryTreeLoadStatement", String.class, boolean.class, String.class);
    method.setAccessible(true);

    final File tsFile = File.createTempFile("test", ".tsfile");
    tsFile.deleteOnExit();

    final LoadTsFileStatement statement =
        (LoadTsFileStatement)
            method.invoke(scheduler, tsFile.getAbsolutePath(), true, "root.test.sg_0");

    Assert.assertEquals("root.test.sg_0", statement.getDatabase());
    Assert.assertEquals(2, statement.getDatabaseLevel());
    Assert.assertTrue(statement.isGeneratedByPipe());
  }
}
