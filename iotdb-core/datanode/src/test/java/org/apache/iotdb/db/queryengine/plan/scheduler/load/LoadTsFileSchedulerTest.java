package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

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
                mock(IPartitionFetcher.class)));
    t.start();
    Assert.assertNull(t.getTotalCpuTime());
    Assert.assertNull(t.getFragmentInfo());
  }
}
