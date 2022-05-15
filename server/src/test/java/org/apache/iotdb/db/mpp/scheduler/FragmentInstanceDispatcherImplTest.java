package org.apache.iotdb.db.mpp.scheduler;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.DeleteTimeSeriesNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesSchemaNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteTimeSeriesDataNode;
import org.apache.iotdb.db.mpp.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.mpp.plan.scheduler.FragmentInstanceDispatcherImpl;
import org.apache.iotdb.db.mpp.plan.scheduler.IFragInstanceDispatcher;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FragmentInstanceDispatcherImpl.class})
public class FragmentInstanceDispatcherImplTest {

  private DeleteTimeSeriesNode deleteTimeSeriesNode;
  private PartialPath s1;
  private PartialPath s2;
  IFragInstanceDispatcher dispatcher;
  DeleteTimeSeriesNode nodeSpy;

  public FragmentInstanceDispatcherImplTest() throws IllegalPathException {}

  @Before
  public void setUp() throws IllegalPathException {
    List<PartialPath> deletedPaths = new ArrayList<>();
    s1 = new PartialPath("root.sg.d.s1");
    s2 = new PartialPath("root.sg.d.s2");
    deletedPaths.add(s1);
    deletedPaths.add(s2);
    deleteTimeSeriesNode = new DeleteTimeSeriesNode(new PlanNodeId("1"), deletedPaths);
    dispatcher =
        new FragmentInstanceDispatcherImpl(
            QueryType.WRITE,
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadScheduledExecutor(),
            null);
    this.nodeSpy = Mockito.spy(deleteTimeSeriesNode);
    Map<PartialPath, List<PlanNode>> dataRegionSet = new HashMap<>();
    final DeleteTimeSeriesDataNode dataNode1 =
        new DeleteTimeSeriesDataNode(new PlanNodeId("1"), s1);
    final DeleteTimeSeriesDataNode dataNode2 =
        new DeleteTimeSeriesDataNode(new PlanNodeId("1"), s2);
    final DeleteTimeSeriesSchemaNode schemaNode1 =
        new DeleteTimeSeriesSchemaNode(new PlanNodeId("1"), s1);
    final DeleteTimeSeriesSchemaNode schemaNode2 =
        new DeleteTimeSeriesSchemaNode(new PlanNodeId("1"), s2);
    Map<PartialPath, PlanNode> schemaRegionSet = new HashMap<>();
    dataRegionSet.put(s1, Collections.singletonList(dataNode1));
    dataRegionSet.put(s2, Collections.singletonList(dataNode2));
    schemaRegionSet.put(s1, schemaNode1);
    schemaRegionSet.put(s2, schemaNode2);
    PowerMockito.when(nodeSpy.getDataRegionSplitMap()).thenReturn(dataRegionSet);
    PowerMockito.when(nodeSpy.getSchemaRegionSpiltMap()).thenReturn(schemaRegionSet);
  }

  @Test
  public void testDispatchDelete() throws Exception {
    final PlanFragmentId planFragmentId = new PlanFragmentId("1", 1);
    FragmentInstance deleteInstance =
        new FragmentInstance(
            new PlanFragment(planFragmentId, nodeSpy),
            planFragmentId.genFragmentInstanceId(),
            null,
            QueryType.WRITE);
    final SettableFuture<FragInstanceDispatchResult> deleteFuture = SettableFuture.create();
    deleteFuture.set(new FragInstanceDispatchResult(true));
    PowerMockito.stub(
            PowerMockito.method(FragmentInstanceDispatcherImpl.class, "dispatchWrite", List.class))
        .toReturn(deleteFuture);
    final Future<FragInstanceDispatchResult> dispatch =
        dispatcher.dispatch(Collections.singletonList(deleteInstance));
    Assert.assertTrue(dispatch.get().isSuccessful());
  }
}
