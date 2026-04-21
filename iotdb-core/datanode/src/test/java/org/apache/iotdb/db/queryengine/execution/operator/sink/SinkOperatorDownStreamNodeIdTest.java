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

package org.apache.iotdb.db.queryengine.execution.operator.sink;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.DataNodeTableOperatorGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator.DOWNSTREAM_PLAN_NODE_ID;
import static org.junit.Assert.assertEquals;

public class SinkOperatorDownStreamNodeIdTest {

  private static ExecutorService instanceNotificationExecutor;
  public final Metadata metadata = new TableMetadataImpl();

  @BeforeClass
  public static void setUp() {
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "sink-downstream-id-notification");
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  private LocalExecutionPlanContext createLocalExecutionPlanContext(
      TypeProvider typeProvider, String query_name, int queryId) {

    QueryId PlanFragmenQueryId = new QueryId(query_name);
    FragmentInstanceId instanceId =
        new FragmentInstanceId(
            new PlanFragmentId(PlanFragmenQueryId, queryId), "stub-instance" + queryId);
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    return new LocalExecutionPlanContext(
        typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  // for table model
  @Test
  public void testSingleDownStreamNodeIdInTable() throws Exception {

    PlanNodeId testIdentitySinkNode = new PlanNodeId("testIdentitySinkNode");
    PlanNode mockedPlanNode = Mockito.mock(PlanNode.class);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContext(
            new TypeProvider(), "query_name_sink_operator_downstream_id_1", 10001);
    DataNodeTableOperatorGenerator tableOperatorGenerator =
        new DataNodeTableOperatorGenerator(metadata);
    Mockito.when(mockedPlanNode.accept(tableOperatorGenerator, context))
        .thenReturn(Mockito.mock(Operator.class));

    String remotePlanNodeId = "178";
    DownStreamChannelLocation downStreamChannelLocation =
        new DownStreamChannelLocation(
            new TEndPoint("test", 1),
            new TFragmentInstanceId("query_name_sink_operator_downstream_id_1", 10001, "test"),
            remotePlanNodeId);
    try (IdentitySinkNode identitySinkNode =
        new IdentitySinkNode(
            testIdentitySinkNode,
            Collections.singletonList(mockedPlanNode),
            Collections.singletonList(downStreamChannelLocation))) {

      Operator identitySinkOperatorOfTable =
          identitySinkNode.accept(tableOperatorGenerator, context);
      assertEquals(
          identitySinkOperatorOfTable
              .getOperatorContext()
              .getSpecifiedInfo()
              .get(DOWNSTREAM_PLAN_NODE_ID)
              .toString(),
          remotePlanNodeId);
    }
  }

  // for tree model
  @Test
  public void testSingleDownStreamNodeIdInTree() throws Exception {

    PlanNodeId testIdentitySinkNode = new PlanNodeId("testIdentitySinkNode");
    PlanNode mockedChildPlanNode = Mockito.mock(PlanNode.class);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContext(
            new TypeProvider(), "query_name_sink_operator_downstream_id_2", 10002);

    OperatorTreeGenerator operatorTreeGenerator = Mockito.spy(new OperatorTreeGenerator());

    List<Operator> dummyChildrenOperator = Collections.singletonList(Mockito.mock(Operator.class));
    Mockito.doReturn(dummyChildrenOperator)
        .when(operatorTreeGenerator)
        .dealWithConsumeChildrenOneByOneNode(
            Mockito.any(PlanNode.class), Mockito.any(LocalExecutionPlanContext.class));

    Mockito.when(mockedChildPlanNode.accept(operatorTreeGenerator, context))
        .thenReturn(Mockito.mock(Operator.class));

    String remotePlanNodeId = "123";
    DownStreamChannelLocation downStreamChannelLocation =
        new DownStreamChannelLocation(
            new TEndPoint("test", 1),
            new TFragmentInstanceId("query_name_sink_operator_downstream_id_2", 10002, "test"),
            remotePlanNodeId);

    try (IdentitySinkNode identitySinkNode =
        new IdentitySinkNode(
            testIdentitySinkNode,
            Collections.singletonList(mockedChildPlanNode),
            Collections.singletonList(downStreamChannelLocation))) {

      Operator identitySinkOperatorOfTree = identitySinkNode.accept(operatorTreeGenerator, context);

      assertEquals(
          identitySinkOperatorOfTree
              .getOperatorContext()
              .getSpecifiedInfo()
              .get(DOWNSTREAM_PLAN_NODE_ID)
              .toString(),
          remotePlanNodeId);
    }
  }
}
