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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataRegionStateMachineTest {

  @Test
  public void testMergeInsertRowNodes() throws IllegalPathException {
    List<InsertNode> list = new ArrayList<>();
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            1000L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            999L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            998L,
            new Object[] {1.0, 2f, 300L},
            false);
    list.add(node);
    DataRegionStateMachine fakeStateMachine = new DataRegionStateMachine(null);
    InsertNode mergedNode = list.get(0).mergeInsertNode(list);
    Assert.assertTrue(mergedNode instanceof InsertRowsNode);
  }

  @Test
  public void testMergeInsertRowsNodes() throws IllegalPathException {
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId("plan node 1"));
    List<InsertNode> list = new ArrayList<>();
    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            1000L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 0);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            999L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 1);
    node =
        new InsertRowNode(
            new PlanNodeId("plan node 1"),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1", "s2", "s3"},
            new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
            998L,
            new Object[] {1.0, 2f, 300L},
            false);
    insertRowsNode.addOneInsertRowNode(node, 2);
    DataRegionStateMachine fakeStateMachine = new DataRegionStateMachine(null);
    list.add(insertRowsNode);
    list.add(insertRowsNode);
    InsertNode mergedNode = list.get(0).mergeInsertNode(list);
    Assert.assertTrue(mergedNode instanceof InsertRowsNode);
  }

  @Test
  public void testWriteRetryDoesNotWaitAfterLastAttempt() {
    final TestingDataRegionStateMachine stateMachine = new TestingDataRegionStateMachine(false);
    final FixedStatusPlanNode planNode =
        new FixedStatusPlanNode(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode());

    final TSStatus status = stateMachine.write(planNode);

    Assert.assertEquals(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), status.getCode());
    Assert.assertEquals(5, planNode.getAcceptCount());
    Assert.assertEquals(4, stateMachine.getWaitCount());
  }

  @Test
  public void testInterruptedWriteRetryStopsImmediately() {
    Thread.interrupted();
    try {
      final TestingDataRegionStateMachine stateMachine = new TestingDataRegionStateMachine(true);
      final FixedStatusPlanNode planNode =
          new FixedStatusPlanNode(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode());

      final TSStatus status = stateMachine.write(planNode);

      Assert.assertEquals(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), status.getCode());
      Assert.assertEquals(1, planNode.getAcceptCount());
      Assert.assertEquals(1, stateMachine.getWaitCount());
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private static class TestingDataRegionStateMachine extends DataRegionStateMachine {

    private final boolean interruptOnWait;
    private int waitCount;

    private TestingDataRegionStateMachine(final boolean interruptOnWait) {
      super(null);
      this.interruptOnWait = interruptOnWait;
    }

    private int getWaitCount() {
      return waitCount;
    }

    @Override
    protected void waitBeforeNextWriteRetry() throws InterruptedException {
      ++waitCount;
      if (interruptOnWait) {
        throw new InterruptedException();
      }
    }
  }

  private static class FixedStatusPlanNode extends PlanNode {

    private final int statusCode;
    private int acceptCount;

    private FixedStatusPlanNode(final int statusCode) {
      super(new PlanNodeId("test"));
      this.statusCode = statusCode;
    }

    private int getAcceptCount() {
      return acceptCount;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
      ++acceptCount;
      return (R) new TSStatus(statusCode);
    }

    @Override
    public List<PlanNode> getChildren() {
      return Collections.emptyList();
    }

    @Override
    public void addChild(final PlanNode child) {}

    @Override
    public PlanNode clone() {
      return new FixedStatusPlanNode(statusCode);
    }

    @Override
    public int allowedChildCount() {
      return NO_CHILD_ALLOWED;
    }

    @Override
    public List<String> getOutputColumnNames() {
      return Collections.emptyList();
    }

    @Override
    protected void serializeAttributes(final ByteBuffer byteBuffer) {}

    @Override
    protected void serializeAttributes(final DataOutputStream stream) throws IOException {}
  }
}
