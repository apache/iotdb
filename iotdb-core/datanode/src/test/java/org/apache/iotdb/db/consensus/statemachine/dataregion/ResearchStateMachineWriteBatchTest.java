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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ResearchStateMachineWriteBatchTest {

  /** Records what DataRegion would receive and answers with a scripted status. */
  private static final class Recording extends ResearchRaftDataRegionStateMachine {
    final List<PlanNode> written = new ArrayList<>();
    final Function<PlanNode, TSStatus> answer;

    Recording(Function<PlanNode, TSStatus> answer) {
      super(null);
      this.answer = answer;
    }

    @Override
    protected TSStatus write(PlanNode planNode) {
      written.add(planNode);
      return answer.apply(planNode);
    }
  }

  private static InsertTabletNode tablet(String device, long time) throws Exception {
    return new InsertTabletNode(
        new PlanNodeId(device + "@" + time),
        new PartialPath(device),
        false,
        new String[] {"s1"},
        new TSDataType[] {TSDataType.INT64},
        new long[] {time},
        new BitMap[] {null},
        new Object[] {new long[] {time}},
        1);
  }

  private static InsertTabletNode remote(InsertTabletNode node) {
    node.markAsGeneratedByRemoteConsensusLeader();
    return node;
  }

  @Test
  public void runsOfPlainTabletsWithOneOriginBecomeOneMultiTabletWriteInOrder() throws Exception {
    InsertTabletNode a = tablet("root.sg.d1", 1);
    InsertTabletNode b = tablet("root.sg.d2", 2);
    InsertTabletNode c = remote(tablet("root.sg.d3", 3));
    InsertTabletNode d = remote(tablet("root.sg.d4", 4));
    InsertRowNode row =
        new InsertRowNode(
            new PlanNodeId("row"),
            new PartialPath("root.sg.d5"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT64},
            5L,
            new Object[] {5L},
            false);
    InsertTabletNode e = tablet("root.sg.d6", 6);
    Recording machine = new Recording(node -> StatusUtils.OK);
    List<IConsensusRequest> requests = Arrays.asList(a, b, c, d, row, e);
    List<TSStatus> statuses = machine.writeBatch(requests);
    assertEquals(6, statuses.size());
    for (TSStatus status : statuses) {
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
    // [a b] local run, [c d] remote run, the row and the lone tablet one by one.
    assertEquals(4, machine.written.size());
    InsertMultiTabletsNode local = (InsertMultiTabletsNode) machine.written.get(0);
    assertEquals(Arrays.asList(a, b), local.getInsertTabletNodeList());
    assertFalse(local.isGeneratedByRemoteConsensusLeader());
    InsertMultiTabletsNode fromLeader = (InsertMultiTabletsNode) machine.written.get(1);
    assertEquals(Arrays.asList(c, d), fromLeader.getInsertTabletNodeList());
    assertTrue(
        "Leader-written points are counted from the node's own flag",
        fromLeader.isGeneratedByRemoteConsensusLeader());
    assertSame(row, machine.written.get(2));
    assertSame(e, machine.written.get(3));
  }

  @Test
  public void aTabletFailureStaysAtItsPositionAndAWholeCallFailureCoversTheRun() throws Exception {
    TSStatus typeError = RpcUtils.getStatus(TSStatusCode.DATA_TYPE_MISMATCH, "type");
    Recording partial =
        new Recording(
            node -> {
              ((InsertMultiTabletsNode) node).getResults().put(1, typeError);
              return typeError;
            });
    List<TSStatus> statuses =
        partial.writeBatch(
            Arrays.asList(
                tablet("root.sg.d1", 1), tablet("root.sg.d2", 2), tablet("root.sg.d3", 3)));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statuses.get(0).getCode());
    assertSame(typeError, statuses.get(1));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statuses.get(2).getCode());

    TSStatus reject = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "memory");
    Recording rejected = new Recording(node -> reject);
    statuses = rejected.writeBatch(Arrays.asList(tablet("root.sg.d1", 1), tablet("root.sg.d2", 2)));
    assertEquals(Collections.nCopies(2, reject), statuses);
  }
}
