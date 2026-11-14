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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TRaftRequestSemanticsTest {
  @Test
  public void treeRowUsesItsExactTimestamp() {
    InsertRowNode row = new InsertRowNode(new PlanNodeId("row"));
    row.setTime(Long.MIN_VALUE);
    assertEquals(RequestSemantics.Kind.DATA, row.getRequestSemantics().getKind());
    assertEquals(Long.MIN_VALUE, row.getRequestSemantics().getTimestamp());
    row.setTime(17);
    assertEquals(17, row.getRequestSemantics().getTimestamp());
  }

  @Test
  public void tableAndUnknownSubclassesAreBarriers() {
    assertEquals(
        RequestSemantics.Kind.BARRIER,
        new RelationalInsertRowNode(new PlanNodeId("table")).getRequestSemantics().getKind());
    assertEquals(
        RequestSemantics.Kind.BARRIER,
        new InsertRowNode(new PlanNodeId("unknown")) {}.getRequestSemantics().getKind());
  }

  @Test
  public void batchesDoNotAdvertiseSingleTimestampSemantics() {
    assertFalse(RequestSemanticsProvider.class.isAssignableFrom(InsertRowsNode.class));
    InsertTabletNode tablet = new InsertTabletNode(new PlanNodeId("tablet"));
    tablet.setRowCount(2);
    tablet.setTimes(new long[] {10, 20});
    assertEquals(RequestSemantics.Kind.BARRIER, tablet.getRequestSemantics().getKind());
    tablet.setRowCount(1);
    assertEquals(10, tablet.getRequestSemantics().getTimestamp());
  }
}
