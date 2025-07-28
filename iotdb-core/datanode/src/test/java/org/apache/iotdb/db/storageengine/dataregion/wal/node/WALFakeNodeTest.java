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
package org.apache.iotdb.db.storageengine.dataregion.wal.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALFlushListener;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WALFakeNodeTest {
  private IWALNode walNode;

  @Test
  public void testSuccessFakeNode() {
    walNode = WALFakeNode.getSuccessInstance();
    // log something
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    walFlushListeners.add(walNode.log(1, new InsertRowNode(new PlanNodeId("0"))));
    walFlushListeners.add(
        walNode.log(
            1,
            new InsertTabletNode(new PlanNodeId("1")),
            Collections.singletonList(new int[] {0, 0})));
    walFlushListeners.add(
        walNode.log(1, new DeleteDataNode(new PlanNodeId("2"), Collections.emptyList(), 0, 0)));
    // check flush listeners
    try {
      for (WALFlushListener walFlushListener : walFlushListeners) {
        assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.waitForResult());
      }
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void testFailureFakeNode() {
    Exception expectedException = new Exception("test");
    walNode = WALFakeNode.getFailureInstance(expectedException);
    // log something
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    walFlushListeners.add(walNode.log(1, new InsertRowNode(new PlanNodeId("0"))));
    walFlushListeners.add(
        walNode.log(
            1,
            new InsertTabletNode(new PlanNodeId("1")),
            Collections.singletonList(new int[] {0, 0})));
    walFlushListeners.add(
        walNode.log(1, new DeleteDataNode(new PlanNodeId("2"), Collections.emptyList(), 0, 0)));
    // check flush listeners
    try {
      for (WALFlushListener walFlushListener : walFlushListeners) {
        assertEquals(WALFlushListener.Status.FAILURE, walFlushListener.waitForResult());
        assertEquals(expectedException, walFlushListener.getCause().getCause());
      }
    } catch (NullPointerException e) {
      // ignore
    }
  }
}
