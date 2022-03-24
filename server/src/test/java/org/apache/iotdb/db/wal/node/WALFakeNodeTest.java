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
package org.apache.iotdb.db.wal.node;

import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WALFakeNodeTest {
  private IWALNode walNode;

  @Test
  public void testSuccessFakeNode() {
    walNode = WALFakeNode.getSuccessInstance();
    // log something
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    walFlushListeners.add(walNode.log(1, new InsertRowPlan()));
    walFlushListeners.add(walNode.log(1, new InsertRowsPlan()));
    walFlushListeners.add(walNode.log(1, new InsertRowsOfOneDevicePlan()));
    walFlushListeners.add(walNode.log(1, new InsertTabletPlan()));
    walFlushListeners.add(walNode.log(1, new InsertMultiTabletPlan()));
    walFlushListeners.add(walNode.log(1, new DeletePlan()));
    // check flush listeners
    for (WALFlushListener walFlushListener : walFlushListeners) {
      assertNotEquals(WALFlushListener.Status.FAILURE, walFlushListener.getResult());
    }
  }

  @Test
  public void testFailureFakeNode() {
    Exception expectedException = new Exception("test");
    walNode = WALFakeNode.getFailureInstance(expectedException);
    // log something
    List<WALFlushListener> walFlushListeners = new ArrayList<>();
    walFlushListeners.add(walNode.log(1, new InsertRowPlan()));
    walFlushListeners.add(walNode.log(1, new InsertRowsPlan()));
    walFlushListeners.add(walNode.log(1, new InsertRowsOfOneDevicePlan()));
    walFlushListeners.add(walNode.log(1, new InsertTabletPlan()));
    walFlushListeners.add(walNode.log(1, new InsertMultiTabletPlan()));
    walFlushListeners.add(walNode.log(1, new DeletePlan()));
    // check flush listeners
    for (WALFlushListener walFlushListener : walFlushListeners) {
      assertEquals(WALFlushListener.Status.FAILURE, walFlushListener.getResult());
      assertEquals(expectedException, walFlushListener.getCause().getCause());
    }
  }
}
