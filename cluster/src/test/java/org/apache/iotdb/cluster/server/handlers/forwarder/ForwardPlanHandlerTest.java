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

package org.apache.iotdb.cluster.server.handlers.forwarder;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ForwardPlanHandlerTest {

  @Test
  public void testComplete() throws IllegalPathException {
    AtomicReference<TSStatus> result = new AtomicReference<>();
    PhysicalPlan plan = new SetStorageGroupPlan(new PartialPath("root.test"));
    ForwardPlanHandler handler = new ForwardPlanHandler(result, plan, TestUtils.getNode(0));

    TSStatus status = new TSStatus();
    handler.onComplete(status);
    assertSame(status, result.get());
  }

  @Test
  public void testError() throws IllegalPathException {
    AtomicReference<TSStatus> result = new AtomicReference<>();
    PhysicalPlan plan = new SetStorageGroupPlan(new PartialPath("root.test"));
    ForwardPlanHandler handler = new ForwardPlanHandler(result, plan, TestUtils.getNode(0));

    handler.onError(new TestException());
    assertEquals("Don't worry, this exception is faked", result.get().getMessage());
  }
}
