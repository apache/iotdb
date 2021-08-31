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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class SnapshotCatchUpHandlerTest {

  @Test
  public void testComplete() throws InterruptedException {
    AtomicBoolean succeeded = new AtomicBoolean(false);
    Node receiver = TestUtils.getNode(0);
    SnapshotCatchUpHandler handler = new SnapshotCatchUpHandler(succeeded, receiver, null);
    synchronized (succeeded) {
      new Thread(() -> handler.onComplete(null)).start();
      succeeded.wait();
    }
    assertTrue(succeeded.get());
  }

  @Test
  public void testError() throws InterruptedException {
    AtomicBoolean succeeded = new AtomicBoolean(false);
    Node receiver = TestUtils.getNode(0);
    SnapshotCatchUpHandler handler = new SnapshotCatchUpHandler(succeeded, receiver, null);
    synchronized (succeeded) {
      new Thread(() -> handler.onError(new TestException())).start();
      succeeded.wait();
    }
    assertFalse(succeeded.get());
  }
}
