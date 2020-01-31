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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.server.Response;
import org.junit.Test;

public class AppendNodeEntryHandlerTest {

  @Test
  public void testAgreement() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (quorum) {
      for (int i = 0; i < 10; i++) {
        AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
        handler.setLeaderShipStale(leadershipStale);
        handler.setQuorum(quorum);
        handler.setLog(log);
        handler.setReceiverTerm(receiverTerm);
        handler.setReceiver(TestUtils.getNode(i));
        long resp = i < 5 ? Response.RESPONSE_AGREE : Response.RESPONSE_LOG_MISMATCH;
        new Thread(() -> handler.onComplete(resp)).start();
      }
      quorum.wait(10 * 1000);
    }
    assertEquals(-1, receiverTerm.get());
    assertFalse(leadershipStale.get());
    assertEquals(0, quorum.get());
  }

  @Test
  public void testNoAgreement() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (quorum) {
      for (int i = 0; i < 3; i++) {
        AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
        handler.setLeaderShipStale(leadershipStale);
        handler.setQuorum(quorum);
        handler.setLog(log);
        handler.setReceiverTerm(receiverTerm);
        handler.setReceiver(TestUtils.getNode(i));
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      quorum.wait(2 * 1000);
    }
    assertEquals(-1, receiverTerm.get());
    assertFalse(leadershipStale.get());
    assertEquals(2, quorum.get());
  }

  @Test
  public void testLeadershipStale() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (quorum) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setQuorum(quorum);
      handler.setLog(log);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      new Thread(() -> handler.onComplete(100L)).start();
      quorum.wait(2 * 1000);
    }
    assertEquals(100, receiverTerm.get());
    assertTrue(leadershipStale.get());
    assertEquals(5, quorum.get());
  }

  @Test
  public void testError() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (quorum) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setQuorum(quorum);
      handler.setLog(log);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      new Thread(() -> handler.onError(new TestException())).start();
      quorum.wait(2 * 1000);
    }
    assertEquals(-1, receiverTerm.get());
    assertFalse(leadershipStale.get());
    assertEquals(5, quorum.get());
  }
}