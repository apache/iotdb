/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.handlers.caller;

import static org.apache.iotdb.cluster.server.member.MetaGroupMember.REPLICATION_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.server.Response;
import org.junit.Test;

public class AppendGroupEntryHandlerTest {

  @Test
  public void testAgreement() throws InterruptedException {
    int[] groupReceivedCounter = new int[10];
    for (int i = 0; i < 10; i++) {
      groupReceivedCounter[i] = REPLICATION_NUM / 2;
    }
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(-1);
    Log testLog = new TestLog();
    synchronized (groupReceivedCounter) {
      for (int i = 0; i < 10; i += 2) {
        AppendGroupEntryHandler handler = new AppendGroupEntryHandler(groupReceivedCounter, i,
            TestUtils.getNode(i), leadershipStale, testLog, newLeaderTerm);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      groupReceivedCounter.wait(10 * 1000);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(0, groupReceivedCounter[i]);
    }
    assertFalse(leadershipStale.get());
    assertEquals(-1, newLeaderTerm.get());
  }

  @Test
  public void testNoAgreement() throws InterruptedException {
    int[] groupReceivedCounter = new int[10];
    for (int i = 0; i < 10; i++) {
      groupReceivedCounter[i] = REPLICATION_NUM;
    }
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(-1);
    Log testLog = new TestLog();
    synchronized (groupReceivedCounter) {
      for (int i = 0; i < 5; i++) {
        AppendGroupEntryHandler handler = new AppendGroupEntryHandler(groupReceivedCounter, i,
            TestUtils.getNode(i), leadershipStale, testLog, newLeaderTerm);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      groupReceivedCounter.wait(2 * 1000);
    }
    for (int i = 0; i < 10; i++) {
      if (i < 5) {
        assertEquals(Math.max(0, REPLICATION_NUM - (5 - i)), groupReceivedCounter[i]);
      } else {
        assertEquals(Math.min(10 - i, REPLICATION_NUM),
            groupReceivedCounter[i]);
      }
    }
    assertFalse(leadershipStale.get());
    assertEquals(-1, newLeaderTerm.get());
  }

  @Test
  public void testLeadershipStale() throws InterruptedException {
    int[] groupReceivedCounter = new int[10];
    for (int i = 0; i < 10; i++) {
      groupReceivedCounter[i] = REPLICATION_NUM / 2;
    }
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(-1);
    Log testLog = new TestLog();
    synchronized (groupReceivedCounter) {
      AppendGroupEntryHandler handler = new AppendGroupEntryHandler(groupReceivedCounter, 0,
          TestUtils.getNode(0), leadershipStale, testLog, newLeaderTerm);
      new Thread(() -> handler.onComplete(100L)).start();
      groupReceivedCounter.wait(10 * 1000);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(REPLICATION_NUM / 2, groupReceivedCounter[i]);
    }
    assertTrue(leadershipStale.get());
    assertEquals(100, newLeaderTerm.get());
  }

  @Test
  public void testError() throws InterruptedException {
    int[] groupReceivedCounter = new int[10];
    for (int i = 0; i < 10; i++) {
      groupReceivedCounter[i] = REPLICATION_NUM / 2;
    }
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(-1);
    Log testLog = new TestLog();
    synchronized (groupReceivedCounter) {
      AppendGroupEntryHandler handler = new AppendGroupEntryHandler(groupReceivedCounter, 0,
          TestUtils.getNode(0), leadershipStale, testLog, newLeaderTerm);
      new Thread(() -> handler.onError(new TestException())).start();
      groupReceivedCounter.wait(10 * 1000);
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(REPLICATION_NUM / 2, groupReceivedCounter[i]);
    }
    assertFalse(leadershipStale.get());
    assertEquals(-1, newLeaderTerm.get());
  }
}