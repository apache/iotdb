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
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AppendGroupEntryHandlerTest {

  private int REPLICATION_NUM;
  private int prevReplicationNum;
  private RaftMember member;

  @Before
  public void setUp() {
    prevReplicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(2);
    REPLICATION_NUM = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    member = new TestMetaGroupMember();
  }

  @After
  public void tearDown() throws IOException {
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(prevReplicationNum);
    member.stop();
    member.closeLogManager();
    EnvironmentUtils.cleanAllDir();
  }

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
        AppendGroupEntryHandler handler =
            new AppendGroupEntryHandler(
                groupReceivedCounter,
                i,
                TestUtils.getNode(i),
                leadershipStale,
                testLog,
                newLeaderTerm,
                member);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      groupReceivedCounter.wait();
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
        AppendGroupEntryHandler handler =
            new AppendGroupEntryHandler(
                groupReceivedCounter,
                i,
                TestUtils.getNode(i),
                leadershipStale,
                testLog,
                newLeaderTerm,
                member);
        handler.onComplete(Response.RESPONSE_AGREE);
      }
    }
    for (int i = 0; i < 10; i++) {
      if (i < 5) {
        assertEquals(Math.max(0, REPLICATION_NUM - (5 - i)), groupReceivedCounter[i]);
      } else {
        assertEquals(Math.min(10 - i, REPLICATION_NUM), groupReceivedCounter[i]);
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
      AppendGroupEntryHandler handler =
          new AppendGroupEntryHandler(
              groupReceivedCounter,
              0,
              TestUtils.getNode(0),
              leadershipStale,
              testLog,
              newLeaderTerm,
              member);
      new Thread(() -> handler.onComplete(100L)).start();
      groupReceivedCounter.wait();
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

    AppendGroupEntryHandler handler =
        new AppendGroupEntryHandler(
            groupReceivedCounter,
            0,
            TestUtils.getNode(0),
            leadershipStale,
            testLog,
            newLeaderTerm,
            member);
    handler.onError(new TestException());

    for (int i = 0; i < 10; i++) {
      assertEquals(REPLICATION_NUM / 2, groupReceivedCounter[i]);
    }
    assertFalse(leadershipStale.get());
    assertEquals(-1, newLeaderTerm.get());
  }
}
