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
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AppendNodeEntryHandlerTest {

  private RaftMember member;

  @Before
  public void setUp() {
    this.member = new TestMetaGroupMember();
  }

  @After
  public void tearDown() throws IOException {
    member.closeLogManager();
    member.stop();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testAgreement() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();

    int replicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    try {
      ClusterDescriptor.getInstance().getConfig().setReplicationNum(10);
      AtomicInteger quorum = new AtomicInteger(5);
      Peer peer = new Peer(1);
      synchronized (quorum) {
        for (int i = 0; i < 10; i++) {
          AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
          handler.setLeaderShipStale(leadershipStale);
          handler.setVoteCounter(quorum);
          handler.setLog(log);
          handler.setMember(member);
          handler.setReceiverTerm(receiverTerm);
          handler.setReceiver(TestUtils.getNode(i));
          handler.setPeer(peer);
          long resp = i >= 5 ? Response.RESPONSE_AGREE : Response.RESPONSE_LOG_MISMATCH;
          new Thread(() -> handler.onComplete(resp)).start();
        }
        quorum.wait();
      }
      assertEquals(-1, receiverTerm.get());
      assertFalse(leadershipStale.get());
      assertEquals(0, quorum.get());
    } finally {
      ClusterDescriptor.getInstance().getConfig().setReplicationNum(replicationNum);
    }
  }

  @Test
  public void testNoAgreement() {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    AtomicInteger quorum = new AtomicInteger(5);
    Peer peer = new Peer(1);

    for (int i = 0; i < 3; i++) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setVoteCounter(quorum);
      handler.setLog(log);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(i));
      handler.setPeer(peer);
      handler.onComplete(Response.RESPONSE_AGREE);
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
    Peer peer = new Peer(1);

    synchronized (quorum) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setVoteCounter(quorum);
      handler.setLog(log);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      handler.setPeer(peer);
      new Thread(() -> handler.onComplete(100L)).start();
      quorum.wait();
    }
    assertEquals(100, receiverTerm.get());
    assertTrue(leadershipStale.get());
    assertEquals(5, quorum.get());
  }

  @Test
  public void testError() {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    int replicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(10);
    try {
      AtomicInteger quorum = new AtomicInteger(5);
      Peer peer = new Peer(1);

      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setVoteCounter(quorum);
      handler.setLog(log);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      handler.setPeer(peer);
      handler.onError(new TestException());

      assertEquals(-1, receiverTerm.get());
      assertFalse(leadershipStale.get());
      assertEquals(5, quorum.get());
    } finally {
      ClusterDescriptor.getInstance().getConfig().setReplicationNum(replicationNum);
    }
  }
}
