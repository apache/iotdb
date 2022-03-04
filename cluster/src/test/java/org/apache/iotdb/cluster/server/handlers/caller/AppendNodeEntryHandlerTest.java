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
import org.apache.iotdb.cluster.log.VotingLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.db.exception.StorageEngineException;
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

public class AppendNodeEntryHandlerTest {

  private RaftMember member;

  @Before
  public void setUp() {
    this.member = new TestMetaGroupMember();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
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
      VotingLog votingLog = new VotingLog(log, 10);
      member.getVotingLogList().insert(votingLog);
      Peer peer = new Peer(1);
      for (int i = 0; i < 10; i++) {
        AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
        handler.setLeaderShipStale(leadershipStale);
        handler.setLog(votingLog);
        handler.setMember(member);
        handler.setReceiverTerm(receiverTerm);
        handler.setReceiver(TestUtils.getNode(i));
        handler.setPeer(peer);
        handler.setQuorumSize(ClusterDescriptor.getInstance().getConfig().getReplicationNum() / 2);
        long resp = i >= 5 ? Response.RESPONSE_AGREE : Response.RESPONSE_LOG_MISMATCH;
        AppendEntryResult result = new AppendEntryResult();
        result.setStatus(resp);
        new Thread(() -> handler.onComplete(result)).start();
      }
      while (votingLog.getStronglyAcceptedNodeIds().size() < 5) {
        synchronized (votingLog) {
          votingLog.wait(1);
        }
      }
      assertEquals(-1, receiverTerm.get());
      assertFalse(leadershipStale.get());
      assertEquals(5, votingLog.getStronglyAcceptedNodeIds().size());
    } finally {
      ClusterDescriptor.getInstance().getConfig().setReplicationNum(replicationNum);
    }
  }

  @Test
  public void testNoAgreement() {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    VotingLog votingLog = new VotingLog(log, 10);
    member.getVotingLogList().insert(votingLog);
    Peer peer = new Peer(1);

    for (int i = 0; i < 3; i++) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setLog(votingLog);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(i));
      handler.setPeer(peer);
      handler.setQuorumSize(ClusterDescriptor.getInstance().getConfig().getReplicationNum() / 2);
      AppendEntryResult result = new AppendEntryResult();
      result.setStatus(Response.RESPONSE_AGREE);
      handler.onComplete(result);
    }

    assertEquals(-1, receiverTerm.get());
    assertFalse(leadershipStale.get());
    assertEquals(3, votingLog.getStronglyAcceptedNodeIds().size());
  }

  @Test
  public void testLeadershipStale() throws InterruptedException {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    VotingLog votingLog = new VotingLog(log, 10);
    Peer peer = new Peer(1);

    synchronized (votingLog) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setLog(votingLog);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      handler.setPeer(peer);
      handler.setQuorumSize(ClusterDescriptor.getInstance().getConfig().getReplicationNum() / 2);
      new Thread(() -> handler.onComplete(new AppendEntryResult(100L))).start();
      votingLog.wait();
    }
    assertEquals(100, receiverTerm.get());
    assertTrue(leadershipStale.get());
    assertEquals(0, votingLog.getStronglyAcceptedNodeIds().size());
  }

  @Test
  public void testError() {
    AtomicLong receiverTerm = new AtomicLong(-1);
    AtomicBoolean leadershipStale = new AtomicBoolean(false);
    Log log = new TestLog();
    int replicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(10);
    try {
      VotingLog votingLog = new VotingLog(log, 10);
      Peer peer = new Peer(1);

      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setLeaderShipStale(leadershipStale);
      handler.setLog(votingLog);
      handler.setMember(member);
      handler.setReceiverTerm(receiverTerm);
      handler.setReceiver(TestUtils.getNode(0));
      handler.setPeer(peer);
      handler.setQuorumSize(ClusterDescriptor.getInstance().getConfig().getReplicationNum() / 2);
      handler.onError(new TestException());

      assertEquals(-1, receiverTerm.get());
      assertFalse(leadershipStale.get());
      assertEquals(0, votingLog.getStronglyAcceptedNodeIds().size());

    } finally {
      ClusterDescriptor.getInstance().getConfig().setReplicationNum(replicationNum);
    }
  }
}
