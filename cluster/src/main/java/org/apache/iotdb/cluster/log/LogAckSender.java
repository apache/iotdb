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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.cluster.config.ClusterConstant.THREAD_POLL_WAIT_TERMINATION_TIME_S;

public class LogAckSender {

  private static final Logger logger = LoggerFactory.getLogger(LogAckSender.class);

  private ExecutorService ackSenderPool;
  private RaftNode header;
  private RaftMember member;
  private BlockingQueue<AckRequest> requestQueue = new ArrayBlockingQueue<>(4096);
  private String baseThreadName;

  public LogAckSender(RaftMember member) {
    this.member = member;
    this.header = member.getHeader();
    ackSenderPool = IoTDBThreadPoolFactory.newFixedThreadPool(4, member.getName() + "-ACKSender");
    for (int i = 0; i < 4; i++) {
      ackSenderPool.submit(this::appendAckLeaderTask);
    }
  }

  public static class AckRequest {

    private Node leader;
    private long index;
    private long term;
    private long response;

    public AckRequest(Node leader, long index, long term, long response) {
      this.leader = leader;
      this.index = index;
      this.term = term;
      this.response = response;
    }
  }

  public void offer(Node leader, long index, long term, long response) {
    requestQueue.add(new AckRequest(leader, index, term, response));
  }

  private void appendAckLeaderTask() {
    List<AckRequest> ackRequestList = new ArrayList<>();
    baseThreadName = Thread.currentThread().getName();
    try {
      while (!Thread.interrupted()) {
        ackRequestList.clear();
        synchronized (requestQueue) {
          AckRequest req = requestQueue.take();
          ackRequestList.add(req);
          requestQueue.drainTo(ackRequestList);
        }

        appendAckLeader(ackRequestList);
        // Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void appendAckLeader(List<AckRequest> requests) {
    if (requests.isEmpty()) {
      return;
    }
    int index = 0;
    AckRequest requestToSend = null;
    for (; index < requests.size(); index++) {
      if (requestToSend == null) {
        requestToSend = requests.get(index);
      } else {
        AckRequest currRequest = requests.get(index);

        if (requestToSend.term == currRequest.term
            && requestToSend.index >= currRequest.index
            && (requestToSend.response == currRequest.response
                || requestToSend.response == Response.RESPONSE_STRONG_ACCEPT)) {
          // currRequest has the same response and leader as requestToSend, but has a smaller
          // index, so it can be covered by requestToSend, ignore it
          // continue
        } else if (requestToSend.term == currRequest.term
            && requestToSend.index < currRequest.index
            && (requestToSend.response == currRequest.response
                || currRequest.response == Response.RESPONSE_STRONG_ACCEPT)) {
          // currRequest has the same response and leader as requestToSend, but has a larger
          // index, so it can replace requestToSend
          requestToSend = currRequest;
        } else {
          // the requests cannot cover each other, send requestToSend first
          appendAckLeader(
              requestToSend.leader,
              requestToSend.index,
              requestToSend.term,
              requestToSend.response);
          requestToSend = currRequest;
        }
      }
    }

    if (requestToSend != null) {
      appendAckLeader(
          requestToSend.leader, requestToSend.index, requestToSend.term, requestToSend.response);
    }
  }

  private void appendAckLeader(Node leader, long index, long term, long response) {
    long ackStartTime = Statistic.RAFT_RECEIVER_APPEND_ACK.getOperationStartTime();
    AppendEntryResult result = new AppendEntryResult();
    result.setLastLogIndex(index);
    result.setLastLogTerm(term);
    result.status = response;
    result.setHeader(header);
    result.setReceiver(member.getThisNode());

    Client syncClient = null;
    try {
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        GenericHandler<Void> handler = new GenericHandler<>(leader, null);
        member.getAsyncClient(leader).acknowledgeAppendEntry(result, handler);
      } else {
        syncClient = member.getSyncClient(leader);
        syncClient.acknowledgeAppendEntry(result);
      }
    } catch (TException e) {
      logger.warn("Cannot send ack of {}-{} to leader {}", index, term, leader, e);
    } finally {
      if (syncClient != null) {
        ClientUtils.putBackSyncClient(syncClient);
      }
    }
    Thread.currentThread().setName(baseThreadName + "-" + index + "-" + response);
    Statistic.RAFT_SEND_RELAY_ACK.add(1);
    Statistic.RAFT_RECEIVER_APPEND_ACK.calOperationCostTimeFromStart(ackStartTime);
  }

  public void stop() {
    ackSenderPool.shutdownNow();
    try {
      ackSenderPool.awaitTermination(THREAD_POLL_WAIT_TERMINATION_TIME_S, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for ackSenderPool to end", e);
    }
  }
}
