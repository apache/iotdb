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

package org.apache.iotdb.cluster.server.handlers;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.appendEntry_call;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient.sendHeartBeat_call;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatHandler implements AsyncMethodCallback<sendHeartBeat_call> {

  private static final Logger logger = LoggerFactory.getLogger(HeartBeatHandler.class);

  private RaftServer raftServer;
  private Node receiver;

  HeartBeatHandler(RaftServer raftServer, Node node) {
    this.raftServer = raftServer;
    this.receiver = node;
  }

  @Override
  public void onComplete(sendHeartBeat_call resp) {
    logger.debug("Received a heartbeat response");
    HeartBeatResponse response;
    try {
      response = resp.getResult();
    } catch (TException e) {
      onError(e);
      return;
    }

    long followerTerm = response.getTerm();
    if (followerTerm == RaftServer.AGREE) {
      // current leadership is still valid
      Node follower = response.getFollower();
      long lastLogIdx = response.getLastLogIndex();
      long lastLogTerm = response.getLastLogTerm();
      logger.debug("Node {} is still alive, log index: {}, log term: {}", follower, lastLogIdx,
          lastLogTerm);
      long localLastLogIdx = raftServer.getLogManager().getLastLogIndex();
      long localLastLogTerm = raftServer.getLogManager().getLastLogTerm();
      if (localLastLogIdx > lastLogIdx ||
          lastLogIdx == localLastLogIdx && localLastLogTerm > lastLogTerm) {
        handleCatchUp(follower, lastLogIdx);
      }
    } else {
      // current leadership is invalid because the follower has a larger term
      synchronized (raftServer.getTerm()) {
        long currTerm = raftServer.getTerm().get();
        if (currTerm < followerTerm) {
          logger.info("Losing leadership because current term {} is smaller than {}", currTerm,
              followerTerm);
          raftServer.getTerm().set(followerTerm);
          raftServer.setCharacter(NodeCharacter.FOLLOWER);
          raftServer.setLeader(null);
          raftServer.setLastHeartBeatReceivedTime(System.currentTimeMillis());
        }
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Heart beat error, receiver {}", receiver, exception);
    // reset the connection
    raftServer.disConnectNode(receiver);
  }


  private void handleCatchUp(Node follower, long followerLastLogIndex) {
    if (followerLastLogIndex == -1) {
      // if the follower does not have any logs, send from the first one
      followerLastLogIndex = 0;
    }
    // avoid concurrent catch-up of the same follower
    synchronized (follower) {
      AsyncClient client = raftServer.connectNode(follower);
      if (client != null) {
        List<Log> logs;
        boolean allLogsValid;
        Snapshot snapshot = null;
        synchronized (raftServer.getLogManager()) {
          allLogsValid = raftServer.getLogManager().logValid(followerLastLogIndex);
          logs = raftServer.getLogManager()
              .getLogs(followerLastLogIndex, raftServer.getLogManager().getLastLogIndex());
          if (!allLogsValid) {
            logger.debug("Logs in {} are too old, catch up with snapshot", follower);
            snapshot = raftServer.getLogManager().getSnapshot();
          }
        }
        if (allLogsValid) {
          try {
            doCatchUp(logs, client);
          } catch (TException e) {
            logger.error("Catch-up error, receiver {}", follower, e);
            // reset the connection
            raftServer.disConnectNode(follower);
          }
        } else {
          doCatchUp(logs, client, snapshot);
        }
      }
    }
  }

  private void doCatchUp(List<Log> logs, AsyncClient client) throws TException {
    AppendEntryRequest request = new AppendEntryRequest();
    AtomicBoolean aborted = new AtomicBoolean(false);
    AtomicBoolean appendSucceed = new AtomicBoolean(false);

    CatchUpHandler handler = new CatchUpHandler();
    handler.aborted = aborted;
    handler.appendSucceed = appendSucceed;

    for (Log log : logs) {
      synchronized (raftServer.getTerm()) {
        // make sure this node is still a leader
        if (raftServer.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up, aborting");
          return;
        }
        request.setTerm(raftServer.getTerm().get());
      }
      handler.log = log;
      request.setEntry(log.serialize());
      client.appendEntry(request, handler);

      synchronized (aborted) {
        if (!aborted.get() && !appendSucceed.get()) {
          try {
            aborted.wait(RaftServer.CONNECTION_TIME_OUT_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TException(new RequestTimeOutException(log));
          }
        }
      }
    }
  }

  class CatchUpHandler implements AsyncMethodCallback<appendEntry_call> {

    private Log log;
    private AtomicBoolean aborted;
    private AtomicBoolean appendSucceed;

    @Override
    public void onComplete(appendEntry_call response) {
      try {
        long resp = response.getResult();
        if (resp == RaftServer.AGREE) {
          synchronized (aborted) {
            appendSucceed.set(true);
            aborted.notifyAll();
          }
          logger.debug("Succeed to sen log {}", log);
        } else if (resp == RaftServer.LOG_MISMATCH) {
          // this is not probably possible
          logger.error("Log mismatch occurred when sending log {}", log);
          synchronized (aborted) {
            aborted.set(true);
            aborted.notifyAll();
          }
        } else {
          // the follower'term has updated, which means a new leader is elected
          synchronized (raftServer.getTerm()) {
            long currTerm = raftServer.getTerm().get();
            if (currTerm < resp) {
              raftServer.setCharacter(NodeCharacter.FOLLOWER);
              raftServer.getTerm().set(currTerm);
            }
          }
          synchronized (aborted) {
            aborted.set(true);
            aborted.notifyAll();
          }
          logger.warn("Catch-up aborted because leadership is lost");
        }
      } catch (TException e) {
        onError(e);
      }
    }

    @Override
    public void onError(Exception exception) {
      synchronized (aborted) {
        aborted.set(true);
        aborted.notifyAll();
      }
      logger.warn("Catchup fails when sending log {}", log, exception);
    }
  }

  private void doCatchUp(List<Log> logs, AsyncClient client, Snapshot snapshot) {
    // TODO-Cluster implement
  }
}
