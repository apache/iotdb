/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.catchup;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogCatchUpTask send a list of logs to a node to make the node keep up with the leader.
 * TODO-Cluster: implement a SnapshotCatchUpTask that use both logs and snapshot.
 */
public class LogCatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpTask.class);

  private List<Log> logs;
  Node node;
  RaftMember raftMember;

  public LogCatchUpTask(List<Log> logs, Node node, RaftMember raftMember) {
    this.logs = logs;
    this.node = node;
    this.raftMember = raftMember;
  }

  void doLogCatchUp() {

    AppendEntryRequest request = new AppendEntryRequest();
    AtomicBoolean appendSucceed = new AtomicBoolean(false);
    boolean abort = false;

    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftMember(raftMember);
    handler.setFollower(node);

    for (int i = 0; i < logs.size() && !abort; i++) {
      Log log = logs.get(i);
      synchronized (raftMember.getTerm()) {
        // make sure this node is still a leader
        if (raftMember.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up, aborting");
          break;
        }
        request.setTerm(raftMember.getTerm().get());
      }

      handler.setLog(log);
      request.setEntry(log.serialize());
      logger.debug("Catching up with log {}", log);

      synchronized (appendSucceed) {
        try {
          AsyncClient client = raftMember.connectNode(node);
          if (client == null) {
            return;
          }
          client.appendEntry(request, handler);
          raftMember.getLastCatchUpResponseTime().put(node, System.currentTimeMillis());
          // if the follower responds fast enough, this response may come before wait() is called and
          // the wait() will surely time out
          appendSucceed.wait(RaftServer.CONNECTION_TIME_OUT_MS);
        } catch (TException e) {
          logger.error("Cannot send log {} to {}", log, node);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Catch up is interrupted:", e);
        }
      }
      abort = !appendSucceed.get();
    }
  }

  public void run() {
    try {
      doLogCatchUp();
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    logger.debug("Catch up finished");
    // the next catch up is enabled
    raftMember.getLastCatchUpResponseTime().remove(node);
  }
}
