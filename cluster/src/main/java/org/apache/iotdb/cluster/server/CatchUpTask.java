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

package org.apache.iotdb.cluster.server;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.handlers.CatchUpHandler;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CatchUpTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpTask.class);

  private List<Log> logs;
  private Node node;
  private RaftServer raftServer;

  CatchUpTask(List<Log> logs, Node node, RaftServer raftServer) {
    this.logs = logs;
    this.node = node;
    this.raftServer = raftServer;
  }

  private void doCatchUp() {
    AsyncClient client = raftServer.connectNode(node);
    if (client == null) {
      return;
    }

    AppendEntryRequest request = new AppendEntryRequest();
    AtomicBoolean aborted = new AtomicBoolean(false);
    AtomicBoolean appendSucceed = new AtomicBoolean(false);

    CatchUpHandler handler = new CatchUpHandler();
    handler.setAborted(aborted);
    handler.setAppendSucceed(appendSucceed);
    handler.setRaftServer(raftServer);

    for (int i = 0; i < logs.size() && !aborted.get(); i++) {
      Log log = logs.get(i);
      synchronized (raftServer.getTerm()) {
        // make sure this node is still a leader
        if (raftServer.getCharacter() != NodeCharacter.LEADER) {
          logger.debug("Leadership is lost when doing a catch-up, aborting");
          break;
        }
        request.setTerm(raftServer.getTerm().get());
      }

      handler.setLog(log);
      request.setEntry(log.serialize());
      logger.debug("Catching up with log {}", log);
      try {
        client.appendEntry(request, handler);
        raftServer.getLastCatchUpResponse().put(node, System.currentTimeMillis());
      } catch (TException e) {
        logger.error("Cannot send log {} to {}", log, node);
        aborted.set(true);
      }

      synchronized (aborted) {
        if (!aborted.get() && !appendSucceed.get()) {
          try {
            aborted.wait(RaftServer.CONNECTION_TIME_OUT_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Catch up is interrupted:", e);
            aborted.set(true);
          }
        }
      }
    }
  }

  public void run() {
    try {
      doCatchUp();
    } catch (Exception e) {
      logger.error("Catch up {} errored", node, e);
    }
    logger.debug("Catch up finished");
    raftServer.getLastCatchUpResponse().remove(node);
  }
}
