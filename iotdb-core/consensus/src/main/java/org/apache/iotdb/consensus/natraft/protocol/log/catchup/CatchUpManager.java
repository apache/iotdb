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

package org.apache.iotdb.consensus.natraft.protocol.log.catchup;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CatchUpManager {

  private static final Logger logger = LoggerFactory.getLogger(CatchUpManager.class);
  private ExecutorService catchUpService;
  /**
   * lastCatchUpResponseTime records when is the latest response of each node's catch-up. There
   * should be only one catch-up task for each node to avoid duplication, but the task may time out
   * or the task may corrupt unexpectedly, and in that case, the next catch up should be enabled. So
   * if we find a catch-up task that does not respond for long, we will start a new one instead of
   * waiting for the previous one to finish.
   */
  private Map<Peer, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();

  private RaftMember member;
  private RaftConfig config;

  public CatchUpManager(RaftMember member, RaftConfig config) {
    this.member = member;
    this.config = config;
  }

  public void start() {
    catchUpService = IoTDBThreadPoolFactory.newCachedThreadPool(member.getName() + "-CatchUp");
  }

  public void stop() {
    catchUpService.shutdownNow();
    try {
      catchUpService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(
          "Unexpected interruption when waiting for heartBeatService and catchUpService "
              + "to end",
          e);
    }
    catchUpService = null;
  }

  public void registerTask(Peer node) {
    lastCatchUpResponseTime.put(node, System.currentTimeMillis());
  }

  public void unregisterTask(Peer node) {
    lastCatchUpResponseTime.remove(node);
  }

  /**
   * Update the followers' log by sending logs whose index >= followerLastMatchedLogIndex to the
   * follower. If some required logs are removed, also send the snapshot. <br>
   * notice that if a part of data is in the snapshot, then it is not in the logs.
   */
  public void catchUp(Peer follower, long lastLogIdx) {
    // for one follower, there is at most one ongoing catch-up, so the same data will not be sent
    // twice to the node
    synchronized (this) {
      // check if the last catch-up is still ongoing and does not time out yet
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < config.getCatchUpTimeoutMS()) {
        logger.debug("{}: last catch up of {} is ongoing", member, follower);
        return;
      } else {
        // record the start of the catch-up
        lastCatchUpResponseTime.put(follower, System.currentTimeMillis());
      }
    }
    logger.info("{}: Start to make {} catch up", member, follower);
    if (!catchUpService.isShutdown()) {
      Future<?> future =
          catchUpService.submit(
              new CatchUpTask(
                  follower,
                  member.getStatus().getPeerMap().get(follower),
                  this,
                  lastLogIdx,
                  config));
      catchUpService.submit(
          () -> {
            try {
              future.get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
              logger.error("{}: Catch up task exits with unexpected exception", member, e);
            }
          });
    }
  }

  public RaftMember getMember() {
    return member;
  }
}
