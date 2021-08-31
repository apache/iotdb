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

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;

/**
 * LogCatchUpHandler checks the result of appending a log in a catch-up task and decides to abort
 * the catch up or not.
 */
public class LogCatchUpHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpHandler.class);

  private Node follower;
  private Log log;
  private AtomicBoolean appendSucceed;
  private String memberName;
  private RaftMember raftMember;

  @Override
  public void onComplete(Long response) {
    logger.debug("{}: Received a catch-up result of {} from {}", memberName, log, follower);
    long resp = response;
    if (resp == RESPONSE_AGREE) {
      synchronized (appendSucceed) {
        appendSucceed.set(true);
        appendSucceed.notifyAll();
      }
      logger.debug("{}: Succeeded to send log {}", memberName, log);
    } else if (resp == RESPONSE_LOG_MISMATCH) {
      // this may occur when the follower suddenly received a lot of logs, committed them and
      // discarded the old ones, so we consider in this case the appending succeeded
      logger.debug("{}: Log mismatch occurred when sending log {}", memberName, log);
      synchronized (appendSucceed) {
        appendSucceed.set(true);
        appendSucceed.notifyAll();
      }
    } else {
      // the follower's term has updated, which means a new leader is elected
      logger.debug("{}: Received a rejection because term is updated to: {}", memberName, resp);
      raftMember.stepDown(resp, false);
      synchronized (appendSucceed) {
        appendSucceed.notifyAll();
      }
      logger.warn("{}: Catch-up aborted because leadership is lost", memberName);
    }
  }

  @Override
  public void onError(Exception exception) {
    synchronized (appendSucceed) {
      appendSucceed.notifyAll();
    }
    logger.warn("{}: Catch-up fails when sending log {}", memberName, log, exception);
  }

  public void setLog(Log log) {
    this.log = log;
  }

  public void setAppendSucceed(AtomicBoolean appendSucceed) {
    this.appendSucceed = appendSucceed;
  }

  public void setRaftMember(RaftMember raftMember) {
    this.raftMember = raftMember;
    this.memberName = raftMember.getName();
  }

  public void setFollower(Node follower) {
    this.follower = follower;
  }

  public AtomicBoolean getAppendSucceed() {
    return appendSucceed;
  }
}
