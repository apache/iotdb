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

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;

public class LogCatchUpInBatchHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpInBatchHandler.class);

  private Node follower;
  private List<ByteBuffer> logs;
  private AtomicBoolean appendSucceed;
  private String memberName;
  private RaftMember raftMember;

  @Override
  public void onComplete(Long response) {
    logger.debug(
        "{}: Received a catch-up result size of {} from {}", memberName, logs.size(), follower);

    long resp = response;
    if (resp == RESPONSE_AGREE) {
      synchronized (appendSucceed) {
        appendSucceed.set(true);
        appendSucceed.notifyAll();
      }
      logger.debug("{}: Succeeded to send logs, size is {}", memberName, logs.size());

    } else if (resp == RESPONSE_LOG_MISMATCH) {
      // this is not probably possible
      logger.error(
          "{}: Log mismatch occurred when sending logs, whose size is {}", memberName, logs.size());
      synchronized (appendSucceed) {
        appendSucceed.notifyAll();
      }
    } else {
      // the follower's term has updated, which means a new leader is elected
      logger.debug(
          "{}: Received a rejection because term is updated to {} when sending {} logs",
          memberName,
          resp,
          logs.size());
      raftMember.stepDown(resp, false);

      synchronized (appendSucceed) {
        appendSucceed.notifyAll();
      }
      logger.warn(
          "{}: Catch-up with {} logs aborted because leadership is lost", logs.size(), memberName);
    }
  }

  @Override
  public void onError(Exception exception) {
    synchronized (appendSucceed) {
      appendSucceed.notifyAll();
    }
    logger.warn(
        "{}: Catch-up fails when sending log, whose size is {}",
        memberName,
        logs.size(),
        exception);
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

  public void setLogs(List<ByteBuffer> logs) {
    this.logs = logs;
  }
}
