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

import org.apache.iotdb.cluster.log.manage.RaftLogManager;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(CommitLogTask.class);
  private RaftLogManager logManager;
  private long leaderCommit;
  private long term;

  public CommitLogTask(RaftLogManager logManager, long leaderCommit, long term) {
    this.logManager = logManager;
    this.leaderCommit = leaderCommit;
    this.term = term;
  }

  /** listener field */
  private AsyncMethodCallback<Void> callback;

  /** @param callback the event listener */
  public void registerCallback(AsyncMethodCallback<Void> callback) {
    this.callback = callback;
  }

  private void doCommitLog() {
    if (callback == null) {
      logger.error("callback is not registered");
      return;
    }

    boolean success = logManager.maybeCommit(leaderCommit, term);
    if (success) {
      callback.onComplete(null);
    }
  }

  @Override
  public void run() {
    doCommitLog();
  }
}
