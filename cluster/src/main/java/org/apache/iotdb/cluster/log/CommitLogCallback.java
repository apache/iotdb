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

import org.apache.iotdb.cluster.server.member.RaftMember;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogCallback implements AsyncMethodCallback<Void> {

  private static final Logger logger = LoggerFactory.getLogger(CommitLogCallback.class);
  private final RaftMember raftMember;

  public CommitLogCallback(RaftMember raftMember) {
    this.raftMember = raftMember;
  }

  @Override
  public void onComplete(Void v) {
    synchronized (raftMember.getSyncLock()) {
      raftMember.getSyncLock().notifyAll();
    }
  }

  @Override
  public void onError(Exception e) {
    logger.error("async commit log failed", e);
  }
}
