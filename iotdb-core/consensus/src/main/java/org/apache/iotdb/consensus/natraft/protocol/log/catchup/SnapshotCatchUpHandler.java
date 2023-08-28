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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.Snapshot;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/** SnapshotCatchUpHandler receives the result of sending a snapshot to a stale node. */
public class SnapshotCatchUpHandler implements AsyncMethodCallback<TSStatus> {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotCatchUpHandler.class);

  private AtomicReference<TSStatus> succeed;
  private Peer receiver;
  private Snapshot snapshot;

  public SnapshotCatchUpHandler(
      AtomicReference<TSStatus> succeed, Peer receiver, Snapshot snapshot) {
    this.succeed = succeed;
    this.receiver = receiver;
    this.snapshot = snapshot;
  }

  @Override
  public void onComplete(TSStatus resp) {
    synchronized (succeed) {
      succeed.set(resp);
      succeed.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot send snapshot {} to {}", snapshot, receiver, exception);
    synchronized (succeed) {
      succeed.notifyAll();
    }
  }
}
