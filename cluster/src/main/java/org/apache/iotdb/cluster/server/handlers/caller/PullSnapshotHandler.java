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

import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

/** PullSnapshotHandler receives the result of pulling a data partition from a node. */
public class PullSnapshotHandler<T extends Snapshot>
    implements AsyncMethodCallback<PullSnapshotResp> {

  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotHandler.class);
  private AtomicReference<Map<Integer, T>> resultRef;
  private Node node;
  private List<Integer> slot;
  private SnapshotFactory<T> factory;

  public PullSnapshotHandler(
      AtomicReference<Map<Integer, T>> resultRef,
      Node owner,
      List<Integer> slots,
      SnapshotFactory<T> factory) {
    this.resultRef = resultRef;
    this.node = owner;
    this.slot = slots;
    this.factory = factory;
  }

  @Override
  public void onComplete(PullSnapshotResp response) {
    synchronized (resultRef) {
      Map<Integer, T> ret = new HashMap<>();
      Map<Integer, ByteBuffer> snapshotBytes = response.snapshotBytes;
      for (Entry<Integer, ByteBuffer> entry : snapshotBytes.entrySet()) {
        T snapshot = factory.create();
        snapshot.deserialize(entry.getValue());
        ret.put(entry.getKey(), snapshot);
      }
      resultRef.set(ret);
      resultRef.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot pull snapshot of {} from {}", slot.size(), node, exception);
    synchronized (resultRef) {
      resultRef.notifyAll();
    }
  }
}
