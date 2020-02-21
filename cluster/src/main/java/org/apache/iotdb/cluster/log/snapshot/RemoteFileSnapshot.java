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

package org.apache.iotdb.cluster.log.snapshot;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.cluster.log.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteFileSnapshot is a place holder that indicates the slot has unpulled data in the remote
 * node.
 */
public class RemoteFileSnapshot extends Snapshot implements RemoteSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(RemoteFileSnapshot.class);
  private static final ByteBuffer EMPTY_SNAPSHOT_BYTES = new FileSnapshot().serialize();
  private Future<Map<Integer, FileSnapshot>> pullSnapshotTask;
  private Map<Integer, FileSnapshot> remoteSnapshots;
  private int slot;

  public RemoteFileSnapshot(Future<Map<Integer, FileSnapshot>> pullSnapshotTask, int slot) {
    this.pullSnapshotTask = pullSnapshotTask;
    this.slot = slot;
  }

  @Override
  public ByteBuffer serialize() {
    if (remoteSnapshots == null) {
      getRemoteSnapshot();
    }
    FileSnapshot fileSnapshot = remoteSnapshots.get(slot);
    if (fileSnapshot != null) {
      return fileSnapshot.serialize();
    } else {
      return EMPTY_SNAPSHOT_BYTES;
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    // remote file snapshot is not deserialized
  }

  @Override
  public void getRemoteSnapshot() {
    try {
       remoteSnapshots = pullSnapshotTask.get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Cannot pull remote file snapshot:", e);
    }
  }
}
