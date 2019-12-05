/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.cluster.log.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteFileSnapshot is a place holder that indicates the socket has unpulled data in the remote
 * node.
 */
public class RemoteFileSnapshot extends Snapshot implements RemoteSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(RemoteFileSnapshot.class);
  private Future pullSnapshotTask;

  public RemoteFileSnapshot(Future pullSnapshotTask) {
    this.pullSnapshotTask = pullSnapshotTask;
  }

  @Override
  public ByteBuffer serialize() {
    return null;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

  }

  @Override
  public void getRemoteSnapshot() {
    try {
      pullSnapshotTask.get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Cannot pull remote file snapshot:", e);
    }
  }
}
