/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteSimpleSnapshot extends SimpleSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSimpleSnapshot.class);
  private Future<SimpleSnapshot> remoteSnapshot;

  public RemoteSimpleSnapshot(Future<SimpleSnapshot> remoteSnapshot) {
    this.remoteSnapshot = remoteSnapshot;
  }

  @Override
  public ByteBuffer serialize() {
    getRemoteSnapshot();
    return super.serialize();
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    getRemoteSnapshot();
    super.deserialize(buffer);
  }

  @Override
  public List<Log> getSnapshot() {
    getRemoteSnapshot();
    return super.getSnapshot();
  }

  @Override
  public void add(Log log) {
    getRemoteSnapshot();
    super.add(log);
  }

  private void getRemoteSnapshot() {
    if (snapshot == null) {
      try {
        snapshot = remoteSnapshot.get().snapshot;
        if (snapshot == null) {
          snapshot = new ArrayList<>();
        }
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        logger.error("Cannot get remote snapshot", e);
        snapshot = new ArrayList<>();
      }
    }
  }
}
