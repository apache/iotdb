/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteDataSimpleSnapshot is a snapshot that is being pulled from a remote node. Any query or
 * modification to this snapshot must wait until the pulling is finished.
 */
public class RemoteDataSimpleSnapshot extends DataSimpleSnapshot implements RemoteSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(RemoteDataSimpleSnapshot.class);
  private Future<Map<Integer, DataSimpleSnapshot>> remoteSnapshotFuture;
  private List<Log> tempList = new ArrayList<>();
  private int socket;

  public RemoteDataSimpleSnapshot(Future<Map<Integer, DataSimpleSnapshot>> remoteSnapshotFuture,
      int socket) {
    this.remoteSnapshotFuture = remoteSnapshotFuture;
    this.socket = socket;
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
    synchronized (this) {
      if (snapshot != null) {
        snapshot.add(log);
      } else {
        tempList.add(log);
      }
    }
  }

  @Override
  public List<MeasurementSchema> getTimeseriesSchemas() {
    getRemoteSnapshot();
    return super.getTimeseriesSchemas();
  }

  public void getRemoteSnapshot() {
    if (snapshot == null) {
      try {
        logger.info("Waiting for the remote snapshot");
        Map<Integer, DataSimpleSnapshot> remoteSnapshots = remoteSnapshotFuture.get();
        synchronized (this) {
          logger.info("The remote snapshot is ready");
          DataSimpleSnapshot remoteSnapshot = remoteSnapshots.get(socket);
          if (remoteSnapshot != null) {
            snapshot = remoteSnapshot.snapshot;
            timeseriesSchemas.addAll(remoteSnapshot.timeseriesSchemas);
            snapshot.addAll(tempList);
            tempList = null;
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        logger.error("Cannot get remote snapshot", e);
        snapshot = tempList;
      }
    }
  }
}
