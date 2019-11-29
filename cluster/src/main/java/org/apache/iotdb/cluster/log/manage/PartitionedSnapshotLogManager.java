/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.manage;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;

public class PartitionedSnapshotLogManager extends MemoryLogManager {

  private Map<Integer, SimpleSnapshot> socketSnapshots = new HashMap<>();
  private long snapshotLastLogId;
  private long snapshotLastLogTerm;

  public PartitionedSnapshotLogManager(LogApplier logApplier) {
    super(logApplier);
  }

  @Override
  public Snapshot getSnapshot() {
    PartitionedSnapshot partitionedSnapshot = new PartitionedSnapshot();
    for (Entry<Integer, SimpleSnapshot> entry : socketSnapshots.entrySet()) {
      partitionedSnapshot.putSnapshot(entry.getKey(), entry.getValue());
    }
    partitionedSnapshot.setLastLogId(snapshotLastLogId);
    partitionedSnapshot.setLastLogTerm(snapshotLastLogTerm);
    return partitionedSnapshot;
  }

  @Override
  public void takeSnapshot() {
    while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
      Log log = logBuffer.removeFirst();
      socketSnapshots.computeIfAbsent(log.calculateSocket(), s -> new SimpleSnapshot()).add(log);
      snapshotLastLogId = log.getCurrLogIndex();
      snapshotLastLogTerm = log.getCurrLogTerm();
    }
  }

  public void setSnapshot(SimpleSnapshot snapshot, int socket) {
    socketSnapshots.put(socket, snapshot);
  }
}
