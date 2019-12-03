/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.manage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.DataSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * PartitionedSnapshotLogManager provides a PartitionedSnapshot as snapshot, dividing each log to
 * a sub-snapshot according to its socket and stores timeseries schemas of each socket.
 */
public class PartitionedSnapshotLogManager extends MemoryLogManager {

  private Map<Integer, DataSimpleSnapshot> socketSnapshots = new HashMap<>();
  private Map<Integer, List<MeasurementSchema>> socketTimeseries = new HashMap<>();
  private long snapshotLastLogId;
  private long snapshotLastLogTerm;
  private PartitionTable partitionTable;

  public PartitionedSnapshotLogManager(LogApplier logApplier, PartitionTable partitionTable) {
    super(logApplier);
    this.partitionTable = partitionTable;
  }

  @Override
  public Snapshot getSnapshot() {
    // copy snapshots
    synchronized (socketSnapshots) {
      PartitionedSnapshot partitionedSnapshot = new PartitionedSnapshot();
      for (Entry<Integer, DataSimpleSnapshot> entry : socketSnapshots.entrySet()) {
        partitionedSnapshot.putSnapshot(entry.getKey(), entry.getValue());
      }
      partitionedSnapshot.setLastLogId(snapshotLastLogId);
      partitionedSnapshot.setLastLogTerm(snapshotLastLogTerm);
      return partitionedSnapshot;
    }
  }

  @Override
  public void takeSnapshot() {
    synchronized (socketSnapshots) {
      List<MNode> allSgNodes = MManager.getInstance().getAllStorageGroups();
      for (MNode sgNode : allSgNodes) {
        String storageGroupName = sgNode.getFullPath();
        int socket = Objects.hash(storageGroupName, 0);
        List<MeasurementSchema> schemas = socketTimeseries.computeIfAbsent(socket, s -> new ArrayList<>());
        collectSeries(sgNode, schemas);
      }

      while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
        Log log = logBuffer.removeFirst();
        socketSnapshots.computeIfAbsent(PartitionUtils.calculateLogSocket(log, partitionTable),
            socket -> new DataSimpleSnapshot(socketTimeseries.get(socket))).add(log);
        snapshotLastLogId = log.getCurrLogIndex();
        snapshotLastLogTerm = log.getCurrLogTerm();
      }
    }
  }

  private void collectSeries(MNode storageGroupNode, List<MeasurementSchema> timeseriesSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(storageGroupNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node.isLeaf()) {
        MeasurementSchema nodeSchema = node.getSchema();
        timeseriesSchemas.add(new MeasurementSchema(node.getFullPath(),
            nodeSchema.getType(), nodeSchema.getEncodingType(), nodeSchema.getCompressor()));
      } else if (node.hasChildren()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  public void setSnapshot(DataSimpleSnapshot snapshot, int socket) {
    synchronized (socketSnapshots) {
      socketSnapshots.put(socket, snapshot);
    }
  }
}
