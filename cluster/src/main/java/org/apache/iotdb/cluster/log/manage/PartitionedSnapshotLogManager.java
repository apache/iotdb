/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.manage;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.DataSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PartitionedSnapshotLogManager provides a PartitionedSnapshot as snapshot, dividing each log to
 * a sub-snapshot according to its socket and stores timeseries schemas of each socket.
 */
public class PartitionedSnapshotLogManager extends MemoryLogManager {

  private static final Logger logger = LoggerFactory.getLogger(PartitionedSnapshotLogManager.class);

  Map<Integer, Snapshot> socketSnapshots = new HashMap<>();
  Map<Integer, Collection<MeasurementSchema>> socketTimeseries = new HashMap<>();
  long snapshotLastLogId;
  long snapshotLastLogTerm;
  PartitionTable partitionTable;
  Node header;

  public PartitionedSnapshotLogManager(LogApplier logApplier, PartitionTable partitionTable,
      Node header) {
    super(logApplier);
    this.partitionTable = partitionTable;
    this.header = header;
  }

  @Override
  public Snapshot getSnapshot() {
    // copy snapshots
    synchronized (socketSnapshots) {
      PartitionedSnapshot partitionedSnapshot = new PartitionedSnapshot(DataSimpleSnapshot::new);
      for (Entry<Integer, Snapshot> entry : socketSnapshots.entrySet()) {
        partitionedSnapshot.putSnapshot(entry.getKey(), entry.getValue());
      }
      partitionedSnapshot.setLastLogId(snapshotLastLogId);
      partitionedSnapshot.setLastLogTerm(snapshotLastLogTerm);
      return partitionedSnapshot;
    }
  }

  @Override
  public void takeSnapshot() {
    logger.info("Taking snapshots, flushing IoTDB");
    StorageEngine.getInstance().syncCloseAllProcessor();
    logger.info("Taking snapshots, IoTDB is flushed");

    synchronized (socketSnapshots) {
      collectTimeseriesSchemas();

      while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
        Log log = logBuffer.removeFirst();
        snapshotLastLogId = log.getCurrLogIndex();
        snapshotLastLogTerm = log.getCurrLogTerm();

        DataSimpleSnapshot dataSimpleSnapshot =
            (DataSimpleSnapshot) socketSnapshots.computeIfAbsent(PartitionUtils.calculateLogSocket(log,
            partitionTable), socket -> new DataSimpleSnapshot(socketTimeseries.get(socket)));
        dataSimpleSnapshot.add(log);
      }

      logger.info("Snapshot is taken");
    }
  }

  void collectTimeseriesSchemas() {
    socketTimeseries.clear();
    // TODO-Cluster: the collection is re-collected each time to prevent inconsistency when some of
    //  them are removed during two snapshots. Incremental addition or removal may be used to
    //  optimize
    List<MNode> allSgNodes = MManager.getInstance().getAllStorageGroups();
    for (MNode sgNode : allSgNodes) {
      String storageGroupName = sgNode.getFullPath();
      int socket = PartitionUtils.calculateStorageGroupSocket(storageGroupName, 0);

      Collection<MeasurementSchema> schemas = socketTimeseries.computeIfAbsent(socket,
          s -> new HashSet<>());
      MManager.getInstance().collectSeries(sgNode, schemas);
      logger.debug("{} timeseries are snapshot in socket {}", schemas.size(), socket);
    }
  }

  public void setSnapshot(Snapshot snapshot, int socket) {
    synchronized (socketSnapshots) {
      socketSnapshots.put(socket, snapshot);
    }
  }
}
