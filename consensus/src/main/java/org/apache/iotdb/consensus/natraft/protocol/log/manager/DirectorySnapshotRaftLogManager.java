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

package org.apache.iotdb.consensus.natraft.protocol.log.manager;

import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.LogApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization.StableEntryManager;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.DirectorySnapshot;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.Snapshot;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DirectorySnapshotRaftLogManager extends RaftLogManager {

  private File latestSnapshotDir;
  private long snapshotIndex;
  private long snapshotTerm;
  DirectorySnapshot directorySnapshot;

  public DirectorySnapshotRaftLogManager(
      StableEntryManager stableEntryManager,
      LogApplier applier,
      String name,
      IStateMachine stateMachine,
      RaftConfig config,
      Consumer<List<Entry>> unappliedEntryExaminer,
      Supplier<Long> safeIndexProvider,
      Consumer<Entry> entryRecycler) {
    super(
        stableEntryManager,
        applier,
        name,
        stateMachine,
        config,
        unappliedEntryExaminer,
        safeIndexProvider,
        entryRecycler);
  }

  @Override
  public Snapshot getSnapshot(long minLogIndex) {
    return directorySnapshot;
  }

  @Override
  public void takeSnapshot(RaftMember member) {
    latestSnapshotDir =
        new File(
            config.getStorageDir()
                + File.separator
                + getName()
                + "-snapshot-"
                + System.currentTimeMillis());
    List<Peer> currNodes;
    try {
      lock.readLock().lock();
      snapshotIndex = getAppliedIndex();
      snapshotTerm = getAppliedTerm();
      currNodes = member.getAllNodes();
    } finally {
      lock.readLock().unlock();
    }
    stateMachine.takeSnapshot(latestSnapshotDir);
    List<Path> snapshotFiles = stateMachine.getSnapshotFiles(latestSnapshotDir);
    directorySnapshot = new DirectorySnapshot(latestSnapshotDir, snapshotFiles, currNodes);
    directorySnapshot.setLastLogIndex(snapshotIndex);
    directorySnapshot.setLastLogTerm(snapshotTerm);
  }
}
