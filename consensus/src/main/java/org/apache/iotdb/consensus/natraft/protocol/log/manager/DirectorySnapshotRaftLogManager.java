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

import java.io.File;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.applier.LogApplier;
import org.apache.iotdb.consensus.natraft.protocol.log.serialization.StableEntryManager;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.DirectorySnapshot;
import org.apache.iotdb.consensus.natraft.protocol.log.snapshot.Snapshot;

public class DirectorySnapshotRaftLogManager extends RaftLogManager {

  private File latestSnapshotDir;

  public DirectorySnapshotRaftLogManager(
      StableEntryManager stableEntryManager,
      LogApplier applier,
      String name,
      IStateMachine stateMachine,
      RaftConfig config) {
    super(stableEntryManager, applier, name, stateMachine, config);
  }

  @Override
  public Snapshot getSnapshot(long minLogIndex) {
    return new DirectorySnapshot(latestSnapshotDir);
  }

  @Override
  public void takeSnapshot() {
    latestSnapshotDir = new File(config.getStorageDir() + File.separator + getName() + "-snapshot-" + System.currentTimeMillis());
    stateMachine.takeSnapshot(latestSnapshotDir);
  }
}
