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

package org.apache.iotdb.cluster.log.manage;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaSingleSnapshotLogManager provides a MetaSimpleSnapshot as snapshot.
 */
public class MetaSingleSnapshotLogManager extends MemoryLogManager {

  private static final Logger logger = LoggerFactory.getLogger(MetaSingleSnapshotLogManager.class);
  private List<Log> snapshot = new ArrayList<>();
  private List<String> storageGroups;

  public MetaSingleSnapshotLogManager(LogApplier logApplier) {
    super(logApplier);
  }

  @Override
  public void takeSnapshot() {
    snapshot.addAll(removeAndReturnCommittedLogs());
    storageGroups = MManager.getInstance().getAllStorageGroupNames();
  }

  @Override
  public Snapshot getSnapshot() {
    return new MetaSimpleSnapshot(new ArrayList<>(this.snapshot), storageGroups);
  }

  public void setSnapshot(SimpleSnapshot snapshot) {
    this.snapshot = snapshot.getSnapshot();
  }
}
