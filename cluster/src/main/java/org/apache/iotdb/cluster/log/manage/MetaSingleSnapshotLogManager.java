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
import java.util.Map;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaSingleSnapshotLogManager provides a MetaSimpleSnapshot as snapshot.
 */
public class MetaSingleSnapshotLogManager extends RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(MetaSingleSnapshotLogManager.class);
  private List<Log> snapshot = new ArrayList<>();
  private List<String> storageGroups;
  private Map<String, Long> storageGroupTTL;
  private Map<String, Boolean> userWaterMarkStatus;

  public MetaSingleSnapshotLogManager(LogApplier logApplier) {
    super(new CommittedEntryManager(), new StableEntryManager(), logApplier);
  }

  public void takeSnapshot() {
    //TODO remove useless logs which have been compacted
    try {
      List<Log> entries = committedEntryManager
          .getEntries(committedEntryManager.getFirstIndex(), Long.MAX_VALUE);
      snapshot.addAll(entries);
    } catch (EntryCompactedException e) {
      logger.error("Unexpected error: {}", e.getMessage());
    }
    storageGroups = MManager.getInstance().getAllStorageGroupNames();
    storageGroupTTL = MManager.getInstance().getStorageGroupsTTL();
    try {
      LocalFileAuthorizer authorizer = LocalFileAuthorizer.getInstance();
      userWaterMarkStatus = authorizer.getAllUserWaterMarkStatus();
    } catch (AuthException e) {
      logger.error("get all user water mark status failed", e);
    }
  }

  @Override
  public Snapshot getSnapshot() {
    return new MetaSimpleSnapshot(new ArrayList<>(this.snapshot), storageGroups, storageGroupTTL,
        userWaterMarkStatus);
  }

  public void setSnapshot(SimpleSnapshot snapshot) {
    this.snapshot = snapshot.getSnapshot();
  }
}
