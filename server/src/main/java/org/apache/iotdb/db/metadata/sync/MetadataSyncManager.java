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
package org.apache.iotdb.db.metadata.sync;

import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class MetadataSyncManager {

  private TsFilePipe syncPipe = null;

  private static class MetadataSyncManagerHolder {

    private MetadataSyncManagerHolder() {
      // allowed to do nothing
    }

    private static final MetadataSyncManager INSTANCE = new MetadataSyncManager();
  }

  public static MetadataSyncManager getInstance() {
    return MetadataSyncManager.MetadataSyncManagerHolder.INSTANCE;
  }

  public void registerSyncTask(TsFilePipe syncPipe) {
    this.syncPipe = syncPipe;
  }

  public void deregisterSyncTask() {
    this.syncPipe = null;
  }

  public boolean isEnableSync() {
    return syncPipe != null;
  }

  public void syncMetadataPlan(PhysicalPlan plan) {
    syncPipe.collectRealTimeMetaData(plan);
  }

  public void clear() {
    this.syncPipe = null;
  }
}
