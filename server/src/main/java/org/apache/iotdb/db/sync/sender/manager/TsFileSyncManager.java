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
package org.apache.iotdb.db.sync.sender.manager;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.dataregion.StorageGroupManager;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * TsFileSyncManager is designed for collect all history TsFiles(i.e. before the pipe start time,
 * all tsfiles whose memtable is set to null.), and realtime tsfiles for registered {@linkplain
 * TsFilePipe}.
 */
public class TsFileSyncManager {
  private static final Logger logger = LoggerFactory.getLogger(TsFileSyncManager.class);

  private TsFilePipe syncPipe;

  /** singleton */
  private TsFileSyncManager() {}

  private static class TsFileSyncManagerHolder {
    private static final TsFileSyncManager INSTANCE = new TsFileSyncManager();

    private TsFileSyncManagerHolder() {}
  }

  public static TsFileSyncManager getInstance() {
    return TsFileSyncManager.TsFileSyncManagerHolder.INSTANCE;
  }

  /** register */
  public void registerSyncTask(TsFilePipe syncPipe) {
    this.syncPipe = syncPipe;
  }

  public void deregisterSyncTask() {
    this.syncPipe = null;
  }

  public boolean isEnableSync() {
    return syncPipe != null;
  }

  public void clear() {
    syncPipe = null;
  }

  /** tsfile */
  public void collectRealTimeDeletion(Deletion deletion) {
    syncPipe.collectRealTimeDeletion(deletion);
  }

  public void collectRealTimeTsFile(File tsFile) {
    syncPipe.collectRealTimeTsFile(tsFile);
  }

  public void collectRealTimeResource(File tsFile) {
    syncPipe.collectRealTimeResource(tsFile);
  }

  public List<File> registerAndCollectHistoryTsFile(TsFilePipe syncPipe, long dataStartTime) {
    registerSyncTask(syncPipe);

    List<File> historyTsFiles = new ArrayList<>();
    Iterator<Map.Entry<PartialPath, StorageGroupManager>> sgIterator =
        StorageEngine.getInstance().getProcessorMap().entrySet().iterator();
    while (sgIterator.hasNext()) {
      historyTsFiles.addAll(
          sgIterator.next().getValue().collectHistoryTsFileForSync(dataStartTime));
    }

    return historyTsFiles;
  }

  public File createHardlink(File tsFile, long modsOffset) {
    return syncPipe.createHistoryTsFileHardlink(tsFile, modsOffset);
  }
}
