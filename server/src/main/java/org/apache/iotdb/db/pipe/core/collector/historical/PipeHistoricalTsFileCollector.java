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

package org.apache.iotdb.db.pipe.core.collector.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.pipe.core.collector.PipeCollector;
import org.apache.iotdb.db.pipe.core.event.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class PipeHistoricalTsFileCollector implements PipeCollector {
  private final AtomicBoolean hasBeenStarted;
  private final String dataRegionId;
  private Queue<PipeTsFileInsertionEvent> pendingQueue;

  public PipeHistoricalTsFileCollector(String dataRegionId) {
    this.hasBeenStarted = new AtomicBoolean(false);
    this.dataRegionId = dataRegionId;
  }

  @Override
  public void start() {
    hasBeenStarted.set(true);

    DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    dataRegion.writeLock("Pipe: collect historical TsFile");
    try {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
      TsFileManager tsFileManager = dataRegion.getTsFileManager();

      tsFileManager.readLock();
      try {
        pendingQueue = new ArrayDeque<>(tsFileManager.size(true) + tsFileManager.size(false));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(true).stream()
                .map(o -> new PipeTsFileInsertionEvent(o.getTsFile()))
                .collect(Collectors.toList()));
        pendingQueue.addAll(
            tsFileManager.getTsFileList(false).stream()
                .map(o -> new PipeTsFileInsertionEvent(o.getTsFile()))
                .collect(Collectors.toList()));
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
    }
  }

  @Override
  public boolean hasBeenStarted() {
    return hasBeenStarted.get();
  }

  @Override
  public Event supply() {
    if (pendingQueue == null) {
      return null;
    }

    return pendingQueue.poll();
  }

  @Override
  public void close() {
    if (pendingQueue != null) {
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
