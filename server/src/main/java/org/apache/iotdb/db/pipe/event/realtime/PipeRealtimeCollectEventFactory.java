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

package org.apache.iotdb.db.pipe.event.realtime;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.collector.realtime.epoch.TsFileEpochManager;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.wal.utils.WALEntryHandler;

public class PipeRealtimeCollectEventFactory {

  private static final TsFileEpochManager TS_FILE_EPOCH_MANAGER = new TsFileEpochManager();

  public static PipeRealtimeCollectEvent createCollectEvent(TsFileResource resource) {
    return TS_FILE_EPOCH_MANAGER.bindPipeTsFileInsertionEvent(
        new PipeTsFileInsertionEvent(resource), resource);
  }

  public static PipeRealtimeCollectEvent createCollectEvent(
      WALEntryHandler walEntryHandler, InsertNode insertNode, TsFileResource resource) {
    return TS_FILE_EPOCH_MANAGER.bindPipeInsertNodeTabletInsertionEvent(
        new PipeInsertNodeTabletInsertionEvent(
            walEntryHandler, insertNode.getProgressIndex(), insertNode.isAligned()),
        insertNode,
        resource);
  }

  private PipeRealtimeCollectEventFactory() {
    // factory class, do not instantiate
  }
}
