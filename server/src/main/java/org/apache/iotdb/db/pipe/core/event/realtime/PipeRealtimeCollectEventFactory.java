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

package org.apache.iotdb.db.pipe.core.event.realtime;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;

import java.io.File;

public class PipeRealtimeCollectEventFactory {

  private static final TsFileEpochManager TS_FILE_EPOCH_MANAGER = new TsFileEpochManager();

  // TODO: resource control here?
  public static PipeRealtimeCollectEvent createCollectEvent(File tsFile, TsFileResource resource) {
    return TS_FILE_EPOCH_MANAGER.bindPipeTsFileInsertionEvent(
        new PipeTsFileInsertionEvent(tsFile), resource);
  }

  // TODO: resource control here?
  public static PipeRealtimeCollectEvent createCollectEvent(
      InsertNode node, TsFileResource resource) {
    return TS_FILE_EPOCH_MANAGER.bindPipeTabletInsertionEvent(
        new PipeTabletInsertionEvent(node), node, resource);
  }

  private PipeRealtimeCollectEventFactory() {
    // factory class, do not instantiate
  }
}
