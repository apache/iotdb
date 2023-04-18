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

package org.apache.iotdb.db.pipe.core.collector.realtime.listener;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.collector.realtime.assigner.DataRegionChangeDataAssigner;
import org.apache.iotdb.db.pipe.core.event.factory.PipeEventFactory;

import java.util.concurrent.ConcurrentMap;

public class PipeChangeDataCaptureListener {
  private ConcurrentMap<String, DataRegionChangeDataAssigner> id2Caches;

  private PipeChangeDataCaptureListener() {}

  public void setDataRegionChangeDataCaches(
      ConcurrentMap<String, DataRegionChangeDataAssigner> id2Caches) {
    this.id2Caches = id2Caches;
  }

  public void collectTsFile(String dataRegionId, TsFileResource resource) {
    if (id2Caches == null || !id2Caches.containsKey(dataRegionId)) {
      return;
    }

    id2Caches.computeIfPresent(
        dataRegionId,
        (k, v) ->
            v.publishCollectorEvent(
                PipeEventFactory.createCollectEvent(
                    PipeEventFactory.createTsFileInsertionEvent(resource.getTsFile()), resource)));
  }

  public void collectPlanNode(String dataRegionId, InsertNode node, TsFileResource resource) {
    if (id2Caches == null || !id2Caches.containsKey(dataRegionId)) {
      return;
    }

    id2Caches.computeIfPresent(
        dataRegionId,
        (k, v) ->
            v.publishCollectorEvent(
                PipeEventFactory.createCollectEvent(
                    PipeEventFactory.createTabletInsertEvent(node), node, resource)));
  }

  public static PipeChangeDataCaptureListener getInstance() {
    return PipeChangeDataCaptureListenerHolder.INSTANCE;
  }

  private static class PipeChangeDataCaptureListenerHolder {
    private static final PipeChangeDataCaptureListener INSTANCE =
        new PipeChangeDataCaptureListener();
  }
}
