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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaNodeListeningQueue {

  private int schemaRegionId;

  private final ConcurrentIterableLinkedQueue<PlanNode> queue =
      new ConcurrentIterableLinkedQueue<>();

  private SchemaNodeListeningQueue(Integer schemaRegionId) {
    this.schemaRegionId = schemaRegionId;
  }

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToNode(PlanNode node) {
    if (queue.hasAnyIterators()) {
      queue.add(node);
    }
  }

  public ConcurrentIterableLinkedQueue<PlanNode>.DynamicIterator newIterator(int index) {
    return queue.iterateFrom(index);
  }

  public void returnIterator(ConcurrentIterableLinkedQueue<PlanNode>.DynamicIterator itr) {
    itr.close();
    if (!queue.hasAnyIterators()) {
      queue.clear();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  private void clear() {
    queue.clear();
  }

  /////////////////////////// Singleton ///////////////////////////

  public static SchemaNodeListeningQueue getInstance(Integer regionId) {
    return SchemaNodeListeningQueue.InstanceHolder.getOrCreateInstance(regionId);
  }

  private static class InstanceHolder {

    private static final Map<Integer, SchemaNodeListeningQueue> INSTANCE_MAP =
        new ConcurrentHashMap<>();

    public static SchemaNodeListeningQueue getOrCreateInstance(Integer key) {
      return INSTANCE_MAP.computeIfAbsent(key, k -> new SchemaNodeListeningQueue(key));
    }

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
