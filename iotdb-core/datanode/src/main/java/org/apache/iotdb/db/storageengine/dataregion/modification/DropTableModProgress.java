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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;

/** Counts the distinct mod files processed by one region's current DROP TABLE attempt. */
public class DropTableModProgress {
  private static final String PLAN_NODE_ID_PREFIX = "drop-table-";

  private long totalModFileCount = -1;
  private long writtenModFileCount;

  public static PlanNodeId planNodeId(final long procedureId) {
    // PlanNodeId already travels through consensus. Reuse it so observability does not change
    // the deletion plan, WAL or DAL serialization formats.
    return new PlanNodeId(PLAN_NODE_ID_PREFIX + procedureId);
  }

  public static long procedureId(final PlanNodeId planNodeId) {
    final String id = planNodeId.getId();
    if (!id.startsWith(PLAN_NODE_ID_PREFIX)) {
      return -1;
    }
    try {
      return Long.parseLong(id.substring(PLAN_NODE_ID_PREFIX.length()));
    } catch (final NumberFormatException e) {
      return -1;
    }
  }

  public synchronized void setTotalModFileCount(final long totalModFileCount) {
    this.totalModFileCount = totalModFileCount;
  }

  public synchronized void modFileWritten() {
    ++writtenModFileCount;
  }

  public synchronized TDropTableModProgress snapshot() {
    final TDropTableModProgress progress = new TDropTableModProgress(writtenModFileCount);
    if (totalModFileCount >= 0) {
      progress.setRemainingModFileCount(totalModFileCount - writtenModFileCount);
    }
    return progress;
  }
}
