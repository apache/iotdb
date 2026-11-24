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

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/** Counts the distinct mod files processed by one region's schema deletion attempt. */
public class SchemaDeletionModProgress {
  private static final String DROP_TABLE_PLAN_NODE_ID_PREFIX = "drop-table-";
  private static final String DELETE_TIMESERIES_PLAN_NODE_ID_PREFIX = "delete-timeseries-";

  private long totalModFileCount = -1;
  private long writtenModFileCount;
  private Set<File> writtenModFiles;

  public static PlanNodeId dropTablePlanNodeId(final long procedureId) {
    // PlanNodeId already travels through consensus. Reuse it so observability does not change
    // the deletion plan, WAL or DAL serialization formats.
    return new PlanNodeId(DROP_TABLE_PLAN_NODE_ID_PREFIX + procedureId);
  }

  public static PlanNodeId deleteTimeSeriesPlanNodeId(final long procedureId) {
    return new PlanNodeId(DELETE_TIMESERIES_PLAN_NODE_ID_PREFIX + procedureId);
  }

  public static long procedureId(final PlanNodeId planNodeId) {
    final String id = planNodeId.getId();
    final String prefix;
    if (id.startsWith(DROP_TABLE_PLAN_NODE_ID_PREFIX)) {
      prefix = DROP_TABLE_PLAN_NODE_ID_PREFIX;
    } else if (id.startsWith(DELETE_TIMESERIES_PLAN_NODE_ID_PREFIX)) {
      prefix = DELETE_TIMESERIES_PLAN_NODE_ID_PREFIX;
    } else {
      return -1;
    }
    try {
      return Long.parseLong(id.substring(prefix.length()));
    } catch (final NumberFormatException e) {
      return -1;
    }
  }

  public synchronized void setTotalModFileCount(final long totalModFileCount) {
    this.totalModFileCount = totalModFileCount;
  }

  public static SchemaDeletionModProgress forTimeSeries() {
    final SchemaDeletionModProgress progress = new SchemaDeletionModProgress();
    progress.writtenModFiles = new HashSet<>();
    return progress;
  }

  public synchronized void modFileWritten(final ModificationFile modFile) {
    if (writtenModFiles == null || writtenModFiles.add(modFile.getFile())) {
      ++writtenModFileCount;
    }
  }

  public synchronized void finishTimeSeriesDeletion(final boolean succeeded) {
    if (succeeded) {
      totalModFileCount = writtenModFileCount;
    }
    writtenModFiles = null;
  }

  public synchronized TDropTableModProgress snapshot() {
    final TDropTableModProgress progress = new TDropTableModProgress(writtenModFileCount);
    if (totalModFileCount >= 0) {
      progress.setRemainingModFileCount(totalModFileCount - writtenModFileCount);
    }
    return progress;
  }
}
