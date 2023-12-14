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

package org.apache.iotdb.db.queryengine.load.scheduler;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;

public class LoadTsFileSchedulerManager {
  private LoadTsFileSplitterManager splitterManager;

  private LoadTsFileSchedulerManager() {}

  public LoadTsFileSchedulerNew submit(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IPartitionFetcher partitionFetcher,
      boolean isGeneratedByPipe) {
    if (splitterManager == null) {
      splitterManager = new LoadTsFileSplitterManager();
    }
    return new LoadTsFileSchedulerNew(
        distributedQueryPlan,
        queryContext,
        stateMachine,
        partitionFetcher,
        isGeneratedByPipe,
        splitterManager);
  }

  private static class LoadTsFileSchedulerManagerHolder {
    private static final LoadTsFileSchedulerManager INSTANCE = new LoadTsFileSchedulerManager();
  }

  public static LoadTsFileSchedulerManager getInstance() {
    return LoadTsFileSchedulerManagerHolder.INSTANCE;
  }
}
