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

package org.apache.iotdb.db.mpp.plan.execution.config.sys;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

public class FlushTask implements IConfigTask {

  private final FlushStatement flushStatement;

  public FlushTask(FlushStatement flushStatement) {
    this.flushStatement = flushStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    TFlushReq tFlushReq = new TFlushReq();
    List<String> storageGroups = new ArrayList<>();
    if (flushStatement.getStorageGroups() != null) {
      for (PartialPath partialPath : flushStatement.getStorageGroups()) {
        storageGroups.add(partialPath.getFullPath());
      }
      tFlushReq.setStorageGroups(storageGroups);
    }
    if (flushStatement.isSeq() != null) {
      tFlushReq.setIsSeq(flushStatement.isSeq().toString());
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return configTaskExecutor.flush(tFlushReq, flushStatement.isOnCluster());
  }
}
