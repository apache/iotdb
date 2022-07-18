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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class SetStorageGroupTask implements IConfigTask {

  private final SetStorageGroupStatement setStorageGroupStatement;

  public SetStorageGroupTask(SetStorageGroupStatement setStorageGroupStatement) {
    this.setStorageGroupStatement = setStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return configTaskExecutor.setStorageGroup(setStorageGroupStatement);
  }

  /** construct set storage group schema according to statement */
  public static TStorageGroupSchema constructStorageGroupSchema(
      SetStorageGroupStatement setStorageGroupStatement) {
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    storageGroupSchema.setName(setStorageGroupStatement.getStorageGroupPath().getFullPath());
    if (setStorageGroupStatement.getTTL() != null) {
      storageGroupSchema.setTTL(setStorageGroupStatement.getTTL());
    }
    if (setStorageGroupStatement.getSchemaReplicationFactor() != null) {
      storageGroupSchema.setSchemaReplicationFactor(
          setStorageGroupStatement.getSchemaReplicationFactor());
    }
    if (setStorageGroupStatement.getDataReplicationFactor() != null) {
      storageGroupSchema.setDataReplicationFactor(
          setStorageGroupStatement.getDataReplicationFactor());
    }
    if (setStorageGroupStatement.getTimePartitionInterval() != null) {
      storageGroupSchema.setTimePartitionInterval(
          setStorageGroupStatement.getTimePartitionInterval());
    }
    return storageGroupSchema;
  }
}
