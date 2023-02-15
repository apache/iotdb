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

package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class DatabaseSchemaTask implements IConfigTask {

  private final DatabaseSchemaStatement databaseSchemaStatement;

  public DatabaseSchemaTask(DatabaseSchemaStatement databaseSchemaStatement) {
    this.databaseSchemaStatement = databaseSchemaStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    switch (databaseSchemaStatement.getSubType()) {
      case CREATE:
        return configTaskExecutor.setDatabase(databaseSchemaStatement);
      case ALTER:
      default:
        return configTaskExecutor.alterDatabase(databaseSchemaStatement);
    }
  }

  /** Construct DatabaseSchema according to statement */
  public static TDatabaseSchema constructStorageGroupSchema(
      DatabaseSchemaStatement databaseSchemaStatement) {
    TDatabaseSchema storageGroupSchema = new TDatabaseSchema();
    storageGroupSchema.setName(databaseSchemaStatement.getStorageGroupPath().getFullPath());
    if (databaseSchemaStatement.getTTL() != null) {
      storageGroupSchema.setTTL(databaseSchemaStatement.getTTL());
    }
    if (databaseSchemaStatement.getSchemaReplicationFactor() != null) {
      storageGroupSchema.setSchemaReplicationFactor(
          databaseSchemaStatement.getSchemaReplicationFactor());
    }
    if (databaseSchemaStatement.getDataReplicationFactor() != null) {
      storageGroupSchema.setDataReplicationFactor(
          databaseSchemaStatement.getDataReplicationFactor());
    }
    if (databaseSchemaStatement.getTimePartitionInterval() != null) {
      storageGroupSchema.setTimePartitionInterval(
          databaseSchemaStatement.getTimePartitionInterval());
    }
    if (databaseSchemaStatement.getSchemaRegionGroupNum() != null) {
      storageGroupSchema.setMinSchemaRegionGroupNum(
          databaseSchemaStatement.getSchemaRegionGroupNum());
    }
    if (databaseSchemaStatement.getDataRegionGroupNum() != null) {
      storageGroupSchema.setMinDataRegionGroupNum(databaseSchemaStatement.getDataRegionGroupNum());
    }
    return storageGroupSchema;
  }
}
