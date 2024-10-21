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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;

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
  public static TDatabaseSchema constructDatabaseSchema(
      final DatabaseSchemaStatement databaseSchemaStatement) {
    final TDatabaseSchema databaseSchema = new TDatabaseSchema();
    databaseSchema.setName(databaseSchemaStatement.getDatabasePath().getFullPath());
    if (databaseSchemaStatement.getTtl() != null) {
      databaseSchema.setTTL(databaseSchemaStatement.getTtl());
    }
    if (databaseSchemaStatement.getSchemaReplicationFactor() != null) {
      databaseSchema.setSchemaReplicationFactor(
          databaseSchemaStatement.getSchemaReplicationFactor());
    }
    if (databaseSchemaStatement.getDataReplicationFactor() != null) {
      databaseSchema.setDataReplicationFactor(databaseSchemaStatement.getDataReplicationFactor());
    }
    if (databaseSchemaStatement.getTimePartitionInterval() != null) {
      databaseSchema.setTimePartitionInterval(databaseSchemaStatement.getTimePartitionInterval());
    }
    if (databaseSchemaStatement.getSchemaRegionGroupNum() != null) {
      databaseSchema.setMinSchemaRegionGroupNum(databaseSchemaStatement.getSchemaRegionGroupNum());
    }
    if (databaseSchemaStatement.getDataRegionGroupNum() != null) {
      databaseSchema.setMinDataRegionGroupNum(databaseSchemaStatement.getDataRegionGroupNum());
    }
    databaseSchema.setIsTableModel(false);
    return databaseSchema;
  }
}
