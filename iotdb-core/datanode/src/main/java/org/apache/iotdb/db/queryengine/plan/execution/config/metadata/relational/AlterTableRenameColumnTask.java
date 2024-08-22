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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;

import com.google.common.util.concurrent.ListenableFuture;

public class AlterTableRenameColumnTask implements IConfigTask {

  private final String database;

  private final String tableName;

  private final String oldName;

  private final String newName;

  private final String queryId;

  private final boolean tableIfExists;

  private final boolean columnIfExists;

  public AlterTableRenameColumnTask(
      String database,
      final String tableName,
      final String oldName,
      final String newName,
      final String queryId,
      final boolean tableIfExists,
      final boolean columnIfExists) {
    database = PathUtils.qualifyDatabaseName(database);
    this.database = database;
    this.tableName = tableName;
    this.oldName = oldName;
    this.newName = newName;
    this.queryId = queryId;
    this.tableIfExists = tableIfExists;
    this.columnIfExists = columnIfExists;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.alterTableRenameColumn(
        database, tableName, oldName, newName, queryId, tableIfExists, columnIfExists);
  }
}
