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

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;

public class AlterColumnDataTypeTask extends AbstractAlterOrDropTableTask {
  private final String columnName;
  private final TSDataType newType;
  private final boolean ifColumnExists;

  public AlterColumnDataTypeTask(
      String database,
      String tableName,
      String queryId,
      boolean tableIfExists,
      boolean ifColumnExists,
      String columnName,
      TSDataType newType,
      final boolean view) {
    super(database, tableName, queryId, tableIfExists, view);
    this.columnName = columnName;
    this.newType = newType;
    this.ifColumnExists = ifColumnExists;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.alterColumnDataType(
        database, tableName, columnName, newType, queryId, tableIfExists, ifColumnExists, view);
  }
}
