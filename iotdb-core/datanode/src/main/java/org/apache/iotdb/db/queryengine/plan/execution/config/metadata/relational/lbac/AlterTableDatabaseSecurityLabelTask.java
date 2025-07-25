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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.lbac;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement.AlterDatabaseSecurityLabelStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.statement.DropDatabaseSecurityLabelStatement;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

/**
 * AlterTableDatabaseSecurityLabelTask handles the execution of database security label operations
 * in table model. This task supports both setting and dropping security labels for databases to
 * support Label-Based Access Control (LBAC) functionality in the table model.
 */
public class AlterTableDatabaseSecurityLabelTask implements IConfigTask {

  private final String database;
  private final Map<String, String> securityLabels;
  private final boolean isDropOperation;

  public AlterTableDatabaseSecurityLabelTask(AlterDatabaseSecurityLabelStatement statement) {
    this.database = statement.getDatabase();
    this.securityLabels = statement.getSecurityLabels();
    this.isDropOperation = false;
  }

  public AlterTableDatabaseSecurityLabelTask(DropDatabaseSecurityLabelStatement statement) {
    this.database = statement.getDatabase();
    this.securityLabels = null;
    this.isDropOperation = true;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    if (isDropOperation) {
      return configTaskExecutor.dropDatabaseSecurityLabelTableModel(database);
    } else {
      // Convert security labels map to string representation
      String securityLabelString = convertSecurityLabelsToString(securityLabels);
      return configTaskExecutor.alterDatabaseSecurityLabelTableModel(database, securityLabelString);
    }
  }

  private String convertSecurityLabelsToString(Map<String, String> securityLabels) {
    if (securityLabels == null || securityLabels.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : securityLabels.entrySet()) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }
}
