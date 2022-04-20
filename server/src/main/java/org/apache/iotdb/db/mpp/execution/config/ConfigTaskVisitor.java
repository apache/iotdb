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

package org.apache.iotdb.db.mpp.execution.config;

import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

public class ConfigTaskVisitor
    extends StatementVisitor<IConfigTask, ConfigTaskVisitor.TaskContext> {

  public IConfigTask visitStatement(Statement statement, TaskContext context) {
    throw new NotImplementedException("ConfigTask is not implemented for: " + statement);
  }

  public IConfigTask visitSetStorageGroup(SetStorageGroupStatement statement, TaskContext context) {
    return new SetStorageGroupTask(statement);
  }

  public static class TaskContext {}
}
