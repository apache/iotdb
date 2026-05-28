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

package com.timecho.iotdb.confignode.consensus.request.write.table.view.writable;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;

import javax.annotation.Nullable;

public class SetWritableViewColumnCommentPlan extends SetTableColumnCommentPlan {

  public SetWritableViewColumnCommentPlan() {
    super(ConfigPhysicalPlanType.SetWritableViewColumnComment);
  }

  public SetWritableViewColumnCommentPlan(
      final String database,
      final String tableName,
      final String columnName,
      final String comment,
      final @Nullable String originalDatabase,
      final @Nullable String originalTableName,
      final @Nullable String originalColumnName) {
    super(
        ConfigPhysicalPlanType.SetWritableViewColumnComment,
        database,
        tableName,
        columnName,
        comment);
    this.originalDatabase = originalDatabase;
    this.originalTableName = originalTableName;
    this.originalColumnName = originalColumnName;
  }
}
