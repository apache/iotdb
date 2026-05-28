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
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;

public class PreDeleteWritableViewColumnPlan extends PreDeleteColumnPlan {

  public PreDeleteWritableViewColumnPlan() {
    super(ConfigPhysicalPlanType.PreDeleteWritableViewColumn);
  }

  public PreDeleteWritableViewColumnPlan(
      final String database,
      final String tableName,
      final String columnName,
      final String originalDatabase,
      final String originalTableName,
      final String originalColumnName) {
    super(ConfigPhysicalPlanType.PreDeleteWritableViewColumn, database, tableName, columnName);
    this.originalDatabase = originalDatabase;
    this.originalTableName = originalTableName;
    this.originalColumnName = originalColumnName;
  }

  @Override
  protected boolean needOriginal() {
    return true;
  }
}
