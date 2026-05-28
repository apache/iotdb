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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AddWritableViewColumnPlan extends AddTableColumnPlan {

  private List<TsTableColumnSchema> originalColumnSchemaList;

  public AddWritableViewColumnPlan() {
    super(ConfigPhysicalPlanType.AddWritableViewColumn);
  }

  public AddWritableViewColumnPlan(
      final String database,
      final String tableName,
      final List<TsTableColumnSchema> columnSchemaList,
      final boolean isRollback,
      final String originalDatabase,
      final String originalTableName,
      final List<TsTableColumnSchema> originalColumnSchemaList) {
    super(
        ConfigPhysicalPlanType.AddWritableViewColumn,
        database,
        tableName,
        columnSchemaList,
        isRollback);
    this.originalDatabase = originalDatabase;
    this.originalTableName = originalTableName;
    this.originalColumnSchemaList = originalColumnSchemaList;
  }

  public List<TsTableColumnSchema> getOriginalColumnSchemaList() {
    return originalColumnSchemaList;
  }

  @Override
  protected boolean needOriginal() {
    return true;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    TsTableColumnSchemaUtil.serialize(originalColumnSchemaList, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    this.originalColumnSchemaList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(buffer);
  }
}
