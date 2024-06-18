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

package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AddTableColumnPlan extends ConfigPhysicalPlan {

  private String database;

  private String tableName;

  private List<TsTableColumnSchema> columnSchemaList;

  private boolean isRollback;

  public AddTableColumnPlan() {
    super(ConfigPhysicalPlanType.AddTableColumn);
  }

  public AddTableColumnPlan(
      String database,
      String tableName,
      List<TsTableColumnSchema> columnSchemaList,
      boolean isRollback) {
    super(ConfigPhysicalPlanType.AddTableColumn);
    this.database = database;
    this.tableName = tableName;
    this.columnSchemaList = columnSchemaList;
    this.isRollback = isRollback;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<TsTableColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    TsTableColumnSchemaUtil.serialize(columnSchemaList, stream);
    ReadWriteIOUtils.write(isRollback, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.database = ReadWriteIOUtils.readString(buffer);
    this.tableName = ReadWriteIOUtils.readString(buffer);
    this.columnSchemaList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(buffer);
    this.isRollback = ReadWriteIOUtils.readBool(buffer);
  }
}
