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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SetTableColumnPropertiesPlan extends AbstractTablePlan {

  private String columnName;
  private Map<String, String> properties;
  private boolean isRollback;

  public SetTableColumnPropertiesPlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  public SetTableColumnPropertiesPlan(
      final String database,
      final String tableName,
      final String columnName,
      final Map<String, String> properties,
      final boolean isRollback) {
    super(ConfigPhysicalPlanType.SetTableColumnProperties, database, tableName);
    this.columnName = columnName;
    this.properties = properties;
    this.isRollback = isRollback;
  }

  public String getColumnName() {
    return columnName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(columnName, stream);
    ReadWriteIOUtils.write(properties, stream);
    ReadWriteIOUtils.write(isRollback, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    this.columnName = ReadWriteIOUtils.readString(buffer);
    this.properties = ReadWriteIOUtils.readMap(buffer);
    this.isRollback = ReadWriteIOUtils.readBool(buffer);
  }
}
