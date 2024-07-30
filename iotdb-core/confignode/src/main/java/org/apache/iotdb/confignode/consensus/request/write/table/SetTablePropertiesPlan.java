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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SetTablePropertiesPlan extends ConfigPhysicalPlan {

  private String database;

  private String tableName;

  private Map<String, String> properties;

  public SetTablePropertiesPlan() {
    super(ConfigPhysicalPlanType.SetTableProperties);
  }

  public SetTablePropertiesPlan(
      final String database, final String tableName, final Map<String, String> properties) {
    super(ConfigPhysicalPlanType.SetTableProperties);
    this.database = database;
    this.tableName = tableName;
    this.properties = properties;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(properties, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    this.database = ReadWriteIOUtils.readString(buffer);
    this.tableName = ReadWriteIOUtils.readString(buffer);
    this.properties = ReadWriteIOUtils.readMap(buffer);
  }
}
