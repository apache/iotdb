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

package org.apache.iotdb.confignode.consensus.request.write.database;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SetDataReplicationFactorPlan extends ConfigPhysicalPlan {

  private String database;

  private int dataReplicationFactor;

  public SetDataReplicationFactorPlan() {
    super(ConfigPhysicalPlanType.SetDataReplicationFactor);
  }

  public SetDataReplicationFactorPlan(String database, int dataReplicationFactor) {
    this();
    this.database = database;
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public String getDatabase() {
    return database;
  }

  public int getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    BasicStructureSerDeUtil.write(database, stream);
    stream.writeInt(dataReplicationFactor);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    database = BasicStructureSerDeUtil.readString(buffer);
    dataReplicationFactor = buffer.getInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SetDataReplicationFactorPlan that = (SetDataReplicationFactorPlan) o;
    return dataReplicationFactor == that.dataReplicationFactor && database.equals(that.database);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, dataReplicationFactor);
  }
}
