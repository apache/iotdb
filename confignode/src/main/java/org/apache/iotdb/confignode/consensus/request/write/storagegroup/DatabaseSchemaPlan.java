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

package org.apache.iotdb.confignode.consensus.request.write.storagegroup;

import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DatabaseSchemaPlan extends ConfigPhysicalPlan {

  private TDatabaseSchema schema;

  public DatabaseSchemaPlan(ConfigPhysicalPlanType planType) {
    super(planType);
    this.schema = new TDatabaseSchema();
  }

  public DatabaseSchemaPlan(ConfigPhysicalPlanType planType, TDatabaseSchema schema) {
    this(planType);
    this.schema = schema;
  }

  public TDatabaseSchema getSchema() {
    return schema;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftConfigNodeSerDeUtils.serializeTStorageGroupSchema(schema, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    schema = ThriftConfigNodeSerDeUtils.deserializeTStorageGroupSchema(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabaseSchemaPlan that = (DatabaseSchemaPlan) o;
    return schema.equals(that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }
}
