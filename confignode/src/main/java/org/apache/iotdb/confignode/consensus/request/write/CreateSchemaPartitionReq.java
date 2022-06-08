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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Create SchemaPartition by assignedSchemaPartition */
public class CreateSchemaPartitionReq extends ConfigRequest {

  // TODO: Replace this field whit new SchemaPartition
  private Map<String, SchemaPartitionTable> assignedSchemaPartition;

  public CreateSchemaPartitionReq() {
    super(ConfigRequestType.CreateSchemaPartition);
  }

  public Map<String, SchemaPartitionTable> getAssignedSchemaPartition() {
    return assignedSchemaPartition;
  }

  public void setAssignedSchemaPartition(
      Map<String, SchemaPartitionTable> assignedSchemaPartition) {
    this.assignedSchemaPartition = assignedSchemaPartition;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateSchemaPartition.ordinal());

    buffer.putInt(assignedSchemaPartition.size());
    assignedSchemaPartition.forEach(
        (storageGroup, schemaPartitionTable) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          schemaPartitionTable.serialize(buffer);
        });
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedSchemaPartition = new HashMap<>();

    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      SchemaPartitionTable schemaPartitionTable = new SchemaPartitionTable();
      schemaPartitionTable.deserialize(buffer);
      assignedSchemaPartition.put(storageGroup, schemaPartitionTable);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateSchemaPartitionReq that = (CreateSchemaPartitionReq) o;
    return assignedSchemaPartition.equals(that.assignedSchemaPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedSchemaPartition);
  }
}
