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
package org.apache.iotdb.confignode.consensus.request.write.partition;

import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Create SchemaPartition by assignedSchemaPartition */
public class CreateSchemaPartitionPlan extends ConfigPhysicalPlan {

  // TODO: Replace this field whit new SchemaPartition
  private Map<String, SchemaPartitionTable> assignedSchemaPartition;

  public CreateSchemaPartitionPlan() {
    super(ConfigPhysicalPlanType.CreateSchemaPartition);
  }

  public Map<String, SchemaPartitionTable> getAssignedSchemaPartition() {
    return assignedSchemaPartition;
  }

  public void setAssignedSchemaPartition(
      Map<String, SchemaPartitionTable> assignedSchemaPartition) {
    this.assignedSchemaPartition = assignedSchemaPartition;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    try {
      TTransport transport = new TIOStreamTransport(stream);
      TBinaryProtocol protocol = new TBinaryProtocol(transport);

      stream.writeShort(getType().getPlanType());

      stream.writeInt(assignedSchemaPartition.size());
      for (Map.Entry<String, SchemaPartitionTable> schemaPartitionTableEntry :
          assignedSchemaPartition.entrySet()) {
        BasicStructureSerDeUtil.write(schemaPartitionTableEntry.getKey(), stream);
        schemaPartitionTableEntry.getValue().serialize(stream, protocol);
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedSchemaPartition = new HashMap<>();

    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
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
    CreateSchemaPartitionPlan that = (CreateSchemaPartitionPlan) o;
    return assignedSchemaPartition.equals(that.assignedSchemaPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedSchemaPartition);
  }
}
