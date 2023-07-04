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

import org.apache.iotdb.commons.partition.DataPartitionTable;
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

/** Create DataPartition by assignedDataPartition. */
public class CreateDataPartitionPlan extends ConfigPhysicalPlan {

  private Map<String, DataPartitionTable> assignedDataPartition;

  public CreateDataPartitionPlan() {
    super(ConfigPhysicalPlanType.CreateDataPartition);
  }

  public Map<String, DataPartitionTable> getAssignedDataPartition() {
    return assignedDataPartition;
  }

  public void setAssignedDataPartition(Map<String, DataPartitionTable> assignedDataPartition) {
    this.assignedDataPartition = assignedDataPartition;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    try {
      TTransport transport = new TIOStreamTransport(stream);
      TBinaryProtocol protocol = new TBinaryProtocol(transport);

      stream.writeShort(getType().getPlanType());

      stream.writeInt(assignedDataPartition.size());
      for (Map.Entry<String, DataPartitionTable> dataPartitionTableEntry :
          assignedDataPartition.entrySet()) {
        BasicStructureSerDeUtil.write(dataPartitionTableEntry.getKey(), stream);
        dataPartitionTableEntry.getValue().serialize(stream, protocol);
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedDataPartition = new HashMap<>();

    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      DataPartitionTable dataPartitionTable = new DataPartitionTable();
      dataPartitionTable.deserialize(buffer);
      assignedDataPartition.put(storageGroup, dataPartitionTable);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateDataPartitionPlan that = (CreateDataPartitionPlan) o;
    return assignedDataPartition.equals(that.assignedDataPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedDataPartition);
  }
}
