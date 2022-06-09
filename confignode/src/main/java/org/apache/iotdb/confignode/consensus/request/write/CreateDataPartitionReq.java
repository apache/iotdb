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

import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Create DataPartition by assignedDataPartition */
public class CreateDataPartitionReq extends ConfigRequest {

  private Map<String, DataPartitionTable> assignedDataPartition;

  public CreateDataPartitionReq() {
    super(ConfigRequestType.CreateDataPartition);
  }

  public Map<String, DataPartitionTable> getAssignedDataPartition() {
    return assignedDataPartition;
  }

  public void setAssignedDataPartition(Map<String, DataPartitionTable> assignedDataPartition) {
    this.assignedDataPartition = assignedDataPartition;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigRequestType.CreateDataPartition.ordinal());

    buffer.putInt(assignedDataPartition.size());
    assignedDataPartition.forEach(
        (storageGroup, dataPartitionTable) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          dataPartitionTable.serialize(buffer);
        });
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedDataPartition = new HashMap<>();

    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      DataPartitionTable dataPartitionTable = new DataPartitionTable();
      dataPartitionTable.deserialize(buffer);
      assignedDataPartition.put(storageGroup, dataPartitionTable);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateDataPartitionReq that = (CreateDataPartitionReq) o;
    return assignedDataPartition.equals(that.assignedDataPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedDataPartition);
  }
}
