/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.partition;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RegionReplicaSet {
  private ConsensusGroupId id;
  private List<DataNodeLocation> dataNodeList;

  public RegionReplicaSet() {}

  public RegionReplicaSet(ConsensusGroupId id, List<DataNodeLocation> dataNodeList) {
    this.id = id;
    this.dataNodeList = dataNodeList;
  }

  public List<DataNodeLocation> getDataNodeList() {
    return dataNodeList;
  }

  public void setDataNodeList(List<DataNodeLocation> dataNodeList) {
    this.dataNodeList = dataNodeList;
  }

  public ConsensusGroupId getId() {
    return id;
  }

  public void setId(ConsensusGroupId id) {
    this.id = id;
  }

  public String toString() {
    return String.format("RegionReplicaSet[%s-%s]: %s", id.getType(), id, dataNodeList);
  }

  public void serializeImpl(ByteBuffer buffer) {
    id.serializeImpl(buffer);
    buffer.putInt(dataNodeList.size());
    dataNodeList.forEach(
        dataNode -> {
          dataNode.serializeImpl(buffer);
        });
  }

  public void deserializeImpl(ByteBuffer buffer) throws IOException {
    id = ConsensusGroupId.Factory.create(buffer);

    int size = buffer.getInt();
    // We should always make dataNodeList as a new Object when deserialization
    dataNodeList = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      DataNodeLocation dataNode = new DataNodeLocation();
      dataNode.deserializeImpl(buffer);
      dataNodeList.add(dataNode);
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
    RegionReplicaSet that = (RegionReplicaSet) o;
    return Objects.equals(id, that.id) && Objects.equals(dataNodeList, that.dataNodeList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, dataNodeList);
  }
}
