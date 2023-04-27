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

package org.apache.iotdb.commons.consensus;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;

import java.util.Objects;

// we abstract this class to hide word `ConsensusGroup` for IoTDB StorageEngine/SchemaEngine
public abstract class ConsensusGroupId {

  protected int id;

  // return specific id
  public int getId() {
    return id;
  }

  // return specific type
  public abstract TConsensusGroupType getType();

  public TConsensusGroupId convertToTConsensusGroupId() {
    return new TConsensusGroupId(getType(), getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsensusGroupId that = (ConsensusGroupId) o;
    return getId() == that.getId() && getType() == that.getType();
  }

  @Override
  public String toString() {
    return String.format("%s[%d]", getType(), getId());
  }

  public static class Factory {

    public static ConsensusGroupId create(int type, int id) {
      ConsensusGroupId groupId;
      if (type == TConsensusGroupType.DataRegion.getValue()) {
        groupId = new DataRegionId(id);
      } else if (type == TConsensusGroupType.SchemaRegion.getValue()) {
        groupId = new SchemaRegionId(id);
      } else if (type == TConsensusGroupType.ConfigRegion.getValue()) {
        groupId = new ConfigRegionId(id);
      } else {
        throw new IllegalArgumentException(
            "Unrecognized TConsensusGroupType: " + type + " with id = " + id);
      }
      return groupId;
    }

    public static ConsensusGroupId createFromTConsensusGroupId(
        TConsensusGroupId tConsensusGroupId) {
      return create(tConsensusGroupId.getType().getValue(), tConsensusGroupId.getId());
    }
  }

  public static String formatTConsensusGroupId(TConsensusGroupId groupId) {
    StringBuilder format = new StringBuilder();

    switch (groupId.getType()) {
      case SchemaRegion:
        format.append("SchemaRegion");
        break;
      case DataRegion:
        format.append("DataRegion");
        break;
      case ConfigRegion:
        format.append("ConfigRegion");
        break;
    }

    format.append("(").append(groupId.getId()).append(")");

    return format.toString();
  }
}
