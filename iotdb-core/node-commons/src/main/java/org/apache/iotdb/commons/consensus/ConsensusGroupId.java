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

/** We abstract this class to hide word `ConsensusGroup` for IoTDB StorageEngine/SchemaEngine. */
public abstract class ConsensusGroupId {

  protected int id;

  /**
   * Get specific id.
   *
   * @return id.
   */
  public int getId() {
    return id;
  }

  /**
   * Get specific type.
   *
   * @return type.
   */
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

    private Factory() {
      // Empty constructor
    }

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

    public static ConsensusGroupId createFromString(String groupIdString) {
      ConsensusGroupId groupId;
      if (groupIdString.startsWith(TConsensusGroupType.DataRegion.name())) {
        groupId =
            new DataRegionId(
                Integer.parseInt(
                    groupIdString.substring(
                        TConsensusGroupType.DataRegion.name().length() + 1,
                        groupIdString.length() - 1)));
      } else if (groupIdString.startsWith(TConsensusGroupType.SchemaRegion.name())) {
        groupId =
            new SchemaRegionId(
                Integer.parseInt(
                    groupIdString.substring(
                        TConsensusGroupType.SchemaRegion.name().length() + 1,
                        groupIdString.length() - 1)));
      } else if (groupIdString.startsWith(TConsensusGroupType.ConfigRegion.name())) {
        groupId =
            new ConfigRegionId(
                Integer.parseInt(
                    groupIdString.substring(
                        TConsensusGroupType.ConfigRegion.name().length() + 1,
                        groupIdString.length() - 1)));
      } else {
        throw new IllegalArgumentException("Unrecognized ConsensusGroupId: " + groupIdString);
      }
      return groupId;
    }

    public static ConsensusGroupId createFromTConsensusGroupId(
        TConsensusGroupId tConsensusGroupId) {
      return create(tConsensusGroupId.getType().getValue(), tConsensusGroupId.getId());
    }
  }
}
