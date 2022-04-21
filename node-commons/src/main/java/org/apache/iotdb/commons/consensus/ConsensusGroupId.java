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

public interface ConsensusGroupId {

  // return specific id
  int getId();

  void setId(int id);

  // return specific type
  TConsensusGroupType getType();

  class Factory {
    public static ConsensusGroupId createEmpty(TConsensusGroupType type) {
      ConsensusGroupId groupId;
      switch (type) {
        case DataRegion:
          groupId = new DataRegionId();
          break;
        case SchemaRegion:
          groupId = new SchemaRegionId();
          break;
        case PartitionRegion:
          groupId = new PartitionRegionId();
          break;
        default:
          throw new IllegalArgumentException("unrecognized id type " + type);
      }
      return groupId;
    }

    public static ConsensusGroupId convertFromTConsensusGroupId(
        TConsensusGroupId tConsensusGroupId) {
      ConsensusGroupId groupId = createEmpty(tConsensusGroupId.getType());
      groupId.setId(tConsensusGroupId.getId());
      return groupId;
    }

    public static TConsensusGroupId convertToTConsensusGroupId(ConsensusGroupId consensusGroupId) {
      TConsensusGroupId result = new TConsensusGroupId();
      if (consensusGroupId instanceof SchemaRegionId) {
        result.setType(TConsensusGroupType.SchemaRegion);
      } else if (consensusGroupId instanceof DataRegionId) {
        result.setType(TConsensusGroupType.DataRegion);
      } else if (consensusGroupId instanceof PartitionRegionId) {
        result.setType(TConsensusGroupType.PartitionRegion);
      }
      result.setId(consensusGroupId.getId());
      return result;
    }
  }
}
