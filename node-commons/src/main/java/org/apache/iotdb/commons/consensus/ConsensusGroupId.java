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

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ConsensusGroupId {

  // contains specific id and type
  void serializeImpl(ByteBuffer buffer);

  // only deserialize specific id
  void deserializeImpl(ByteBuffer buffer);

  // return specific id
  int getId();

  void setId(int id);

  // return specific type
  GroupType getType();

  class Factory {
    public static ConsensusGroupId create(ByteBuffer buffer) throws IOException {
      int index = buffer.get();
      if (index >= GroupType.values().length) {
        throw new IOException("unrecognized id type " + index);
      }
      GroupType type = GroupType.values()[index];
      ConsensusGroupId groupId = createEmpty(type);
      groupId.deserializeImpl(buffer);
      return groupId;
    }

    public static ConsensusGroupId createEmpty(GroupType type) {
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

    public static ConsensusGroupId create(int id, GroupType type) {
      ConsensusGroupId groupId = createEmpty(type);
      groupId.setId(id);
      return groupId;
    }
  }
}
