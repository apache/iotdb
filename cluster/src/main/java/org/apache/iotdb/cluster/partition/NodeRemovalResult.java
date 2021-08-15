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

package org.apache.iotdb.cluster.partition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** NodeRemovalResult stores the removed partition group. */
public class NodeRemovalResult {

  private List<PartitionGroup> removedGroupList = new ArrayList<>();

  public PartitionGroup getRemovedGroup(int raftId) {
    for (PartitionGroup group : removedGroupList) {
      if (group.getId() == raftId) {
        return group;
      }
    }
    return null;
  }

  public void addRemovedGroup(PartitionGroup group) {
    this.removedGroupList.add(group);
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeInt(removedGroupList.size());
    for (PartitionGroup group : removedGroupList) {
      group.serialize(dataOutputStream);
    }
  }

  public void deserialize(ByteBuffer buffer) {
    int removedGroupListSize = buffer.getInt();
    for (int i = 0; i < removedGroupListSize; i++) {
      PartitionGroup group = new PartitionGroup();
      group.deserialize(buffer);
      removedGroupList.add(group);
    }
  }
}
