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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EnrichedTablets {

  private transient String topicName;
  private transient List<Tablet> tablets = new ArrayList<>();
  private transient List<Pair<String, Integer>> committerKeyAndCommitIds = new ArrayList<>();

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
    ReadWriteIOUtils.write(committerKeyAndCommitIds.size(), stream);
    for (Pair<String, Integer> committerKeyAndCommitId : committerKeyAndCommitIds) {
      ReadWriteIOUtils.write(committerKeyAndCommitId.left, stream);
      ReadWriteIOUtils.write(committerKeyAndCommitId.right, stream);
    }
  }

  public static EnrichedTablets deserialize(ByteBuffer buffer) {
    final EnrichedTablets enrichedTablets = new EnrichedTablets();
    enrichedTablets.topicName = ReadWriteIOUtils.readString(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      enrichedTablets.tablets.add(Tablet.deserialize(buffer));
    }
    size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      String committerKey = ReadWriteIOUtils.readString(buffer);
      int commitId = ReadWriteIOUtils.readInt(buffer);
      enrichedTablets.committerKeyAndCommitIds.add(new Pair<>(committerKey, commitId));
    }
    return enrichedTablets;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    EnrichedTablets that = (EnrichedTablets) obj;
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.tablets, that.tablets)
        && Objects.equals(this.committerKeyAndCommitIds, that.committerKeyAndCommitIds);
  }

  @Override
  public int hashCode() {
    // TODO: Tablet hashCode
    return Objects.hash(topicName, tablets, committerKeyAndCommitIds);
  }
}
