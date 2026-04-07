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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class WriterId {

  private final String regionId;
  private final int nodeId;
  private final long writerEpoch;

  public WriterId(final String regionId, final int nodeId, final long writerEpoch) {
    this.regionId = regionId;
    this.nodeId = nodeId;
    this.writerEpoch = writerEpoch;
  }

  public String getRegionId() {
    return regionId;
  }

  public int getNodeId() {
    return nodeId;
  }

  public long getWriterEpoch() {
    return writerEpoch;
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(regionId, stream);
    ReadWriteIOUtils.write(nodeId, stream);
    ReadWriteIOUtils.write(writerEpoch, stream);
  }

  public static WriterId deserialize(final ByteBuffer buffer) {
    return new WriterId(
        ReadWriteIOUtils.readString(buffer),
        ReadWriteIOUtils.readInt(buffer),
        ReadWriteIOUtils.readLong(buffer));
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof WriterId)) {
      return false;
    }
    final WriterId that = (WriterId) obj;
    return nodeId == that.nodeId
        && writerEpoch == that.writerEpoch
        && Objects.equals(regionId, that.regionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, nodeId, writerEpoch);
  }

  @Override
  public String toString() {
    return "WriterId{"
        + "regionId='"
        + regionId
        + '\''
        + ", nodeId="
        + nodeId
        + ", writerEpoch="
        + writerEpoch
        + '}';
  }
}
