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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class HardState {

  private long currentTerm;
  private Node voteFor;

  public HardState() {
    this.voteFor = null;
  }

  public static HardState deserialize(ByteBuffer buffer) {
    HardState res = new HardState();
    res.setCurrentTerm(ReadWriteIOUtils.readLong(buffer));
    // marker is previously read, remaining fields:
    // currentTerm(long), marker(byte)
    // (optional)ipLength(int), ipBytes(byte[]), port(int), identifier(int), dataPort(int)
    int isNull = buffer.get();
    if (isNull == 1) {
      Node node = new Node();
      SerializeUtils.deserialize(node, buffer);
      res.setVoteFor(node);
    } else {
      res.setVoteFor(null);
    }
    return res;
  }

  @SuppressWarnings("java:S125") // not commented code
  public ByteBuffer serialize() {
    int totalSize = Long.BYTES + Byte.BYTES;
    // currentTerm(long), marker(byte)
    // (optional) ipLength(int), ipBytes(byte[]), port(int), identifier(int), dataPort(int),
    // clientPort(int)
    if (voteFor != null) {
      byte[] ipBytes = voteFor.getIp().getBytes();
      totalSize +=
          Integer.BYTES
              + ipBytes.length
              + Integer.BYTES
              + Integer.BYTES
              + Integer.BYTES
              + Integer.BYTES;
      byte[] buffer = new byte[totalSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
      byteBuffer.putLong(currentTerm);
      byteBuffer.put((byte) 1);
      byteBuffer.putInt(ipBytes.length);
      byteBuffer.put(ipBytes);
      byteBuffer.putInt(voteFor.getMetaPort());
      byteBuffer.putInt(voteFor.getNodeIdentifier());
      byteBuffer.putInt(voteFor.getDataPort());
      byteBuffer.putInt(voteFor.getClientPort());
      byteBuffer.flip();
      return byteBuffer;
    }
    byte[] buffer = new byte[totalSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    byteBuffer.putLong(currentTerm);
    byteBuffer.put((byte) 0);
    byteBuffer.flip();

    return byteBuffer;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public void setCurrentTerm(long currentTerm) {
    this.currentTerm = currentTerm;
  }

  public Node getVoteFor() {
    return voteFor;
  }

  public void setVoteFor(Node voteFor) {
    this.voteFor = voteFor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HardState)) {
      return false;
    }
    HardState that = (HardState) o;
    return new EqualsBuilder()
        .append(currentTerm, that.currentTerm)
        .append(voteFor, that.voteFor)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(currentTerm).append(voteFor).toHashCode();
  }

  @Override
  public String toString() {
    return "HardState{" + "currentTerm=" + currentTerm + ", voteFor=" + voteFor + '}';
  }
}
