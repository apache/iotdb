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
package org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class LogManagerMeta {

  private long commitLogIndex = -1;
  private long lastLogIndex = -1;
  private long lastLogTerm = -1;
  private long lastAppliedIndex = -1;
  private long lastAppliedTerm = -1;

  public static LogManagerMeta deserialize(ByteBuffer buffer) {
    LogManagerMeta res = new LogManagerMeta();
    res.commitLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogTerm = ReadWriteIOUtils.readLong(buffer);
    res.lastAppliedIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastAppliedTerm = ReadWriteIOUtils.readLong(buffer);

    return res;
  }

  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  void setCommitLogIndex(long commitLogIndex) {
    this.commitLogIndex = commitLogIndex;
  }

  public ByteBuffer serialize() {
    // 5 is the number of attributes in class LogManagerMeta
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 6);
    byteBuffer.putLong(commitLogIndex);
    byteBuffer.putLong(lastLogIndex);
    byteBuffer.putLong(lastLogTerm);
    byteBuffer.putLong(lastAppliedIndex);
    byteBuffer.putLong(lastAppliedTerm);

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public String toString() {
    return "LogManagerMeta{"
        + ", commitLogIndex="
        + commitLogIndex
        + ", lastLogIndex="
        + lastLogIndex
        + ", lastLogTerm="
        + lastLogTerm
        + ", maxHaveAppliedCommitIndex="
        + lastAppliedIndex
        + "}";
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public void setLastLogIndex(long lastLogIndex) {
    this.lastLogIndex = lastLogIndex;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }

  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  public long getLastAppliedIndex() {
    return lastAppliedIndex;
  }

  public void setLastAppliedIndex(long lastAppliedIndex) {
    this.lastAppliedIndex = lastAppliedIndex;
  }

  public long getLastAppliedTerm() {
    return lastAppliedTerm;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof LogManagerMeta)) {
      return false;
    }

    LogManagerMeta that = (LogManagerMeta) o;

    return new EqualsBuilder()
        .append(commitLogIndex, that.commitLogIndex)
        .append(lastLogIndex, that.lastLogIndex)
        .append(lastLogTerm, that.lastLogTerm)
        .append(lastAppliedIndex, that.lastAppliedIndex)
        .append(lastAppliedTerm, that.lastAppliedTerm)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(commitLogIndex)
        .append(lastLogIndex)
        .append(lastLogTerm)
        .append(lastAppliedIndex)
        .toHashCode();
  }
}
