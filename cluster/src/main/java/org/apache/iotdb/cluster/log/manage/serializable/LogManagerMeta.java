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
package org.apache.iotdb.cluster.log.manage.serializable;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class LogManagerMeta {

  private long commitLogTerm = -1;
  private long commitLogIndex = -1;
  private long lastLogIndex = -1;
  private long lastLogTerm = -1;
  private long maxHaveAppliedCommitIndex = -1;

  public static LogManagerMeta deserialize(ByteBuffer buffer) {
    LogManagerMeta res = new LogManagerMeta();
    res.commitLogTerm = ReadWriteIOUtils.readLong(buffer);
    res.commitLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogTerm = ReadWriteIOUtils.readLong(buffer);
    res.maxHaveAppliedCommitIndex = ReadWriteIOUtils.readLong(buffer);

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
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 5);
    byteBuffer.putLong(commitLogTerm);
    byteBuffer.putLong(commitLogIndex);
    byteBuffer.putLong(lastLogIndex);
    byteBuffer.putLong(lastLogTerm);
    byteBuffer.putLong(maxHaveAppliedCommitIndex);

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public String toString() {
    return "LogManagerMeta{"
        + " commitLogTerm="
        + commitLogTerm
        + ", commitLogIndex="
        + commitLogIndex
        + ", lastLogIndex="
        + lastLogIndex
        + ", lastLogTerm="
        + lastLogTerm
        + ", maxHaveAppliedCommitIndex="
        + maxHaveAppliedCommitIndex
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

  public void setCommitLogTerm(long commitLogTerm) {
    this.commitLogTerm = commitLogTerm;
  }

  public long getMaxHaveAppliedCommitIndex() {
    return maxHaveAppliedCommitIndex;
  }

  public void setMaxHaveAppliedCommitIndex(long maxHaveAppliedCommitIndex) {
    this.maxHaveAppliedCommitIndex = maxHaveAppliedCommitIndex;
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
        .append(commitLogTerm, that.commitLogTerm)
        .append(maxHaveAppliedCommitIndex, that.maxHaveAppliedCommitIndex)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(commitLogIndex)
        .append(lastLogIndex)
        .append(lastLogTerm)
        .append(commitLogTerm)
        .append(maxHaveAppliedCommitIndex)
        .toHashCode();
  }
}
