package org.apache.iotdb.cluster.log.manage.serializable;

import java.nio.ByteBuffer;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class LogManagerMeta {

  private long commitLogIndex = -1;
  private long lastLogId = -1;
  private long lastLogTerm = -1;

  public static LogManagerMeta deserialize(ByteBuffer buffer) {
    LogManagerMeta res = new LogManagerMeta();
    res.commitLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogId = ReadWriteIOUtils.readLong(buffer);
    res.lastLogTerm = ReadWriteIOUtils.readLong(buffer);

    return res;
  }

  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  public void setCommitLogIndex(long commitLogIndex) {
    this.commitLogIndex = commitLogIndex;
  }

  public ByteBuffer serialize() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 3);
    byteBuffer.putLong(commitLogIndex);
    byteBuffer.putLong(lastLogId);
    byteBuffer.putLong(lastLogTerm);

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public String toString() {
    return "LogManagerMeta{" +
        "commitLogIndex=" + commitLogIndex +
        ", lastLogId=" + lastLogId +
        ", lastLogTerm=" + lastLogTerm +
        '}';
  }

  public long getLastLogId() {
    return lastLogId;
  }

  public void setLastLogId(long lastLogId) {
    this.lastLogId = lastLogId;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }

  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
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
        .append(lastLogId, that.lastLogId)
        .append(lastLogTerm, that.lastLogTerm)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(commitLogIndex)
        .append(lastLogId)
        .append(lastLogTerm)
        .toHashCode();
  }
}
