package org.apache.iotdb.cluster.log.manage.serializable;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class LogManagerMeta {

  private long commitLogIndex = -1;
  private long lastLogId = -1;
  private long lastLogTerm = -1;

  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  public ByteBuffer serialize(){
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 3);
    byteBuffer.putLong(commitLogIndex);
    byteBuffer.putLong(lastLogId);
    byteBuffer.putLong(lastLogId);

    return byteBuffer;
  }

  public static LogManagerMeta deserialize(ByteBuffer buffer){
    LogManagerMeta res = new LogManagerMeta();
    res.commitLogIndex = ReadWriteIOUtils.readLong(buffer);
    res.lastLogId = ReadWriteIOUtils.readLong(buffer);
    res.lastLogTerm = ReadWriteIOUtils.readLong(buffer);

    return res;
  }

  public void setCommitLogIndex(long commitLogIndex) {
    this.commitLogIndex = commitLogIndex;
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
}
