package org.apache.iotdb.cluster.log.manage;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.manage.serializable.LogDequeSerializer;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DiskLogManager extends MemoryLogManager {
  private static final Logger logger = LoggerFactory.getLogger(DiskLogManager.class);

  // manage logs in disk
  private LogDequeSerializer logDequeSerializer;

  private LogManagerMeta managerMeta = new LogManagerMeta();


  protected DiskLogManager(LogApplier logApplier) {
    super(logApplier);
    logDequeSerializer = new SyncLogDequeSerializer();
    recovery();
  }

  private void recovery(){
    // recover meta
    LogManagerMeta logManagerMeta = logDequeSerializer.recoverMeta();
    if(logManagerMeta != null){
      setCommitLogIndex(logManagerMeta.getCommitLogIndex());
      setLastLogId(logManagerMeta.getLastLogId());
      setLastLogTerm(logManagerMeta.getLastLogTerm());
    }
    // recover logs
    setLogBuffer(logDequeSerializer.recoverLog());
  }


  @Override
  public long getLastLogIndex() {
    return lastLogId;
  }

  @Override
  public long getLastLogTerm() {
    return lastLogTerm;
  }

  @Override
  public void setLastLogTerm(long lastLogTerm) {
    this.lastLogTerm = lastLogTerm;
  }

  @Override
  public long getCommitLogIndex() {
    return commitLogIndex;
  }

  @Override
  public boolean appendLog(Log log) {
    boolean result = super.appendLog(log);
    if(result) {
      logDequeSerializer.addLast(log, getMeta());
    }

    return result;
  }


  public void truncateLog(int count) {
    if (logBuffer.size() > count) {
      // do super truncate log
      // super.truncateLog();
      logDequeSerializer.truncateLog(count, getMeta());
    }
  }

  @Override
  public synchronized void commitLog(long maxLogIndex) {
    super.commitLog(maxLogIndex);
    // save commit log index
    serializeMeta();
  }
  

  @Override
  public void setLastLogId(long lastLogId) {
    super.setLastLogId(lastLogId);
    // save meta
    serializeMeta();
  }

  /**
   * refresh meta info
   * @return meta info
   */
  public LogManagerMeta getMeta(){
    managerMeta.setCommitLogIndex(commitLogIndex);
    managerMeta.setLastLogId(lastLogId);
    managerMeta.setLastLogTerm(lastLogTerm);

    return managerMeta;
  }

  /**
   * serialize meta data of this log manager
   */
  private void serializeMeta(){
    logDequeSerializer.serializeMeta(getMeta());
  }


  @Override
  public void removeFromHead(int length){
    super.removeFromHead(length);
    logDequeSerializer.removeFirst(length);
  }

  /**
   * close file and release resource
   */
  public void close(){
    logDequeSerializer.close();
  }
}
