package org.apache.iotdb.cluster.log.manage;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.manage.serializable.LogDequeSerializer;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DiskLogManager extends MemoryLogManager {
  private static final Logger logger = LoggerFactory.getLogger(DiskLogManager.class);

  // manage logs in disk
  private LogDequeSerializer logDequeSerializer;


  protected DiskLogManager(LogApplier logApplier) {
    super(logApplier);
    logDequeSerializer = new SyncLogDequeSerializer();
    recovery();
  }

  private void recovery(){
    // recover meta
    LogManagerMeta logManagerMeta = logDequeSerializer.recoverMeta();
    setCommitLogIndex(logManagerMeta.getCommitLogIndex());
    setLastLogId(logManagerMeta.getLastLogId());
    setLastLogTerm(logManagerMeta.getLastLogTerm());
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
  public void appendLog(Log log) {
    super.appendLog(log);
    logDequeSerializer.addLast(log);
    // save last log id and term
    serializeMeta();
  }

  @Override
  public void removeLastLog() {
    if (!logBuffer.isEmpty()) {
      super.removeLastLog();
      logDequeSerializer.removeLast();
      // save last log id and term
      serializeMeta();
    }
  }

  @Override
  public void replaceLastLog(Log log) {
    super.replaceLastLog(log);
    logDequeSerializer.removeLast();
    logDequeSerializer.addLast(log);
    // save last log id and term
    serializeMeta();
  }

  @Override
  public synchronized void commitLog(long maxLogIndex) {
    super.commitLog(maxLogIndex);
    // save commit log index
    serializeMeta();
  }

  @Override
  public void commitLog(Log log) throws QueryProcessException {
    super.commitLog(log);
    // save commit log index
    serializeMeta();
  }

  @Override
  public void setLastLogId(long lastLogId) {
    super.setLastLogId(lastLogId);
    // save meta
    serializeMeta();
  }

  Log removeFirstLog() {
    Log res = super.removeFirstLog();
    return res;
  }

  private void serializeMeta(){
    logDequeSerializer.serializeMeta(null);
  }
}
