package org.apache.iotdb.cluster.log.manage;

import java.util.ArrayList;
import java.util.List;
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
  public void appendLog(Log log) {
    super.appendLog(log);
    logDequeSerializer.addLast(log, getMeta());
  }

  @Override
  public void removeLastLog() {
    if (!logBuffer.isEmpty()) {
      super.removeLastLog();
      logDequeSerializer.removeLast(getMeta());
    }
  }

  public void truncateLog(int count) {
    if (logBuffer.size() > count) {
      // do super truncate log
      // super.truncateLog();
      logDequeSerializer.truncateLog(count, getMeta());
    }
  }

  @Override
  public void replaceLastLog(Log log) {
    super.replaceLastLog(log);
    LogManagerMeta curMeta = getMeta();
    logDequeSerializer.removeLast(curMeta);
    logDequeSerializer.addLast(log, curMeta);
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

  /**
   * remove logs which haven been committed
   *
   * @return last committed log
   */
  @Override
  public Log removeCommittedLogsReturnLastLog() {
    Log res = null;
    int removeCount = 0;
    while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
      removeCount++;
      // remove committed logs
      res = removeFirstLog();
    }

    logDequeSerializer.removeFirst(removeCount);
    return res;
  }

  /**
   * remove logs which haven been committed
   *
   * @return committed logs List
   */
  @Override
  public List<Log> removeAndReturnCommittedLogs() {
    List<Log> res = new ArrayList<>();
    int removeCount = 0;
    while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
      removeCount++;
      res.add(removeFirstLog());
    }

    logDequeSerializer.removeFirst(removeCount);
    return res;
  }

  /**
   * close file and release resource
   */
  public void close(){
    logDequeSerializer.close();
  }
}
