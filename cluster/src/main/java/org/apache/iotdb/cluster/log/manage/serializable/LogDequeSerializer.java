package org.apache.iotdb.cluster.log.manage.serializable;

import java.util.Deque;
import org.apache.iotdb.cluster.log.Log;

public interface LogDequeSerializer {

  /**
   * append a log
   * @param log appended log
   */
  public void addLast(Log log, LogManagerMeta meta);

  /**
   * remove last log
   * @param meta metadata
   */
  public void removeLast(LogManagerMeta meta);

  /**
   * truncate num of logs
   * @param num num of logs
   * @param meta metadata
   */
  public void truncateLog(int num, LogManagerMeta meta);

  /**
   * remove 'num' of logs from first
   * @param num the number of removed logs
   */
  public void removeFirst(int num);

  /**
   * recover logs from disk
   * @return recovered logs
   */
  public Deque<Log> recoverLog();

  /**
   * recover meta from disk
   * @return recovered meta
   */
  public LogManagerMeta recoverMeta();

  /**
   * serialize meta of log manager
   * @param meta meta of log manager
   */
  public void serializeMeta(LogManagerMeta meta);

  /**
   * close file and release resource
   */
  public void close();
}
