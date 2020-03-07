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
   */
  public void removeLast(LogManagerMeta meta);

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
