package org.apache.iotdb.cluster.log.manage.serializable;

import java.util.Deque;
import org.apache.iotdb.cluster.log.Log;

public interface LogDequeSerializer {

  public void addLast(Log log);

  public void removeLast();

  public void removeFirst(int removeNum);

  public Deque<Log> recoverLog();

  public LogManagerMeta recoverMeta();

  public void serializeMeta(LogManagerMeta meta);

  public void close();
}
