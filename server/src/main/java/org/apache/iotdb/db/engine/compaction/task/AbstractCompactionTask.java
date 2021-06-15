package org.apache.iotdb.db.engine.compaction.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractCompactionTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCompactionTask.class);
  private AtomicInteger globalActiveCompactionTaskNum;

  public void setCompactionTaskNum(AtomicInteger compactionTaskNum) {
    globalActiveCompactionTaskNum = compactionTaskNum;
  }

  @Override
  public void run() {
    try {
      doCompaction();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
    } finally {
      globalActiveCompactionTaskNum.decrementAndGet();
    }
  }

  protected abstract void doCompaction() throws Exception;
}
