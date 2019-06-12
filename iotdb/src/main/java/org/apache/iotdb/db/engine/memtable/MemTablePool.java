package org.apache.iotdb.db.engine.memtable;

import java.util.Stack;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTablePool {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemTablePool.class);

  private Stack<IMemTable> emptyMemTables;
  // >= number of storage group * 2
  private int capacity = 20;
  private int size = 0;

  private static final MemTablePool INSTANCE = new MemTablePool();

  public MemTablePool() {
    emptyMemTables = new Stack<>();
  }

  public IMemTable getEmptyMemTable(Object applier) {
    synchronized (emptyMemTables) {
      if (emptyMemTables.isEmpty() && size < capacity) {
        size++;
        LOGGER.info("generated a new memtable for {}, system memtable size: {}, stack size: {}",
            applier, size, emptyMemTables.size());
        return new PrimitiveMemTable();
      } else if (!emptyMemTables.isEmpty()){
        LOGGER.info("system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
            size, emptyMemTables.size(), applier);
        return emptyMemTables.pop();
      }
    }
    // wait until some one has released a memtable
    long waitStartTime = System.currentTimeMillis();
    long lastPrintIdx = 0;
    while (true) {
      if(!emptyMemTables.isEmpty()) {
        synchronized (emptyMemTables) {
          if (!emptyMemTables.isEmpty()){
            LOGGER.info("system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
                size, emptyMemTables.size(), applier);
            return emptyMemTables.pop();
          }
        }
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.error("Unexpected interruption", e);
      }
      long waitedTime = System.currentTimeMillis() - waitStartTime;
      if (waitedTime / 2000 > lastPrintIdx) {
        lastPrintIdx = waitedTime / 2000;
        LOGGER.info("{} has waited for a memtable for {}ms", applier, waitedTime);
      }
    }
  }


  public void release(IMemTable memTable) {
    synchronized (emptyMemTables) {
      memTable.clear();
      emptyMemTables.push(memTable);
    }
  }

  public static MemTablePool getInstance() {
    return INSTANCE;
  }

}
