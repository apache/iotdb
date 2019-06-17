package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTablePool {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemTablePool.class);

  private Deque<IMemTable> emptyMemTables;
  // >= number of storage group * 2
  private int capacity = 20;
  private int size = 0;
  private static final int WAIT_TIME = 2000;

  private static final MemTablePool INSTANCE = new MemTablePool();

  private MemTablePool() {
    emptyMemTables = new ArrayDeque<>();
  }

  public IMemTable getEmptyMemTable(Object applier) {
    synchronized (emptyMemTables) {
      if (emptyMemTables.isEmpty() && size < capacity) {
        size++;
        LOGGER.info("generated a new memtable for {}, system memtable size: {}, stack size: {}",
            applier, size, emptyMemTables.size());
        return new PrimitiveMemTable();
      } else if (!emptyMemTables.isEmpty()) {
        LOGGER
            .info("system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
                size, emptyMemTables.size(), applier);
        return emptyMemTables.pop();
      }

      // wait until some one has released a memtable
      int waitCount = 1;
      while (true) {
        if (!emptyMemTables.isEmpty()) {
          LOGGER.info(
              "system memtable size: {}, stack size: {}, then get a memtable from stack for {}",
              size, emptyMemTables.size(), applier);
          return emptyMemTables.pop();
        }
        try {
          emptyMemTables.wait(WAIT_TIME);
        } catch (InterruptedException e) {
          LOGGER.error("{} fails to wait fot memtables {}, continue to wait", applier, e);
        }
        LOGGER.info("{} has waited for a memtable for {}ms", applier, waitCount * WAIT_TIME);
      }
    }
  }

  public void release(IMemTable memTable) {
    synchronized (emptyMemTables) {
      memTable.clear();
      emptyMemTables.push(memTable);
      emptyMemTables.notify();
      LOGGER.info("a memtable returned, stack size {}", emptyMemTables.size());
    }
  }

  public static MemTablePool getInstance() {
    return INSTANCE;
  }

}
