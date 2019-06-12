package org.apache.iotdb.db.engine.memtable;

import java.util.Stack;
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

  public IMemTable getEmptyMemTable() {
    synchronized (emptyMemTables) {
      if (emptyMemTables.isEmpty() && size < capacity) {
        size++;
        LOGGER.info("generated a new memtable, system memtable size: {}, stack size: {}", size, emptyMemTables.size());
        return new PrimitiveMemTable();
      } else if (!emptyMemTables.isEmpty()){
        LOGGER.info("system memtable size: {}, stack size: {}, then get a memtable from stack", size, emptyMemTables.size());
        return emptyMemTables.pop();
      }
    }
    // wait until some one has released a memtable
    while (true) {
      if(!emptyMemTables.isEmpty()) {
        synchronized (emptyMemTables) {
          if (!emptyMemTables.isEmpty()){
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
