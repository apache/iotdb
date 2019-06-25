package org.apache.iotdb.db.engine.memtable;

import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MemTablePoolTest {

  private ConcurrentLinkedQueue<IMemTable> memTables;

  @Before
  public void setUp() throws Exception {
    memTables = new ConcurrentLinkedQueue();
    new ReturnThread().start();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetAndRelease() {
    for (int i = 0; i < 100; i++) {
      IMemTable memTable = MemTablePool.getInstance().getEmptyMemTable("test case");
      memTables.add(memTable);
    }
  }

  @Test
  public void testSort() {
    long start = System.currentTimeMillis();
    TreeMap<Long, Long> treeMap = new TreeMap<>();
    for (int i = 0; i < 1000000; i++) {
      treeMap.put((long)i, (long)i);
    }
    start = System.currentTimeMillis() - start;
    System.out.println("time cost: " + start);
  }


  class ReturnThread extends Thread{

    @Override
    public void run() {
      while (true) {
        IMemTable memTable = memTables.poll();
        if (memTable == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        memTables.remove(memTable);
        MemTablePool.getInstance().putBack(memTable, "test case");
      }
    }
  }

}