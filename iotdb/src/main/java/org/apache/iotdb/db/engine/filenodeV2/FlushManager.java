package org.apache.iotdb.db.engine.filenodeV2;

import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ThreadFactory;
import sun.nio.ch.ThreadPool;

public class FlushManager {

  private Queue<BWP> queue;

  private ThreadPool flushPool;

  private static final Object object;

  public FlushManager(int n) {
    this.flushPool = createFlushThreads(n, flushThread);
  }

  private boolean addBWP(BWP){
    synchronized (BWP) {
      // 对同一个BWP至多一个线程执行此操作
      if (!BWP.isManagedByFlushManager() && BWP.taskSize() > 0) {
        synchronized (queue) {
          queue.add(BWP);
        }
        BWP.setManagedByFlushManager(true);
        flushPool.submit(flushThread);
//        object.notify();
        return true;
      }
      return false;
    }
  }

  Runnable flushThread = new Runnable(){
    @Override
    public void run() {
//      object.wait();
      synchronized (queue) {
        BWP = queue.poll();
      }
      flushOneMemTable(BWP);
      // 对同一个BWP至多一个线程执行此操作
      BWP.setManagedByFlushManager(false);
      addBWP(BWP);
    }
  };


}
