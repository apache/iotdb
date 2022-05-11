package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 设计思路：根据trigger（未来根据subscription）， 分别实例化对应的ForwardQueue。
 *
 * @param <T>
 */
public class ForwardQueue<T extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardQueue.class);

  private static final int MAX_QUEUE_COUNT = 8;
  private static final int MAX_QUEUE_SIZE = 2000;
  private static final int BATCH_SIZE = 100;

  private final ArrayBlockingQueue<T>[] queues = new ArrayBlockingQueue[MAX_QUEUE_COUNT];

  private final Handler handler;

  public ForwardQueue(Handler handler) {
    for (int i = 0; i < MAX_QUEUE_COUNT; i++) {
      queues[i] = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
      new ForwardQueueThread(ForwardQueue.class.getSimpleName() + "-" + i, queues[i]).start();
    }
    this.handler = handler;
  }

  int getQueueID(int hashCode) {
    return (hashCode & 0x7FFFFFFF) % queues.length;
  }

  // 按照设备取模入队列
  public boolean offer(T event) {
    if (event.getFullPath() != null) {
      return queues[getQueueID(event.getFullPath().hashCode())].offer(event);
    } else {
      return queues[getQueueID(event.hashCode())].offer(event);
    }
  }

  public void put(T event, int hashCode) throws InterruptedException {
    queues[getQueueID(hashCode)].put(event);
  }

  private void handle(ArrayList<T> events) {
    try {
      handler.onEvent(events);
    } catch (Exception e) {
      //
    }
  }

  class ForwardQueueThread extends Thread {

    ArrayBlockingQueue<T> queue;

    public ForwardQueueThread(String name, ArrayBlockingQueue<T> queue) {
      super(name);
      this.queue = queue;
    }

    public void run() {
      final long maxWaitMillis = 500;
      final ArrayList<T> list = new ArrayList<>();
      long startMillis = System.currentTimeMillis();
      long restMillis = maxWaitMillis;
      while (true) {
        try {
          T obj;
          if (list.isEmpty()) {
            obj = queue.take();
          } else {
            obj = queue.poll(restMillis, TimeUnit.MILLISECONDS);
          }
          if (obj != null) {
            list.add(obj);
            queue.drainTo(list, BATCH_SIZE - list.size());
            if (list.size() < BATCH_SIZE) {
              long waitMillis = System.currentTimeMillis() - startMillis;
              if (waitMillis < maxWaitMillis) {
                restMillis = maxWaitMillis - waitMillis;
                continue;
              }
            }
          }
          handle(list);
          list.clear();
          startMillis = System.currentTimeMillis();
        } catch (InterruptedException e) {
          break;
        } catch (Throwable t) {
          LOGGER.error("ForwardTaskQueue consumer error", t);
        }
      }
    }
  }
}
