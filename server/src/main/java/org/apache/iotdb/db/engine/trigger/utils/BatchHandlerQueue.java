/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.trigger.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;
import org.apache.iotdb.db.engine.trigger.sink.api.Handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Each Trigger instantiate a ForwardQueue
 *
 * @param <T> Subclass of Event
 */
public class BatchHandlerQueue<T extends Event> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandlerQueue.class);

  private final int queueNumber;
  private final int queueSize;
  private final int batchSize;
  private final AtomicInteger atomicCount = new AtomicInteger();

  private final ArrayBlockingQueue<T>[] queues;

  private final Handler handler;

  public BatchHandlerQueue(int queueNumber, int queueSize, int batchSize, Handler handler) {
    this.queueNumber =
        Math.min(
            queueNumber,
            IoTDBDescriptor.getInstance().getConfig().getTriggerForwardMaxQueueNumber());
    this.queueSize =
        Math.min(
            queueSize,
            IoTDBDescriptor.getInstance().getConfig().getTriggerForwardMaxSizePerQueue());
    this.batchSize =
        Math.min(batchSize, IoTDBDescriptor.getInstance().getConfig().getTriggerForwardBatchSize());
    this.handler = handler;
    queues = new ArrayBlockingQueue[this.queueNumber];
    for (int i = 0; i < queueNumber; i++) {
      queues[i] = new ArrayBlockingQueue<>(this.queueSize);
      Thread t =
          new ForwardQueueConsumer(
              handler.getClass().getSimpleName()
                  + "-"
                  + BatchHandlerQueue.class.getSimpleName()
                  + "-"
                  + i,
              queues[i]);
      t.setDaemon(true);
      t.start();
    }
  }

  private int getQueueID(int hashCode) {
    return (hashCode & 0x7FFFFFFF) % queues.length;
  }

  public boolean offer(T event) {
    // Group by device or polling
    if (event.getFullPath() != null) {
      return queues[getQueueID(event.getFullPath().getDevice().hashCode())].offer(event);
    } else {
      return queues[getQueueID(atomicCount.incrementAndGet())].offer(event);
    }
  }

  public void put(T event) throws InterruptedException {
    // Group by device or polling
    if (event.getFullPath() != null) {
      queues[getQueueID(event.getFullPath().getDevice().hashCode())].put(event);
    } else {
      queues[getQueueID(atomicCount.incrementAndGet())].put(event);
    }
  }

  private void handle(ArrayList<T> events) throws Exception {
    handler.onEvent(events);
  }

  class ForwardQueueConsumer extends Thread {

    ArrayBlockingQueue<T> queue;

    public ForwardQueueConsumer(String name, ArrayBlockingQueue<T> queue) {
      super(name);
      this.queue = queue;
    }

    @Override
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
            queue.drainTo(list, batchSize - list.size());
            if (list.size() < batchSize) {
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
