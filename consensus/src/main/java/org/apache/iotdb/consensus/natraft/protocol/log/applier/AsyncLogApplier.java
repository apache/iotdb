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

package org.apache.iotdb.consensus.natraft.protocol.log.applier;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AsyncLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncLogApplier.class);
  private static final int CONCURRENT_CONSUMER_NUM = Runtime.getRuntime().availableProcessors();
  private RaftConfig config;
  private LogApplier embeddedApplier;
  private Map<PartialPath, DataLogConsumer> consumerMap;
  private ExecutorService consumerPool;
  private String name;

  // a plan that affects multiple sgs should wait until all consumers become empty to assure all
  // previous logs are applied, such a plan will wait on this condition if it finds any
  // consumers nonempty, and each time a consumer becomes empty, this will be notified so the
  // waiting log can start another round of check
  private final Object consumerEmptyCondition = new Object();

  public AsyncLogApplier(LogApplier embeddedApplier, String name, RaftConfig config) {
    this.embeddedApplier = embeddedApplier;
    consumerMap = new HashMap<>();
    consumerPool =
        new ThreadPoolExecutor(
            CONCURRENT_CONSUMER_NUM,
            Integer.MAX_VALUE,
            0,
            TimeUnit.SECONDS,
            new SynchronousQueue<>());
    this.name = name;
    this.config = config;
  }

  @Override
  public void close() {
    consumerPool.shutdownNow();
  }

  @Override
  // synchronized: when a log is draining consumers, avoid other threads adding more logs so that
  // the consumers will never be drained
  public synchronized void apply(Entry e) {

    PartialPath logKey = getLogKey(e);

    if (logKey != null) {
      // this plan only affects one sg, so we can run it with other plans in parallel
      provideLogToConsumers(logKey, e);
      return;
    }

    logger.debug("{}: {} is waiting for consumers to drain", name, e);
    drainConsumers();
    applyInternal(e);
  }

  private PartialPath getLogKey(Entry e) {
    // TODO-raft: implement
    return null;
  }

  private void provideLogToConsumers(PartialPath planKey, Entry e) {
    consumerMap.computeIfAbsent(planKey, d -> new DataLogConsumer(name + "-" + d)).accept(e);
  }

  private void drainConsumers() {
    synchronized (consumerEmptyCondition) {
      while (!allConsumersEmpty()) {
        // wait until all consumers empty
        try {
          consumerEmptyCondition.wait(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private boolean allConsumersEmpty() {
    for (DataLogConsumer consumer : consumerMap.values()) {
      if (!consumer.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Consumer not empty: {}", consumer);
        }
        return false;
      }
    }
    return true;
  }

  private void applyInternal(Entry e) {
    embeddedApplier.apply(e);
  }

  private class DataLogConsumer implements Runnable, Consumer<Entry> {

    private BlockingQueue<Entry> logQueue = new ArrayBlockingQueue<>(config.getMaxNumOfLogsInMem());
    private volatile long lastLogIndex;
    private volatile long lastAppliedLogIndex;
    private String name;
    private Future<?> future;

    public DataLogConsumer(String name) {
      this.name = name;
    }

    public boolean isEmpty() {
      return lastLogIndex == lastAppliedLogIndex;
    }

    @Override
    public void run() {
      // appliers have a higher priority than normal threads (like client threads and low
      // priority background threads), to assure fast ingestion, but a lower priority than
      // heartbeat threads
      Thread.currentThread().setPriority(8);
      if (logger.isDebugEnabled()) {
        Thread.currentThread().setName(name);
      }
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Entry e = logQueue.take();
          try {
            applyInternal(e);
          } finally {
            lastAppliedLogIndex = e.getCurrLogIndex();
            if (isEmpty()) {
              synchronized (consumerEmptyCondition) {
                consumerEmptyCondition.notifyAll();
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          logger.error("DataLogConsumer exits", e);
          return;
        }
      }
      logger.info("DataLogConsumer exits");
    }

    @Override
    public void accept(Entry e) {
      if (future == null || future.isCancelled() || future.isDone()) {
        if (future != null) {
          try {
            future.get();
          } catch (InterruptedException ex) {
            logger.error("Last applier thread exits unexpectedly", ex);
            Thread.currentThread().interrupt();
          } catch (ExecutionException ex) {
            logger.error("Last applier thread exits unexpectedly", ex);
          }
        }
        future = consumerPool.submit(this);
      }

      try {
        lastLogIndex = e.getCurrLogIndex();
        logQueue.put(e);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        e.setException(ex);
        e.setApplied(true);
        lastAppliedLogIndex = e.getCurrLogIndex();
      }
    }

    @Override
    public String toString() {
      return "DataLogConsumer{"
          + "logQueue="
          + logQueue.size()
          + ", lastLogIndex="
          + lastLogIndex
          + ", lastAppliedLogIndex="
          + lastAppliedLogIndex
          + ", name='"
          + name
          + '\''
          + '}';
    }
  }
}
