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

package org.apache.iotdb.cluster.log.applier;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.server.Timer;
import org.apache.iotdb.cluster.server.Timer.Statistic;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * this mainly copy from the {@link AsyncDataLogApplier}, so some codes are the same
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class AsyncMetaLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncMetaLogApplier.class);
  private static final int CONCURRENT_CONSUMER_NUM = 1;
  private LogApplier embeddedApplier;
  /**
   * only one consumer now, the key is {@link AsyncMetaLogApplier#metaPartialPath}
   */
  private Map<PartialPath, MetaLogConsumer> consumerMap;
  private PartialPath metaPartialPath;
  private ExecutorService consumerPool;
  private String name;

  // a plan that affects multiple sgs should wait until all consumers become empty to assure all
  // previous logs are applied, such a plan will wait on this condition if it finds any
  // consumers nonempty, and each time a consumer becomes empty, this will be notified so the
  // waiting log can start another round of check
  private final Object consumerEmptyCondition = new Object();

  public AsyncMetaLogApplier(LogApplier embeddedApplier, String name) {
    this.embeddedApplier = embeddedApplier;
    consumerMap = new HashMap<>();
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("meta-applier--%d").build();
    consumerPool = new ThreadPoolExecutor(CONCURRENT_CONSUMER_NUM,
        Runtime.getRuntime().availableProcessors(), 0, TimeUnit.SECONDS, new SynchronousQueue<>(),
        namedThreadFactory);
    this.name = name;
    try {
      metaPartialPath = new PartialPath("root.meta");
    } catch (IllegalPathException e) {
      logger.error("create add node partial path failed", e);
    }
  }

  @Override
  public void close() {
    consumerPool.shutdownNow();
  }

  @Override
  // synchronized: when a log is draining consumers, avoid other threads adding more logs so that
  // the consumers will never be drained
  public synchronized void apply(Log log) {
    PartialPath logKey;
    logKey = getLogKey(log);

    if (logKey != null) {
      // this plan only affects one sg, so we can run it with other plans in parallel
      long startTime = Statistic.RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS.getOperationStartTime();
      provideLogToConsumers(logKey, log);
      Statistic.RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS.calOperationCostTimeFromStart(startTime);
      return;
    }

    logger.debug("{}: {} is waiting for consumers to drain", name, log);
    long startTime = Statistic.RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS.getOperationStartTime();
    drainConsumers();
    applyInternal(log);
    Statistic.RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS.calOperationCostTimeFromStart(startTime);
  }

  /**
   * @param log Parallel apply based on the type of the log, now all log operations in
   *            metaLogApplier is synchronous
   * @return the metaPartialPath
   */
  private PartialPath getLogKey(Log log) {
    return metaPartialPath;
  }

  private void provideLogToConsumers(PartialPath planKey, Log log) {
    if (Timer.ENABLE_INSTRUMENTING) {
      log.setEnqueueTime(System.nanoTime());
    }
    consumerMap
        .computeIfAbsent(planKey, d -> new MetaLogConsumer(name + "-" + d))
        .accept(log);
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
    for (MetaLogConsumer consumer : consumerMap.values()) {
      if (!consumer.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Consumer not empty: {}", consumer);
        }
        return false;
      }
    }
    return true;
  }

  private void applyInternal(Log log) {
    long startTime = Statistic.RAFT_SENDER_DATA_LOG_APPLY.getOperationStartTime();
    embeddedApplier.apply(log);
    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.RAFT_SENDER_DATA_LOG_APPLY.calOperationCostTimeFromStart(startTime);
    }
  }

  private class MetaLogConsumer implements Runnable, Consumer<Log> {

    private BlockingQueue<Log> logQueue = new ArrayBlockingQueue<>(4096);
    private volatile long lastLogIndex;
    private volatile long lastAppliedLogIndex;
    private String name;
    private Future<?> future;

    public MetaLogConsumer(String name) {
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
      Thread.currentThread().setName(name);
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Log log = logQueue.take();
          Statistic.RAFT_SENDER_IN_APPLY_QUEUE.calOperationCostTimeFromStart(log.getEnqueueTime());
          try {
            applyInternal(log);
          } finally {
            lastAppliedLogIndex = log.getCurrLogIndex();
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
          logger.error("MetaLogConsumer exits", e);
          return;
        }
      }
      logger.info("MetaLogConsumer exits");
    }

    @Override
    public void accept(Log log) {
      if (future == null || future.isCancelled() || future.isDone()) {
        if (future != null) {
          try {
            future.get();
          } catch (InterruptedException e) {
            logger.error("Last applier thread exits unexpectedly", e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            logger.error("Last applier thread exits unexpectedly", e);
          }
        }
        future = consumerPool.submit(this);
      }

      try {
        lastLogIndex = log.getCurrLogIndex();
        logQueue.put(log);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.setException(e);
        log.setApplied(true);
        lastAppliedLogIndex = log.getCurrLogIndex();
      }
    }

    @Override
    public String toString() {
      return "MetaLogConsumer{"
          + " logQueue=" + logQueue
          + ", lastLogIndex=" + lastLogIndex
          + ", lastAppliedLogIndex=" + lastAppliedLogIndex
          + ", name='" + name + '\''
          + "}";
    }
  }
}
