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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.RequestEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class AsyncLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncLogApplier.class);
  private LogApplier embeddedApplier;
  private DataLogConsumer[] consumers;
  private ExecutorService consumerPool;
  private String name;

  // a plan that affects multiple sgs should wait until all consumers become empty to assure all
  // previous logs are applied, such a plan will wait on this condition if it finds any
  // consumers nonempty, and each time a consumer becomes empty, this will be notified so the
  // waiting log can start another round of check
  private final Object consumerEmptyCondition = new Object();
  private volatile boolean waitingForDraining = false;

  public AsyncLogApplier(LogApplier embeddedApplier, String name, RaftConfig config) {
    this.embeddedApplier = embeddedApplier;
    consumers = new DataLogConsumer[config.getApplierThreadNum()];
    consumerPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            config.getApplierThreadNum(), "ApplierThread-" + name);
    for (int i = 0; i < consumers.length; i++) {
      consumers[i] = new DataLogConsumer(name + "-" + i, config.getMaxNumOfLogsInMem());
      consumerPool.submit(consumers[i]);
    }
    this.name = name;
  }

  @Override
  public void close() {
    consumerPool.shutdownNow();
  }

  @Override
  // synchronized: when a log is draining consumers, avoid other threads adding more logs so that
  // the consumers will never be drained
  public synchronized void apply(Entry e) {

    if (e instanceof RequestEntry) {
      RequestEntry requestEntry = (RequestEntry) e;
      IConsensusRequest request = requestEntry.getRequest();
      request = getStateMachine().deserializeRequest(request);
      requestEntry.setRequest(request);

      PartialPath logKey = getLogKey(request);
      if (logKey != null) {
        // this plan only affects one sg, so we can run it with other plans in parallel
        provideLogToConsumers(logKey, e);
        return;
      }
    }

    logger.debug("{}: {} is waiting for consumers to drain", name, e);
    drainConsumers();
    applyInternal(e);
  }

  private PartialPath getLogKey(IConsensusRequest e) {
    return e.conflictKey();
  }

  private void provideLogToConsumers(PartialPath planKey, Entry e) {
    consumers[Math.abs(planKey.hashCode()) % consumers.length].accept(e);
  }

  private void drainConsumers() {
    waitingForDraining = true;
    synchronized (consumerEmptyCondition) {
      while (!allConsumersEmpty()) {
        // wait until all consumers empty
        try {
          consumerEmptyCondition.wait(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          waitingForDraining = false;
          return;
        }
      }
    }
    waitingForDraining = false;
  }

  private boolean allConsumersEmpty() {
    for (DataLogConsumer consumer : consumers) {
      if (!consumer.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Consumer not empty: {}", consumer);
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "AsyncLogApplier{" + "consumers=" + Arrays.toString(consumers) + '}';
  }

  private void applyInternal(Entry e) {
    embeddedApplier.apply(e);
  }

  private class DataLogConsumer implements Runnable, Consumer<Entry> {

    private BlockingQueue<Entry> logQueue;
    private String name;

    public DataLogConsumer(String name, int queueCapacity) {
      this.name = name;
      this.logQueue = new ArrayBlockingQueue<>(queueCapacity);
    }

    public boolean isEmpty() {
      return logQueue.isEmpty();
    }

    @Override
    public void run() {
      if (logger.isDebugEnabled()) {
        Thread.currentThread().setName(name);
      }
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Entry e = logQueue.take();
          try {
            applyInternal(e);
          } finally {
            if (waitingForDraining && isEmpty()) {
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
      logger.debug("DataLogConsumer exits");
    }

    @Override
    public void accept(Entry e) {
      try {
        logQueue.put(e);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        e.setException(ex);
        e.setApplied(true);
      }
    }

    @Override
    public String toString() {
      return "DataLogConsumer{" + "logQueue=" + logQueue.size() + ", name='" + name + '\'' + '}';
    }
  }

  @Override
  public IStateMachine getStateMachine() {
    return embeddedApplier.getStateMachine();
  }
}
