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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.server.Timer;
import org.apache.iotdb.cluster.server.Timer.Statistic;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDataLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncDataLogApplier.class);
  private static final int CONCURRENT_CONSUMER_NUM = Runtime.getRuntime().availableProcessors();
  private LogApplier embeddedApplier;
  private Map<PartialPath, DataLogConsumer> consumerMap;
  private ExecutorService consumerPool;
  private String name;

  // a plan that affects multiple sgs should wait until all consumers become empty to assure all
  // previous logs are applied, such a plan will wait on this condition if it finds any
  // consumers nonempty, and each time a consumer becomes empty, this will be notified so the
  // waiting log can start another round of check
  private final Object consumerEmptyCondition = new Object();

  public AsyncDataLogApplier(LogApplier embeddedApplier, String name) {
    this.embeddedApplier = embeddedApplier;
    consumerMap = new ConcurrentHashMap<>();
    consumerPool = new ThreadPoolExecutor(CONCURRENT_CONSUMER_NUM,
        Integer.MAX_VALUE, 0, TimeUnit.SECONDS, new SynchronousQueue<>());
    this.name = name;
  }

  @Override
  // synchronized: when a log is draining consumers, avoid other threads adding more logs so that
  // the consumers will never be drained
  public synchronized void apply(Log log)  {
    // we can only apply some kinds of plans in parallel, for other logs, we must wait until all
    // previous logs are applied, or the order of deletions and insertions may get wrong
    if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      PhysicalPlan plan = physicalPlanLog.getPlan();
      try {
        PartialPath planKey = getPlanKey(plan);
        if (planKey != null) {
          // this plan only affects one sg, so we can run it with other plans in parallel
          provideLogToConsumers(planKey, log);
          return;
        }
      } catch (StorageGroupNotSetException e) {
        logger.debug("Exception occurred when applying {}", log, e);
        log.setException(e);
        log.setApplied(true);
        return;
      }
    }
    drainConsumers();
    applyInternal(log);
  }

  private PartialPath getPlanKey(PhysicalPlan plan) throws StorageGroupNotSetException {
    return getPlanDevice(plan);
  }

  private PartialPath getPlanDevice(PhysicalPlan plan) {
    PartialPath sgPath = null;
    if (plan instanceof InsertPlan) {
      sgPath = ((InsertPlan) plan).getDeviceId();
    } else if (plan instanceof CreateTimeSeriesPlan) {
      PartialPath path = ((CreateTimeSeriesPlan) plan).getPath();
      sgPath = path.getDevicePath();
    }
    return sgPath;
  }

  private PartialPath getPlanSG(PhysicalPlan plan) throws StorageGroupNotSetException {
    PartialPath sgPath = null;
    if (plan instanceof InsertPlan) {
      PartialPath deviceId = ((InsertPlan) plan).getDeviceId();
      sgPath = IoTDB.metaManager.getStorageGroupPath(deviceId);
    } else if (plan instanceof CreateTimeSeriesPlan) {
      PartialPath path = ((CreateTimeSeriesPlan) plan).getPath();
      sgPath = IoTDB.metaManager.getStorageGroupPath(path);
    }
    return sgPath;
  }

  private void provideLogToConsumers(PartialPath planKey, Log log) {
    if (Timer.ENABLE_INSTRUMENTING) {
      log.setEnqueueTime(System.nanoTime());
    }
    consumerMap.computeIfAbsent(planKey, d -> {
      DataLogConsumer dataLogConsumer = new DataLogConsumer(name + "-" + d);
      consumerPool.submit(dataLogConsumer);
      return dataLogConsumer;
    }).accept(log);
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

  private void applyInternal(Log log) {
    long applyStart;
    if (Timer.ENABLE_INSTRUMENTING) {
      applyStart = System.nanoTime();
    }
    embeddedApplier.apply(log);
    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.RAFT_SENDER_DATA_LOG_APPLY.addNanoFromStart(applyStart);
    }
  }

  private class DataLogConsumer implements Runnable, Consumer<Log> {

    private BlockingQueue<Log> logQueue = new LinkedBlockingQueue<>();
    private volatile long lastLogIndex;
    private volatile long lastAppliedLogIndex;
    private String name;

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
      Thread.currentThread().setName(name);
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Log log = logQueue.take();
          Statistic.RAFT_SENDER_IN_APPLY_QUEUE.addNanoFromStart(log.getEnqueueTime());
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
          logger.info("DataLogConsumer exits");
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          logger.error("DataLogConsumer exits", e);
          return;
        }
      }
    }

    @Override
    public void accept(Log log) {
      if (!logQueue.offer(log)) {
        logger.error("Cannot insert log into queue");
      } else {
        lastLogIndex = log.getCurrLogIndex();
      }
    }

    @Override
    public String toString() {
      return "DataLogConsumer{" +
          "logQueue=" + logQueue.size() +
          ", lastLogIndex=" + lastLogIndex +
          ", lastAppliedLogIndex=" + lastAppliedLogIndex +
          ", name='" + name + '\'' +
          '}';
    }
  }
}
