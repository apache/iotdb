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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncDataLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncDataLogApplier.class);
  private static final int CONCURRENT_CONSUMER_NUM = 64;
  private LogApplier embeddedApplier;
  private DataLogConsumer[] consumers;

  public AsyncDataLogApplier(LogApplier embeddedApplier) {
    this.embeddedApplier = embeddedApplier;
    this.consumers = new DataLogConsumer[CONCURRENT_CONSUMER_NUM];
    ExecutorService consumerPool = Executors.newFixedThreadPool(CONCURRENT_CONSUMER_NUM);
    for (int i = 0; i < consumers.length; i++) {
      consumers[i] = new DataLogConsumer();
      consumerPool.submit(consumers[i]);
    }
  }

  @Override
  public void apply(Log log) {
    // we can only apply insertions in parallel, for other logs, we must wait until all previous
    // logs are applied, or the order of deletions and insertions may get wrong
    if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      PhysicalPlan plan = physicalPlanLog.getPlan();
      if (plan instanceof InsertPlan) {
        // decide the consumer by deviceId
        InsertPlan insertPlan = (InsertPlan) plan;
        String deviceId = insertPlan.getDeviceId();
        consumers[Math.abs(deviceId.hashCode() % consumers.length)].accept(log);
        return;
      }
    }
    drainConsumers();
    applyInternal(log);
  }

  private void drainConsumers() {
    while (!allConsumersEmpty()) {
      // wait until all consumers empty
    }
  }

  private boolean allConsumersEmpty() {
    for (DataLogConsumer consumer : consumers) {
      if (!consumer.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private void applyInternal(Log log) {
    try {
      embeddedApplier.apply(log);
    } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
      log.setException(e);
    } finally {
      log.setApplied(true);
    }
  }

  private class DataLogConsumer implements Runnable, Consumer<Log> {
    private BlockingQueue<Log> logQueue = new LinkedBlockingQueue<>();

    public boolean isEmpty() {
      return logQueue.isEmpty();
    }


    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Log log = logQueue.take();
          applyInternal(log);
        } catch (InterruptedException e) {
          logger.info("DataLogConsumer exits");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    @Override
    public void accept(Log log) {
      if(!logQueue.offer(log)) {
        logger.error("Cannot insert log into queue");
      }
    }
  }
}
