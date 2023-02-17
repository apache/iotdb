/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedDeque;

public class FlushManager implements FlushManagerMBean, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlushManager.class);
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ConcurrentLinkedDeque<TsFileProcessor> tsFileProcessorQueue =
      new ConcurrentLinkedDeque<>();

  private FlushTaskPoolManager flushPool = FlushTaskPoolManager.getInstance();

  @Override
  public void start() throws StartupException {
    FlushSubTaskPoolManager.getInstance().start();
    flushPool.start();
    try {
      JMXService.registerMBean(this, ServiceType.FLUSH_SERVICE.getJmxName());
      MetricService.getInstance().addMetricSet(new FlushManagerMetrics(this));
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    FlushSubTaskPoolManager.getInstance().stop();
    FlushTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.FLUSH_SERVICE.getJmxName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FLUSH_SERVICE;
  }

  @Override
  public int getNumberOfWaitingTasks() {
    return flushPool.getWaitingTasksNumber();
  }

  @Override
  public int getNumberOfWorkingTasks() {
    return flushPool.getWorkingTasksNumber();
  }

  @Override
  public int getNumberOfPendingTasks() {
    return flushPool.getWaitingTasksNumber();
  }

  @Override
  public int getNumberOfWorkingSubTasks() {
    return FlushSubTaskPoolManager.getInstance().getWorkingTasksNumber();
  }

  @Override
  public int getNumberOfPendingSubTasks() {
    return FlushSubTaskPoolManager.getInstance().getWaitingTasksNumber();
  }

  /** a flush thread handles flush task */
  class FlushThread extends WrappedRunnable {

    @Override
    public void runMayThrow() {
      TsFileProcessor tsFileProcessor = tsFileProcessorQueue.poll();
      if (null == tsFileProcessor) {
        return;
      }

      tsFileProcessor.flushOneMemTable();
      tsFileProcessor.setManagedByFlushManager(false);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Flush Thread re-register TSProcessor {} to the queue.",
            tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
      }
      registerTsFileProcessor(tsFileProcessor);
    }
  }

  /**
   * Add tsFileProcessor to asyncTryToFlush manager
   *
   * @param tsFileProcessor tsFileProcessor to be flushed
   */
  @SuppressWarnings("squid:S2445")
  public void registerTsFileProcessor(TsFileProcessor tsFileProcessor) {
    synchronized (tsFileProcessor) {
      if (tsFileProcessor.isManagedByFlushManager()) {
        LOGGER.debug(
            "{} is already in the flushPool, the given processor flushMemtable number = {}",
            tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath(),
            tsFileProcessor.getFlushingMemTableSize());
      } else {
        if (tsFileProcessor.getFlushingMemTableSize() > 0) {
          tsFileProcessorQueue.add(tsFileProcessor);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "{} begin to submit a flush thread, flushing memtable size: {}, queue size: {}",
                tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath(),
                tsFileProcessor.getFlushingMemTableSize(),
                tsFileProcessorQueue.size());
          }
          tsFileProcessor.setManagedByFlushManager(true);
          flushPool.submit(new FlushThread());
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "No flushing memetable to do, register TsProcessor {} failed.",
                tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
          }
        }
      }
    }
  }

  private FlushManager() {}

  public static FlushManager getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static FlushManager instance = new FlushManager();
  }

  @Override
  public String toString() {
    return String.format(
        "TSProcessors in the queue: %d, TaskPool size %d + %d,",
        tsFileProcessorQueue.size(),
        flushPool.getWorkingTasksNumber(),
        flushPool.getWaitingTasksNumber());
  }
}
