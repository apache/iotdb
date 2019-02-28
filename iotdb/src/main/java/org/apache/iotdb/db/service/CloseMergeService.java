/**
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
package org.apache.iotdb.db.service;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that triggers close and merge operation regularly.
 *
 * @author liukun
 */
public class CloseMergeService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CloseMergeService.class);
  private static IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();
  private static final long MERGE_DELAY = dbConfig.getPeriodTimeForMerge();
  private static final long CLOSE_DELAY = dbConfig.getPeriodTimeForFlush();
  private static final long MERGE_PERIOD = dbConfig.getPeriodTimeForMerge();
  private static final long CLOSE_PERIOD = dbConfig.getPeriodTimeForFlush();
  private static CloseMergeService closeMergeService = new CloseMergeService();
  private Runnable mergeService = new MergeServiceThread();
  private Runnable closeService = new CloseServiceThread();
  private ScheduledExecutorService service;
  private CloseAndMergeDaemon closeAndMergeDaemon = new CloseAndMergeDaemon();
  private volatile boolean isStart = false;
  private long closeAllLastTime;
  private long mergeAllLastTime;

  private CloseMergeService() {
    service = IoTDBThreadPoolFactory
        .newScheduledThreadPool(2, ThreadName.CLOSE_MERGE_SERVICE.getName());
  }

  /**
   * get instance of CloseMergeService.
   *
   * @return CloseMergeService instance
   */
  public static synchronized CloseMergeService getInstance() {
    if (closeMergeService == null) {
      closeMergeService = new CloseMergeService();
    }
    return closeMergeService;
  }

  /**
   * start service.
   */
  public void startService() {
    if (dbConfig.isEnableTimingCloseAndMerge()) {
      if (!isStart) {
        LOGGER.info("Start the close and merge service");
        closeAndMergeDaemon.start();
        isStart = true;
        closeAllLastTime = System.currentTimeMillis();
        mergeAllLastTime = System.currentTimeMillis();
      } else {
        LOGGER.warn("The close and merge service has been already running.");
      }
    } else {
      LOGGER.info("Cannot start close and merge service, it is disabled by configuration.");
    }
  }

  /**
   * close service.
   */
  public void closeService() {
    if (dbConfig.isEnableTimingCloseAndMerge()) {
      if (isStart) {
        LOGGER.info("Prepare to shutdown the close and merge service.");
        isStart = false;
        synchronized (service) {
          service.shutdown();
          service.notifyAll();
        }
        resetCloseMergeService();
        LOGGER.info("Shutdown close and merge service successfully.");
      } else {
        LOGGER.warn("The close and merge service is not running now.");
      }
    }
  }

  private static void resetCloseMergeService(){
    closeMergeService = null;
  }

  @Override
  public void start() throws StartupException {
    try {
      startService();
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      LOGGER.error(errorMessage);
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    closeService();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CLOSE_MERGE_SERVICE;
  }

  private class CloseAndMergeDaemon extends Thread {

    public CloseAndMergeDaemon() {
      super(ThreadName.CLOSE_MERGE_DAEMON.getName());
    }

    @Override
    public void run() {
      service.scheduleWithFixedDelay(mergeService, MERGE_DELAY, MERGE_PERIOD, TimeUnit.SECONDS);
      service.scheduleWithFixedDelay(closeService, CLOSE_DELAY, CLOSE_PERIOD, TimeUnit.SECONDS);
      while (!service.isShutdown()) {
        synchronized (service) {
          try {
            service.wait();
          } catch (InterruptedException e) {
            LOGGER.error("Close and merge error.", e);
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  private class MergeServiceThread extends Thread {

    public MergeServiceThread() {
      super(ThreadName.MERGE_DAEMON.getName());
    }

    @Override
    public void run() {
      long thisMergeTime = System.currentTimeMillis();
      ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(mergeAllLastTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisMergeTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      long timeInterval = thisMergeTime - mergeAllLastTime;
      LOGGER.info(
          "Start the merge action regularly, last time is {}, this time is {}, "
              + "time interval is {}s.", startDateTime, endDateTime, timeInterval / 1000);
      mergeAllLastTime = System.currentTimeMillis();
      try {
        FileNodeManager.getInstance().mergeAll();
      } catch (Exception e) {
        LOGGER.error("Merge all error.", e);
      }
    }
  }

  private class CloseServiceThread extends Thread {

    public CloseServiceThread() {
      super(ThreadName.CLOSE_DAEMON.getName());
    }

    @Override
    public void run() {
      long thisCloseTime = System.currentTimeMillis();
      ZonedDateTime startDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(closeAllLastTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      ZonedDateTime endDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(thisCloseTime),
          IoTDBDescriptor.getInstance().getConfig().getZoneID());
      long timeInterval = thisCloseTime - closeAllLastTime;
      LOGGER.info(
          "Start the close action regularly, last time is {}, this time is {}, "
              + "time interval is {}s.", startDateTime, endDateTime, timeInterval / 1000);
      closeAllLastTime = System.currentTimeMillis();
      try {
        FileNodeManager.getInstance().closeAll();
      } catch (Exception e) {
        LOGGER.error("close all error.", e);
      }
    }
  }
}
