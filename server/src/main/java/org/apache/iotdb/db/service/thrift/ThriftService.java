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

package org.apache.iotdb.db.service.thrift;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public abstract class ThriftService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(ThriftService.class);
  private static final String STATUS_UP = "UP";
  private static final String STATUS_DOWN = "DOWN";
  protected final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());
  protected ThriftServiceThread thriftServiceThread;
  protected TProcessor processor;

  private CountDownLatch stopLatch;

  public String getRPCServiceStatus() {
    if (thriftServiceThread == null) {
      logger.debug("Start latch is null when getting status");
    } else {
      logger.debug("Start status is {} when getting status", thriftServiceThread.isServing());
    }
    if (stopLatch == null) {
      logger.debug("Stop latch is null when getting status");
    } else {
      logger.debug("Stop latch is {} when getting status", stopLatch.getCount());
    }

    if (thriftServiceThread != null && thriftServiceThread.isServing()) {
      return STATUS_UP;
    } else {
      return STATUS_DOWN;
    }
  }

  public int getRPCPort() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.getRpcPort();
  }

  public abstract ThriftService getImplementation();

  @Override
  public void start() throws StartupException {
    JMXService.registerMBean(getImplementation(), mbeanName);
    startService();
  }

  @Override
  public void stop() {
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  public abstract void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException;

  public abstract void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException;

  public abstract String getBindIP();

  public abstract int getBindPort();

  @SuppressWarnings("squid:S2276")
  public void startService() throws StartupException {
    if (STATUS_UP.equals(getRPCServiceStatus())) {
      logger.info(
          "{}: {} has been already running now",
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
      return;
    }
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      reset();
      initTProcessor();
      initThriftServiceThread();
      thriftServiceThread.setThreadStopLatch(stopLatch);
      thriftServiceThread.start();

      while (!thriftServiceThread.isServing()) {
        // sleep 100ms for waiting the rpc server start.
        Thread.sleep(100);
      }
    } catch (InterruptedException
        | ClassNotFoundException
        | IllegalAccessException
        | InstantiationException e) {
      Thread.currentThread().interrupt();
      throw new StartupException(this.getID().getName(), e.getMessage());
    }

    logger.info(
        "{}: start {} successfully, listening on ip {} port {}",
        IoTDBConstant.GLOBAL_DB_NAME,
        this.getID().getName(),
        getBindIP(),
        getBindPort());
  }

  private void reset() {
    thriftServiceThread = null;
    stopLatch = new CountDownLatch(1);
  }

  public void restartService() throws StartupException {
    stopService();
    startService();
  }

  public void stopService() {
    if (STATUS_DOWN.equals(getRPCServiceStatus())) {
      logger.info("{}: {} isn't running now", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
      return;
    }
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    if (thriftServiceThread != null) {
      thriftServiceThread.close();
    }
    try {
      stopLatch.await();
      reset();
      logger.info(
          "{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (InterruptedException e) {
      logger.error(
          "{}: close {} failed because: ", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(), e);
      Thread.currentThread().interrupt();
    }
  }
}
