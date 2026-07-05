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

package org.apache.iotdb.commons.service;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.i18n.ServiceMessages;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;

public abstract class ThriftService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(ThriftService.class);

  public static final String STATUS_UP = "UP";
  public static final String STATUS_DOWN = "DOWN";

  protected String mbeanName =
      String.format(
          "%s:%s=%s",
          IoTDBConstant.IOTDB_SERVICE_JMX_NAME, IoTDBConstant.JMX_TYPE, getID().getJmxName());
  protected AbstractThriftServiceThread thriftServiceThread;
  protected TProcessor processor;

  private CountDownLatch stopLatch;

  public String getRPCServiceStatus() {
    if (thriftServiceThread == null) {
      logger.debug(ServiceMessages.START_LATCH_NULL_WHEN_GETTING_STATUS);
    } else {
      logger.debug(
          ServiceMessages.START_STATUS_WHEN_GETTING_STATUS, thriftServiceThread.isServing());
    }
    if (stopLatch == null) {
      logger.debug(ServiceMessages.STOP_LATCH_NULL_WHEN_GETTING_STATUS);
    } else {
      logger.debug(ServiceMessages.STOP_LATCH_WHEN_GETTING_STATUS, stopLatch.getCount());
    }

    if (thriftServiceThread != null && thriftServiceThread.isServing()) {
      return STATUS_UP;
    } else {
      return STATUS_DOWN;
    }
  }

  @Override
  public void start() throws StartupException {
    JMXService.registerMBean(this, mbeanName);
    startService();
  }

  @Override
  public void stop() {
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  public void initSyncedServiceImpl(Object serviceImpl) {}

  public void initAsyncServiceImpl(Object serviceImpl) {}

  public abstract void initTProcessor()
      throws ClassNotFoundException,
          IllegalAccessException,
          InstantiationException,
          NoSuchMethodException,
          InvocationTargetException;

  public abstract void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException;

  public abstract String getBindIP();

  public abstract int getBindPort();

  @SuppressWarnings("squid:S2276")
  public void startService() throws StartupException {
    if (STATUS_UP.equals(getRPCServiceStatus())) {
      logger.info(
          ServiceMessages.SERVICE_ALREADY_RUNNING,
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
      return;
    }
    logger.info(
        ServiceMessages.START_SERVICE, IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
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
        | InstantiationException
        | NoSuchMethodException
        | InvocationTargetException e) {
      Thread.currentThread().interrupt();
      throw new StartupException(this.getID().getName(), e.getMessage());
    }

    logger.info(
        ServiceMessages.START_SERVICE_SUCCESSFULLY,
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
      logger.info(
          ServiceMessages.SERVICE_NOT_RUNNING,
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
      return;
    }
    logger.info(
        ServiceMessages.CLOSING_SERVICE, IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    if (thriftServiceThread != null) {
      thriftServiceThread.close();
    }
    try {
      stopLatch.await();
      reset();
      logger.info(
          ServiceMessages.CLOSE_SERVICE_SUCCESSFULLY,
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
    } catch (InterruptedException e) {
      logger.error(
          ServiceMessages.CLOSE_SERVICE_FAILED,
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName(),
          e);
      Thread.currentThread().interrupt();
    }
  }
}
