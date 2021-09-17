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
package org.apache.iotdb.db.service;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metrics.server.MetricsSystem;
import org.apache.iotdb.db.metrics.server.ServerArgument;
import org.apache.iotdb.db.metrics.ui.MetricsWebUI;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetricsService implements MetricsServiceMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private Server server;
  private ExecutorService executorService;

  private MetricsService() {}

  public static MetricsService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRICS_SERVICE;
  }

  @Override
  public int getMetricsPort() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.getMetricsPort();
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public synchronized void startService() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableMetricService()) {
      return;
    }
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    executorService = Executors.newSingleThreadExecutor();
    int port = getMetricsPort();
    MetricsSystem metricsSystem = new MetricsSystem(new ServerArgument(port));
    MetricsWebUI metricsWebUI = new MetricsWebUI(metricsSystem.getMetricRegistry());
    metricsWebUI.getHandlers().add(metricsSystem.getServletHandlers());
    metricsWebUI.initialize();
    server = metricsWebUI.getServer(port);
    server.setStopTimeout(10000);
    metricsSystem.start();
    try {
      executorService.execute(new MetricsServiceThread(server));
      logger.info(
          "{}: start {} successfully, listening on ip {} port {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName(),
          IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
          IoTDBDescriptor.getInstance().getConfig().getMetricsPort());
    } catch (NullPointerException e) {
      // issue IOTDB-415, we need to stop the service.
      logger.error(
          "{}: start {} failed, listening on ip {} port {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName(),
          IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
          IoTDBDescriptor.getInstance().getConfig().getMetricsPort());
      stopService();
    }
  }

  @Override
  public void restartService() {
    stopService();
    startService();
  }

  @Override
  public void stopService() {
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      if (server != null) {
        server.stop();
        server = null;
      }
      if (executorService != null) {
        executorService.shutdown();
        if (!executorService.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
          executorService.shutdownNow();
        }
        executorService = null;
      }
    } catch (InterruptedException e) {
      logger.warn("MetricsService can not be closed in {} ms", 3000);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error(
          "{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      executorService.shutdownNow();
    }
    checkAndWaitPortIsClosed();
    logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  private void checkAndWaitPortIsClosed() {
    SocketAddress socketAddress = new InetSocketAddress("localhost", getMetricsPort());
    @SuppressWarnings("squid:S2095")
    Socket socket = new Socket();
    int timeout = 1;
    int count = 10000; // 10 seconds
    while (count > 0) {
      try {
        socket.connect(socketAddress, timeout);
        count--;
      } catch (IOException e) {
        return;
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          // do nothing
        }
      }
    }
    logger.error("Port {} can not be closed.", getMetricsPort());
  }

  private static class MetricsServiceHolder {

    private static final MetricsService INSTANCE = new MetricsService();

    private MetricsServiceHolder() {}
  }

  private class MetricsServiceThread extends WrappedRunnable {

    private Server server;

    public MetricsServiceThread(Server server) {
      this.server = server;
    }

    @Override
    public void runMayThrow() {
      try {
        Thread.currentThread().setName(ThreadName.METRICS_SERVICE.getName());
        server.start();
        server.join();
      } catch (
          @SuppressWarnings("squid:S2142")
          InterruptedException e1) {
        // we do not sure why InterruptedException happens, but it indeed occurs in Travis WinOS
        logger.error(e1.getMessage(), e1);
      } catch (Exception e) {
        logger.error(
            "{}: failed to start {}, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      }
    }
  }
}
