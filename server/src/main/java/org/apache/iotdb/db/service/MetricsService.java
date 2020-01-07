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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.ThreadName;
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

public class MetricsService implements MetricsServiceMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
  private final String mbeanName = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private Server server;
  private ExecutorService executorService;

  public static final MetricsService getInstance() {
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
  public synchronized void startService() throws StartupException {
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    executorService = Executors.newSingleThreadExecutor();
    int port = getMetricsPort();
    MetricsSystem metricsSystem = new MetricsSystem(new ServerArgument(port));
    MetricsWebUI metricsWebUI = new MetricsWebUI(metricsSystem.getMetricRegistry());
    metricsWebUI.getHandlers().add(metricsSystem.getServletHandlers());
    metricsWebUI.initialize();
    server = metricsWebUI.getServer(port);
    server.setStopTimeout(
        IoTDBDescriptor.getInstance().getConfig().getMetricServiceAwaitTimeForStopService());
    metricsSystem.start();
    executorService.execute(new MetricsServiceThread(server));
    logger.info("{}: start {} successfully, listening on ip {} port {}",
        IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(),
        IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
        IoTDBDescriptor.getInstance().getConfig().getMetricsPort());
  }

  @Override
  public void restartService() throws StartupException {
    stopService();
    startService();
  }

  @Override
  public void stopService() {
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      if (server != null) {
        server.stop();
      }
      if (executorService != null) {
        executorService.shutdown();
        if (!executorService.awaitTermination(
            IoTDBDescriptor.getInstance().getConfig().getMetricServiceAwaitTimeForStopService(),
            TimeUnit.MILLISECONDS)) {
          executorService.shutdownNow();
        }
      }
    } catch (Exception e) {
      logger
          .error("{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(),
              e);
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  private static class MetricsServiceHolder {

    private static final MetricsService INSTANCE = new MetricsService();

    private MetricsServiceHolder() {}
  }

  private class MetricsServiceThread implements Runnable {

    private Server server;

    public MetricsServiceThread(Server server) {
      this.server = server;
    }

    @Override
    public void run() {
      try {
        Thread.currentThread().setName(ThreadName.METRICS_SERVICE.getName());
        server.start();
        server.join();
      } catch (Exception e) {
        logger.error("{}: failed to start {}, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      }
    }
  }
}
