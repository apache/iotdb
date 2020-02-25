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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rest.util.RestUtil;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestService implements RestServiceMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(RestService.class);
  private final String mbeanName = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private Server server;
  private ExecutorService executorService;

  public static final RestService getInstance() {
    return RestServiceHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRICS_SERVICE;
  }

  @Override
  public int getRestPort() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.getRestPort();
  }

  @Override
  public void start() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableMetricsWebService()) {
      return;
    }
    try {
      startService();
      JMXService.registerMBean(getInstance(), mbeanName);
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
    }
  }

  @Override
  public void stop() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableMetricsWebService()) {
      return;
    }
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public synchronized void startService() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableMetricsWebService()) {
      return;
    }
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    executorService = Executors.newSingleThreadExecutor();
    int port = getRestPort();
    server = RestUtil.getJettyServer(RestUtil.getRestContextHandler(), port);
    server.setStopTimeout(10000);
    try {
      executorService.execute(new MetricsServiceThread(server));
      logger.info("{}: start {} successfully, listening on ip {} port {}",
          IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(),
          IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
          IoTDBDescriptor.getInstance().getConfig().getRestPort());
    } catch (NullPointerException e) {
      //issue IOTDB-415, we need to stop the service.
      logger.error("{}: start {} failed, listening on ip {} port {}",
          IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(),
          IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
          IoTDBDescriptor.getInstance().getConfig().getRestPort());
      stopService();
    }
  }

  @Override
  public void restartService(){
    stopService();
    startService();
  }

  @Override
  public void stopService() {
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableMetricsWebService()) {
      return;
    }
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      if (server != null) {
        server.stop();
        server.destroy();
        server = null;
      }
      if (executorService != null) {
        executorService.shutdown();
        if (!executorService.awaitTermination(
            3000, TimeUnit.MILLISECONDS)) {
          executorService.shutdownNow();
        }
        executorService = null;
      }
    } catch (Exception e) {
      logger
          .error("{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(),
              e);
      executorService.shutdownNow();
    }
    checkAndWaitPortIsClosed();
    logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  private void checkAndWaitPortIsClosed() {
    SocketAddress socketAddress = new InetSocketAddress("localhost", getRestPort());
    @SuppressWarnings("squid:S2095")
    Socket socket = new Socket();
    try {
      socket.connect(socketAddress, 1000);
    } catch (IOException e) {
      logger.error(e.getMessage());
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        logger.error("Port {} can not be closed.", getRestPort());
      }
    }
  }

  private static class RestServiceHolder {

    private static final RestService INSTANCE = new RestService();

    private RestServiceHolder() {}
  }

  private class MetricsServiceThread implements Runnable {

    private Server server;

    MetricsServiceThread(Server server) {
      this.server = server;
    }

    @Override
    public void run() {
      try {
        Thread.currentThread().setName(ThreadName.METRICS_SERVICE.getName());
        server.start();
        server.join();
      } catch (@SuppressWarnings("squid:S2142") InterruptedException e1) {
        //we do not sure why InterruptedException happens, but it indeed occurs in Travis WinOS
        logger.error(e1.getMessage(), e1);
      } catch (Exception e) {
        logger.error("{}: failed to start {}, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      }
    }
  }
}
