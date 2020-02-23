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

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.runtime.JDBCServiceException;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to handle jdbc request from client.
 */
public class JDBCService implements JDBCServiceMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(JDBCService.class);
  private static final String STATUS_UP = "UP";
  private static final String STATUS_DOWN = "DOWN";
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private Thread jdbcServiceThread;
  private TProtocolFactory protocolFactory;
  private Processor<TSIService.Iface> processor;
  private TThreadPoolServer.Args poolArgs;
  private TSServiceImpl impl;
  private CountDownLatch startLatch;
  private CountDownLatch stopLatch;

  private JDBCService() {
  }

  public static final JDBCService getInstance() {
    return JDBCServiceHolder.INSTANCE;
  }

  @Override
  public String getJDBCServiceStatus() {
    // TODO debug log, will be deleted in production env
    if(startLatch == null) {
      logger.info("Start latch is null when getting status");
    } else {
      logger.info("Start latch is {} when getting status", startLatch.getCount());
    }
    if(stopLatch == null) {
      logger.info("Stop latch is null when getting status");
    } else {
      logger.info("Stop latch is {} when getting status", stopLatch.getCount());
    }	
    // debug log, will be deleted in production env

    if(startLatch != null && startLatch.getCount() == 0) {
      return STATUS_UP;
    } else {
      return STATUS_DOWN;
    }
  }

  @Override
  public int getRPCPort() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.getRpcPort();
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.JDBC_SERVICE;
  }

  @Override
  public synchronized void startService() throws StartupException {
    if (STATUS_UP.equals(getJDBCServiceStatus())) {
      logger.info("{}: {} has been already running now", IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
      return;
    }
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      reset();
      jdbcServiceThread = new JDBCServiceThread(startLatch, stopLatch);
      jdbcServiceThread.setName(ThreadName.JDBC_SERVICE.getName());
      jdbcServiceThread.start();
      startLatch.await();
    } catch (InterruptedException | ClassNotFoundException |
        IllegalAccessException | InstantiationException e) {
      Thread.currentThread().interrupt();
      throw new StartupException(this.getID().getName(), e.getMessage());
    }

    logger.info("{}: start {} successfully, listening on ip {} port {}", IoTDBConstant.GLOBAL_DB_NAME,
        this.getID().getName(), IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
        IoTDBDescriptor.getInstance().getConfig().getRpcPort());
  }
  
  private void reset() {
    startLatch = new CountDownLatch(1);
    stopLatch = new CountDownLatch(1);	  
  }

  @Override
  public synchronized void restartService() throws StartupException {
    stopService();
    startService();
  }

  @Override
  public synchronized void stopService() {
    if (STATUS_DOWN.equals(getJDBCServiceStatus())) {
      logger.info("{}: {} isn't running now", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
      return;
    }
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    if (jdbcServiceThread != null) {
      ((JDBCServiceThread) jdbcServiceThread).close();
    }
    try {
      stopLatch.await();
      reset();
      logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (InterruptedException e) {
      logger.error("{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(), e);
      Thread.currentThread().interrupt();
    }
  }

  private static class JDBCServiceHolder {

    private static final JDBCService INSTANCE = new JDBCService();

    private JDBCServiceHolder() {
    }
  }

  private class JDBCServiceThread extends Thread {

    private TServerSocket serverTransport;
    private TServer poolServer;
    private CountDownLatch threadStartLatch;
    private CountDownLatch threadStopLatch;

    public JDBCServiceThread(CountDownLatch threadStartLatch, CountDownLatch threadStopLatch)
        throws ClassNotFoundException, IllegalAccessException, InstantiationException {
      if(IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
        protocolFactory = new TCompactProtocol.Factory();
      }
      else {
        protocolFactory = new TBinaryProtocol.Factory();
      }
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      impl = (TSServiceImpl) Class.forName(config.getRpcImplClassName()).newInstance();
      processor = new TSIService.Processor<>(impl);
      this.threadStartLatch = threadStartLatch;
      this.threadStopLatch = threadStopLatch;
    }

    @SuppressWarnings("squid:S2093") // socket will be used later
    @Override
    public void run() {
      try {
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        serverTransport = new TServerSocket(new InetSocketAddress(config.getRpcAddress(),
            config.getRpcPort()));
        poolArgs = new Args(serverTransport).maxWorkerThreads(IoTDBDescriptor.
            getInstance().getConfig().getRpcMaxConcurrentClientNum()).minWorkerThreads(1)
            .stopTimeoutVal(
                IoTDBDescriptor.getInstance().getConfig().getThriftServerAwaitTimeForStopService());
        poolArgs.executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs,
            ThreadName.JDBC_CLIENT.getName());
        poolArgs.processor(processor);
        poolArgs.protocolFactory(protocolFactory);
        poolServer = new TThreadPoolServer(poolArgs);
        poolServer.setServerEventHandler(new JDBCServiceEventHandler(impl, threadStartLatch));
        poolServer.serve();
      } catch (TTransportException e) {
        throw new JDBCServiceException(String.format("%s: failed to start %s, because ", IoTDBConstant.GLOBAL_DB_NAME,
            getID().getName()), e);
      } catch (Exception e) {
        throw new JDBCServiceException(String.format("%s: %s exit, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName()), e);
      } finally {
        close();
        // TODO debug log, will be deleted in production env
        if (threadStopLatch == null) {
          logger.info("Stop Count Down latch is null");
        } else {
          logger.info("Stop Count Down latch is {}", threadStopLatch.getCount());
        }
        // debug log, will be deleted in production env

        if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
          threadStopLatch.countDown();
        }
        logger.info("{}: close TThreadPoolServer and TServerSocket for {}",
            IoTDBConstant.GLOBAL_DB_NAME,
            getID().getName());
      }
    }

    private synchronized void close() {
      if (poolServer != null) {
        poolServer.stop();
        poolServer = null;
      }
      if (serverTransport != null) {
        serverTransport.close();
        serverTransport = null;
      }
    }
  }
}
