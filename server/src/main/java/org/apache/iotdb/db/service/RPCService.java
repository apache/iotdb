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
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.CustomizedTThreadPoolServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.CustomizedTThreadPoolServer.Args;
import org.apache.thrift.transport.CustomizedTServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to handle jdbc request from client.
 */
public class RPCService implements RPCServiceMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(RPCService.class);
  private static final String STATUS_UP = "UP";
  private static final String STATUS_DOWN = "DOWN";
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private RPCServiceThread rpcServiceThread;
  private TProtocolFactory protocolFactory;
  private Processor<TSIService.Iface> processor;
  private CustomizedTThreadPoolServer.Args poolArgs;
  private TSServiceImpl impl;

  private CountDownLatch stopLatch;

  private RPCService() {
  }

  public static final RPCService getInstance() {
    return RPCServiceHolder.INSTANCE;
  }

  @Override
  public String getRPCServiceStatus() {
    if (rpcServiceThread == null) {
      logger.debug("Start latch is null when getting status");
    } else {
      logger.debug("Start status is {} when getting status", rpcServiceThread.isServing());
    }
    if(stopLatch == null) {
      logger.debug("Stop latch is null when getting status");
    } else {
      logger.debug("Stop latch is {} when getting status", stopLatch.getCount());
    }	

    if(rpcServiceThread != null && rpcServiceThread.isServing()) {
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
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
  }

  @Override
  public void stop() {
    stopService();
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.RPC_SERVICE;
  }

  @Override
  @SuppressWarnings("squid:S2276")
  public synchronized void startService() throws StartupException {
    if (STATUS_UP.equals(getRPCServiceStatus())) {
      logger.info("{}: {} has been already running now", IoTDBConstant.GLOBAL_DB_NAME,
          this.getID().getName());
      return;
    }
    logger.info("{}: start {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    try {
      reset();
      rpcServiceThread = new RPCServiceThread(stopLatch);
      rpcServiceThread.setName(ThreadName.RPC_SERVICE.getName());
      rpcServiceThread.start();
      while (!rpcServiceThread.isServing()) {
        //sleep 100ms for waiting the rpc server start.
        Thread.sleep(100);
      }
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
    rpcServiceThread = null;
    stopLatch = new CountDownLatch(1);	  
  }

  @Override
  public synchronized void restartService() throws StartupException {
    stopService();
    startService();
  }

  @Override
  public synchronized void stopService() {
    if (STATUS_DOWN.equals(getRPCServiceStatus())) {
      logger.info("{}: {} isn't running now", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
      return;
    }
    logger.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    if (rpcServiceThread != null) {
      rpcServiceThread.close();
    }
    try {
      stopLatch.await();
      reset();
      logger.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (InterruptedException e) {
      logger.error("{}: close {} failed because: ", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(), e);
      Thread.currentThread().interrupt();
    }
  }

  private static class RPCServiceHolder {

    private static final RPCService INSTANCE = new RPCService();

    private RPCServiceHolder() {
    }
  }

  private class RPCServiceThread extends Thread {

    private CustomizedTServerSocket serverTransport;
    private TServer poolServer;
    private CountDownLatch threadStopLatch;

    public RPCServiceThread(CountDownLatch threadStopLatch)
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
      this.threadStopLatch = threadStopLatch;
    }

    @SuppressWarnings("squid:S2093") // socket will be used later
    @Override
    public void run() {
      logger.info("The RPC service thread begin to run...");
      try {
        IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
        serverTransport = new CustomizedTServerSocket(new InetSocketAddress(config.getRpcAddress(),
            config.getRpcPort()));
        //this is for testing.
        if (!serverTransport.getServerSocket().isBound()) {
          logger.error("The RPC service port is not bound.");
        }
        poolArgs = new Args(serverTransport).maxWorkerThreads(IoTDBDescriptor.
            getInstance().getConfig().getRpcMaxConcurrentClientNum()).minWorkerThreads(1)
            .stopTimeoutVal(
                IoTDBDescriptor.getInstance().getConfig().getThriftServerAwaitTimeForStopService());
        poolArgs.executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs,
            ThreadName.RPC_CLIENT.getName());
        poolArgs.processor(processor);
        poolArgs.protocolFactory(protocolFactory);
        poolServer = new CustomizedTThreadPoolServer(poolArgs);
        poolServer.serve();
      } catch (TTransportException e) {
        throw new RPCServiceException(String.format("%s: failed to start %s, because ", IoTDBConstant.GLOBAL_DB_NAME,
            getID().getName()), e);
      } catch (Exception e) {
        throw new RPCServiceException(String.format("%s: %s exit, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName()), e);
      } finally {
        close();
        if (threadStopLatch == null) {
          logger.debug("Stop Count Down latch is null");
        } else {
          logger.debug("Stop Count Down latch is {}", threadStopLatch.getCount());
        }

        if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
          threadStopLatch.countDown();
        }
        logger.debug("{}: close TThreadPoolServer and TServerSocket for {}",
            IoTDBConstant.GLOBAL_DB_NAME, getID().getName());
      }
    }

    private synchronized void close() {
      if (poolServer != null) {
        poolServer.setShouldStop(true);
        poolServer.stop();
        poolServer = null;
      }
      if (serverTransport != null) {
        serverTransport.close();
        serverTransport = null;
      }
    }

    boolean isServing() {
      if (poolServer != null) {
        return poolServer.isServing();
      }
      return false;
    }
  }
}
