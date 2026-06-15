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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.WrappedThreadPoolExecutor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.i18n.ServiceMessages;

import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.file.AccessDeniedException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractThriftServiceThread extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(AbstractThriftServiceThread.class);
  private TServerTransport serverTransport;
  private TServer poolServer;
  private CountDownLatch threadStopLatch;

  private ExecutorService executorService;

  private String serviceName;

  private TProtocolFactory protocolFactory;

  private TTransportFactory transportFactory;

  // currently, we can reuse the ProtocolFactory instance.
  private static TCompactProtocol.Factory compactProtocolFactory = new TCompactProtocol.Factory();
  private static TBinaryProtocol.Factory binaryProtocolFactory = new TBinaryProtocol.Factory();
  private static final ServerContext NOOP_SERVER_CONTEXT =
      new ServerContext() {
        @Override
        public <T> T unwrap(Class<T> iface) {
          return iface.isInstance(this) ? iface.cast(this) : null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
          return iface.isInstance(this);
        }
      };

  private void initProtocolFactory(boolean compress) {
    protocolFactory = getProtocolFactory(compress);
  }

  public TTransportFactory getTTransportFactory() {
    return transportFactory;
  }

  public static TProtocolFactory getProtocolFactory(boolean compress) {
    if (compress) {
      return compactProtocolFactory;
    } else {
      return binaryProtocolFactory;
    }
  }

  private void catchFailedInitialization(TTransportException e) throws RPCServiceException {
    close();
    if (threadStopLatch == null) {
      logger.debug(ServiceMessages.STOP_COUNT_DOWN_LATCH_IS_NULL);
    } else {
      logger.debug(ServiceMessages.STOP_COUNT_DOWN_LATCH_IS, threadStopLatch.getCount());
    }
    if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
      threadStopLatch.countDown();
    }
    logger.debug(
        "{}: close TThreadPoolServer and TServerSocket for {}",
        IoTDBConstant.GLOBAL_DB_NAME,
        serviceName);
    logger.error(ServiceMessages.FAILED_TO_START_BECAUSE, e.getCause());
    throw new RPCServiceException(
        String.format(
            "%s: failed to start %s, because ", IoTDBConstant.GLOBAL_DB_NAME, serviceName),
        e);
  }

  /** For async ThriftService. */
  @SuppressWarnings("squid:S107")
  protected AbstractThriftServiceThread(
      TBaseAsyncProcessor<?> processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int selectorThreads,
      int maxWorkerThreads,
      int timeoutSecond,
      TServerEventHandler serverEventHandler,
      boolean compress,
      int connectionTimeoutInMS,
      int maxReadBufferBytes,
      ServerType serverType,
      TTransportFactory transportFactory) {
    this.transportFactory = transportFactory;
    initProtocolFactory(compress);
    this.serviceName = serviceName;
    try {
      serverTransport = openNonblockingTransport(bindAddress, port, connectionTimeoutInMS);
      switch (serverType) {
        case SELECTOR:
          TThreadedSelectorServer.Args poolArgs =
              initAsyncedSelectorPoolArgs(
                  processor,
                  threadsName,
                  selectorThreads,
                  maxWorkerThreads,
                  timeoutSecond,
                  maxReadBufferBytes);
          poolServer = new TThreadedSelectorServer(poolArgs);
          break;
        case HSHA:
          THsHaServer.Args poolArgs1 =
              initAsyncedHshaPoolArgs(
                  processor, threadsName, maxWorkerThreads, timeoutSecond, maxReadBufferBytes);
          poolServer = new THsHaServer(poolArgs1);
          break;
        default:
          logger.error(ServiceMessages.UNEXPECTED_SERVER_TYPE, serverType);
      }
      poolServer.setServerEventHandler(wrapServerEventHandler(serverEventHandler));
    } catch (TTransportException e) {
      catchFailedInitialization(e);
    }
  }

  /** Synced ThriftServiceThread with ssl enabled */
  @SuppressWarnings("squid:S107")
  protected AbstractThriftServiceThread(
      TProcessor processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int maxWorkerThreads,
      int timeoutSecond,
      TServerEventHandler serverEventHandler,
      boolean compress,
      String keyStorePath,
      String keyStorePwd,
      String trustStorePath,
      String trustStorePwd,
      int clientTimeout,
      TTransportFactory transportFactory) {
    this.transportFactory = transportFactory;
    initProtocolFactory(compress);
    this.serviceName = serviceName;

    try {
      validateCertificate(keyStorePath, keyStorePwd);
      TSSLTransportFactory.TSSLTransportParameters params =
          new TSSLTransportFactory.TSSLTransportParameters();
      params.setKeyStore(keyStorePath, keyStorePwd);
      if (trustStorePath != null && !trustStorePath.isEmpty()) {
        validateCertificate(trustStorePath, trustStorePwd);
        params.setTrustStore(trustStorePath, trustStorePwd);
        params.requireClientAuth(true);
      }
      InetSocketAddress socketAddress = new InetSocketAddress(bindAddress, port);
      serverTransport =
          TSSLTransportFactory.getServerSocket(
              socketAddress.getPort(), clientTimeout, socketAddress.getAddress(), params);
      TThreadPoolServer.Args poolArgs =
          initSyncedPoolArgs(processor, threadsName, maxWorkerThreads, timeoutSecond);
      poolServer = new TThreadPoolServer(poolArgs);
      poolServer.setServerEventHandler(wrapServerEventHandler(serverEventHandler));
    } catch (TTransportException e) {
      catchFailedInitialization(e);
    }
  }

  private static void validateCertificate(String keyStorePath, String keystorePassword)
      throws TTransportException {
    try {
      KeyStore keystore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(keyStorePath)) {
        keystore.load(fis, keystorePassword.toCharArray());
      }

      Enumeration<String> aliases = keystore.aliases();
      while (aliases.hasMoreElements()) {
        String currentAlias = aliases.nextElement();
        checkCertificate(keystore, currentAlias);
      }
    } catch (AccessDeniedException e) {
      throw new TTransportException(ServiceMessages.FAILED_TO_LOAD_KEYSTORE_OR_TRUSTSTORE);
    } catch (FileNotFoundException e) {
      throw new TTransportException(ServiceMessages.KEYSTORE_OR_TRUSTSTORE_NOT_FOUND);
    } catch (Exception e) {
      throw new TTransportException(e);
    }
  }

  private static void checkCertificate(KeyStore keystore, String alias) throws Exception {
    if (!keystore.containsAlias(alias)) {
      return;
    }

    X509Certificate cert = (X509Certificate) keystore.getCertificate(alias);
    if (cert == null) {
      return;
    }

    cert.checkValidity();
  }

  @SuppressWarnings("squid:S107")
  protected AbstractThriftServiceThread(
      TProcessor processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int maxWorkerThreads,
      int timeoutSecond,
      TServerEventHandler serverEventHandler,
      boolean compress,
      TTransportFactory transportFactory) {
    this.transportFactory = transportFactory;
    initProtocolFactory(compress);
    this.serviceName = serviceName;

    try {
      serverTransport = openTransport(bindAddress, port);
      TThreadPoolServer.Args poolArgs =
          initSyncedPoolArgs(processor, threadsName, maxWorkerThreads, timeoutSecond);
      poolServer = new TThreadPoolServer(poolArgs);
      poolServer.setServerEventHandler(wrapServerEventHandler(serverEventHandler));
    } catch (TTransportException e) {
      catchFailedInitialization(e);
    }
  }

  private TThreadPoolServer.Args initSyncedPoolArgs(
      TProcessor processor, String threadsName, int maxWorkerThreads, int timeoutSecond) {
    TThreadPoolServer.Args poolArgs = new TThreadPoolServer.Args(serverTransport);
    poolArgs.maxWorkerThreads(maxWorkerThreads).minWorkerThreads(0).stopTimeoutVal(timeoutSecond);
    executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs, threadsName);
    poolArgs.executorService = executorService;
    poolArgs.processor(processor);
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(getTTransportFactory());
    return poolArgs;
  }

  private static TServerEventHandler wrapServerEventHandler(
      TServerEventHandler serverEventHandler) {
    if (serverEventHandler == null) {
      return null;
    }
    return new TServerEventHandler() {
      @Override
      public void preServe() {
        serverEventHandler.preServe();
      }

      @Override
      public ServerContext createContext(TProtocol input, TProtocol output) {
        ServerContext serverContext = serverEventHandler.createContext(input, output);
        return serverContext == null ? NOOP_SERVER_CONTEXT : serverContext;
      }

      @Override
      public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
        serverEventHandler.deleteContext(serverContext, input, output);
      }

      @Override
      public void processContext(
          ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
        serverEventHandler.processContext(serverContext, inputTransport, outputTransport);
      }
    };
  }

  private TThreadedSelectorServer.Args initAsyncedSelectorPoolArgs(
      TBaseAsyncProcessor<?> processor,
      String threadsName,
      int selectorThreads,
      int maxWorkerThreads,
      int timeoutSecond,
      int maxReadBufferBytes) {
    TThreadedSelectorServer.Args poolArgs =
        new TThreadedSelectorServer.Args((TNonblockingServerTransport) serverTransport);
    poolArgs.maxReadBufferBytes = maxReadBufferBytes;
    poolArgs.selectorThreads(selectorThreads);
    executorService =
        IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(
            0, maxWorkerThreads, timeoutSecond, TimeUnit.SECONDS, threadsName);
    poolArgs.executorService(executorService);
    poolArgs.processor(processor);
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(getTTransportFactory());
    return poolArgs;
  }

  private THsHaServer.Args initAsyncedHshaPoolArgs(
      TBaseAsyncProcessor<?> processor,
      String threadsName,
      int maxWorkerThreads,
      int timeoutSecond,
      int maxReadBufferBytes) {
    THsHaServer.Args poolArgs = new THsHaServer.Args((TNonblockingServerTransport) serverTransport);
    poolArgs.maxReadBufferBytes = maxReadBufferBytes;
    executorService =
        IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(
            0, maxWorkerThreads, timeoutSecond, TimeUnit.SECONDS, threadsName);
    poolArgs.executorService(executorService);
    poolArgs.processor(processor);
    poolArgs.protocolFactory(protocolFactory);
    poolArgs.transportFactory(getTTransportFactory());
    return poolArgs;
  }

  @SuppressWarnings("java:S2259")
  private TServerTransport openTransport(String bindAddress, int port) throws TTransportException {
    if (bindAddress == null) {
      // bind any address
      return new TServerSocket(new InetSocketAddress(port));
    }
    return new TServerSocket(new InetSocketAddress(bindAddress, port));
  }

  private TServerTransport openNonblockingTransport(
      String bindAddress, int port, int connectionTimeoutInMS) throws TTransportException {
    if (bindAddress == null) {
      // bind any address
      return new TNonblockingServerSocket(new InetSocketAddress(port), connectionTimeoutInMS);
    }
    return new TNonblockingServerSocket(
        new InetSocketAddress(bindAddress, port), connectionTimeoutInMS);
  }

  public void setThreadStopLatch(CountDownLatch threadStopLatch) {
    this.threadStopLatch = threadStopLatch;
  }

  @SuppressWarnings("squid:S2093") // socket will be used later
  @Override
  public void run() {
    logger.info(ServiceMessages.SERVICE_THREAD_BEGIN_TO_RUN, serviceName);
    try {
      poolServer.serve();
    } catch (Exception e) {
      throw new RPCServiceException(
          String.format(
              ServiceMessages.SERVICE_EXIT_BECAUSE, IoTDBConstant.GLOBAL_DB_NAME, serviceName),
          e);
    } finally {
      close();
      if (threadStopLatch == null) {
        logger.debug(ServiceMessages.STOP_COUNT_DOWN_LATCH_IS_NULL);
      } else {
        logger.debug(ServiceMessages.STOP_COUNT_DOWN_LATCH_IS, threadStopLatch.getCount());
      }

      if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
        threadStopLatch.countDown();
      }
      logger.debug(
          "{}: close TThreadPoolServer and TServerSocket for {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          serviceName);
    }
  }

  public synchronized void close() {
    if (poolServer != null) {
      poolServer.setShouldStop(true);
      poolServer.stop();

      poolServer = null;
    }
    if (serverTransport != null) {
      serverTransport.close();
      serverTransport = null;
    }
    // fix bug when DataNode.stop()
    if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
      threadStopLatch.countDown();
    }
  }

  public boolean isServing() {
    if (poolServer != null) {
      return poolServer.isServing();
    }
    return false;
  }

  public long getActiveThreadCount() {
    if (executorService != null) {
      return ((WrappedThreadPoolExecutor) executorService).getActiveCount();
    }
    return -1;
  }

  public enum ServerType {
    SELECTOR,
    HSHA
  }
}
