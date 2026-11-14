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

package org.apache.iotdb.consensus.traft.paper.runtime;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperBatch;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperControl;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperWrite;
import org.apache.iotdb.consensus.traft.paper.thrift.TRaftPaperService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Uses the same IoTDB Thrift framing, compression, SSL and endpoint configuration as Raft. */
public final class TRaftRpc implements Closeable {
  public interface Handler {
    /** Returns the encoded replies addressed to the batch sender; never null. */
    List<ByteBuffer> deliver(TPaperBatch request) throws Exception;

    TSStatus write(TPaperWrite request) throws Exception;

    TSStatus control(TPaperControl request) throws Exception;
  }

  private final TEndPoint endpoint;
  private final RaftConfig.Rpc config;
  private final Handler handler;
  private final Map<TEndPoint, ConcurrentLinkedQueue<TRaftPaperService.Client>> idle =
      new ConcurrentHashMap<>();
  // Size of each idle queue: ConcurrentLinkedQueue.size() walks the whole queue on every call.
  private final Map<TEndPoint, AtomicInteger> idleCount = new ConcurrentHashMap<>();
  private final java.util.Set<TTransport> transports = ConcurrentHashMap.newKeySet();
  private ThriftServiceThread server;
  private CountDownLatch stopped;
  private volatile boolean closed;

  public TRaftRpc(TEndPoint endpoint, RaftConfig.Rpc config, Handler handler) {
    this.endpoint = endpoint;
    this.config = config;
    this.handler = handler;
  }

  public void start() throws IOException {
    TRaftPaperService.Iface api =
        new TRaftPaperService.Iface() {
          public List<ByteBuffer> deliver(TPaperBatch r) throws TException {
            try {
              return handler.deliver(r);
            } catch (Exception e) {
              throw new TException(e);
            }
          }

          public TSStatus write(TPaperWrite r) throws TException {
            try {
              return handler.write(r);
            } catch (Exception e) {
              throw new TException(e);
            }
          }

          public TSStatus control(TPaperControl r) throws TException {
            try {
              return handler.control(r);
            } catch (Exception e) {
              throw new TException(e);
            }
          }
        };
    TServerEventHandler events =
        new TServerEventHandler() {
          public void preServe() {}

          public ServerContext createContext(TProtocol a, TProtocol b) {
            return null;
          }

          public void deleteContext(ServerContext c, TProtocol a, TProtocol b) {}

          public void processContext(ServerContext c, TTransport a, TTransport b) {}
        };
    try {
      TRaftPaperService.Processor<TRaftPaperService.Iface> processor =
          new TRaftPaperService.Processor<>(api);
      server =
          config.enableSsl
              ? new ThriftServiceThread(
                  processor,
                  "TRaftPaperRPC",
                  "TRaftPaperProcessor",
                  endpoint.getIp(),
                  endpoint.getPort(),
                  config.rpcMaxConcurrentClientNum,
                  config.thriftServerAwaitTimeForStopService,
                  events,
                  config.rpcThriftCompressionEnabled,
                  config.sslKeyStorePath,
                  config.sslKeyStorePassword,
                  config.sslTrustStorePath,
                  config.sslTrustStorePassword,
                  ZeroCopyRpcTransportFactory.INSTANCE)
              : new ThriftServiceThread(
                  processor,
                  "TRaftPaperRPC",
                  "TRaftPaperProcessor",
                  endpoint.getIp(),
                  endpoint.getPort(),
                  config.rpcMaxConcurrentClientNum,
                  config.thriftServerAwaitTimeForStopService,
                  events,
                  config.rpcThriftCompressionEnabled,
                  ZeroCopyRpcTransportFactory.INSTANCE);
      stopped = new CountDownLatch(1);
      server.setThreadStopLatch(stopped);
      server.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!server.isServing()) {
        if (!server.isAlive() || System.nanoTime() > deadline) {
          throw new IOException("TRaft RPC failed to bind " + endpoint);
        }
        Thread.sleep(5);
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Cannot start TRaft RPC at " + endpoint, e);
    }
  }

  private TRaftPaperService.Client borrow(TEndPoint target) throws TException {
    if (closed) {
      throw new TException("TRaft RPC closed");
    }
    TRaftPaperService.Client client =
        idle.computeIfAbsent(target, ignored -> new ConcurrentLinkedQueue<>()).poll();
    if (client != null) {
      idleCount(target).decrementAndGet();
      return client;
    }
    int timeout = (int) Math.min(Integer.MAX_VALUE, config.rpcTimeoutMs);
    TTransport transport =
        config.enableSsl
            ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                target.getIp(),
                target.getPort(),
                timeout,
                config.sslTrustStorePath,
                config.sslTrustStorePassword,
                config.sslKeyStorePath,
                config.sslKeyStorePassword)
            : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    target.getIp(),
                    target.getPort(),
                    timeout));
    TProtocol protocol =
        config.rpcThriftCompressionEnabled
            ? new TCompactProtocol(transport)
            : new TBinaryProtocol(transport);
    transport.open();
    transports.add(transport);
    return new TRaftPaperService.Client(protocol);
  }

  private AtomicInteger idleCount(TEndPoint target) {
    return idleCount.computeIfAbsent(target, ignored -> new AtomicInteger());
  }

  private interface Call<T> {
    T run(TRaftPaperService.Client client) throws TException;
  }

  private <T> T invoke(TEndPoint target, Call<T> call) throws TException {
    TRaftPaperService.Client client = borrow(target);
    boolean success = false;
    try {
      T result = call.run(client);
      success = true;
      return result;
    } finally {
      TTransport transport = client.getInputProtocol().getTransport();
      ConcurrentLinkedQueue<TRaftPaperService.Client> pool = idle.get(target);
      AtomicInteger count = idleCount(target);
      if (success && !closed && count.incrementAndGet() <= config.maxClientNumForEachNode) {
        pool.offer(client);
      } else {
        if (success && !closed) {
          count.decrementAndGet();
        }
        transports.remove(transport);
        transport.close();
      }
    }
  }

  /** Delivers one batch and returns the replies the receiver addressed to this node. */
  public List<ByteBuffer> deliver(TEndPoint target, TPaperBatch batch) throws TException {
    return invoke(target, c -> c.deliver(batch));
  }

  public TSStatus write(TEndPoint target, TPaperWrite request) throws TException {
    return invoke(target, c -> c.write(request));
  }

  public TSStatus control(TEndPoint target, TPaperControl request) throws TException {
    return invoke(target, c -> c.control(request));
  }

  @Override
  public void close() {
    closed = true;
    for (TTransport transport : transports) {
      transport.close();
    }
    transports.clear();
    idle.clear();
    idleCount.clear();
    if (server != null) {
      server.close();
      try {
        stopped.await(config.thriftServerAwaitTimeForStopService, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
