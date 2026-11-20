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

package org.apache.iotdb.pipe.it.dual;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/** Forwards real RPCs unchanged and records the receiver's responses at the wire boundary. */
public final class PipeTransferObserver implements AutoCloseable {
  private final List<TPipeTransferReq> requests = new CopyOnWriteArrayList<>();
  private final List<Transfer> transfers = new CopyOnWriteArrayList<>();
  private final Set<IoTDBSyncClient> clients = ConcurrentHashMap.newKeySet();
  private final Set<TTransport> incomingTransports = ConcurrentHashMap.newKeySet();
  private final ExecutorService workers = Executors.newCachedThreadPool();
  private final ServerSocket socket;
  private final TThreadPoolServer server;
  private final Thread serverThread;
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile CountDownLatch dataGate = new CountDownLatch(0);

  public PipeTransferObserver(final DataNodeWrapper receiver) throws Exception {
    socket = new ServerSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    server =
        new TThreadPoolServer(
            new TThreadPoolServer.Args(new TServerSocket(socket))
                .processorFactory(
                    new TProcessorFactory(null) {
                      @Override
                      public TProcessor getProcessor(final TTransport transport) {
                        incomingTransports.add(transport);
                        // A separate upstream connection preserves each receiver session's
                        // identity.
                        final IoTDBSyncClient client;
                        try {
                          client =
                              new IoTDBSyncClient(
                                  new ThriftClientProperty.Builder().build(),
                                  receiver.getIp(),
                                  receiver.getPort(),
                                  false,
                                  null,
                                  null);
                          clients.add(client);
                        } catch (final Exception e) {
                          throw new IllegalStateException(
                              "Cannot connect the test RPC observer", e);
                        }
                        final IClientRPCService.Iface forwarding =
                            (IClientRPCService.Iface)
                                Proxy.newProxyInstance(
                                    IClientRPCService.Iface.class.getClassLoader(),
                                    new Class<?>[] {IClientRPCService.Iface.class},
                                    (proxy, method, args) -> {
                                      try {
                                        final TPipeTransferReq request =
                                            "pipeTransfer".equals(method.getName())
                                                ? ((TPipeTransferReq) args[0]).deepCopy()
                                                : null;
                                        if (request != null
                                            && (request.getType()
                                                    == PipeRequestType.HANDSHAKE_DATANODE_V1
                                                        .getType()
                                                || request.getType()
                                                    == PipeRequestType.HANDSHAKE_DATANODE_V2
                                                        .getType())) {
                                          // Handshakes contain credentials. Only the type and
                                          // response are needed.
                                          request.setBody(new byte[0]);
                                        } else if (request != null
                                            && !dataGate.await(60, TimeUnit.SECONDS)) {
                                          throw new TException("Test relay data gate timed out");
                                        }
                                        if (request != null) {
                                          // Record attempts before forwarding: receiver visibility
                                          // must not hide an unfinished or failed response.
                                          requests.add(request);
                                        }
                                        final Object result = method.invoke(client, args);
                                        if (request != null) {
                                          transfers.add(
                                              new Transfer(
                                                  request,
                                                  ((TPipeTransferResp) result).deepCopy()));
                                        }
                                        return result;
                                      } catch (final InvocationTargetException e) {
                                        throw e.getCause();
                                      }
                                    });
                        final TProcessor processor = new IClientRPCService.Processor<>(forwarding);
                        return (input, output) -> {
                          try {
                            processor.process(input, output);
                          } catch (final TException | RuntimeException e) {
                            client.invalidate();
                            clients.remove(client);
                            incomingTransports.remove(transport);
                            throw e;
                          }
                        };
                      }
                    })
                .protocolFactory(new TBinaryProtocol.Factory())
                .transportFactory(new TFramedTransport.Factory())
                .executorService(workers)
                .stopTimeoutVal(1)
                .stopTimeoutUnit(TimeUnit.SECONDS));
    serverThread = new Thread(server::serve, "pipe-transfer-observer");
    serverThread.setDaemon(true);
    serverThread.start();
    await().atMost(5, TimeUnit.SECONDS).until(server::isServing);
  }

  public String getEndpoint() {
    return "127.0.0.1:" + socket.getLocalPort();
  }

  public List<Transfer> getTransfers() {
    return new ArrayList<>(transfers);
  }

  public List<TPipeTransferReq> getRequests() {
    return new ArrayList<>(requests);
  }

  public void awaitNoConnections() {
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> clients.isEmpty() && incomingTransports.isEmpty());
  }

  public void blockDataTransfers() {
    dataGate = new CountDownLatch(1);
  }

  public void releaseDataTransfers() {
    dataGate.countDown();
  }

  @Override
  public void close() throws Exception {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    releaseDataTransfers();
    server.stop();
    incomingTransports.forEach(TTransport::close);
    clients.forEach(IoTDBSyncClient::invalidate);
    socket.close();
    workers.shutdownNow();
    Assert.assertTrue(
        "RPC observer workers did not stop", workers.awaitTermination(5, TimeUnit.SECONDS));
    serverThread.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse("RPC observer acceptor did not stop", serverThread.isAlive());
  }

  public static final class Transfer {
    public final TPipeTransferReq request;
    public final TPipeTransferResp response;

    private Transfer(final TPipeTransferReq request, final TPipeTransferResp response) {
      this.request = request;
      this.response = response;
    }
  }
}
