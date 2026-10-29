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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.service.NoopServerContext;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

/** A real Thrift relay with deterministic failure boundaries, without changing the receiver. */
final class PipeTransferFaultInjector implements AutoCloseable {

  enum Mode {
    PASS,
    PAUSE_BEFORE_FORWARD,
    REJECT_BEFORE_FORWARD,
    LOSE_RESPONSE
  }

  private final DataNodeWrapper receiver;
  private final TServerSocket socket;
  private final TThreadPoolServer server;
  private final ExecutorService executor = Executors.newFixedThreadPool(16);
  private final Map<TTransport, IoTDBSyncClient> clients = new ConcurrentHashMap<>();
  private final Set<TTransport> senderTransports = ConcurrentHashMap.newKeySet();
  private final AtomicInteger pausedRequests = new AtomicInteger();
  private final AtomicInteger rejectedRequests = new AtomicInteger();
  private final AtomicInteger lostResponses = new AtomicInteger();
  private final AtomicInteger committedResponses = new AtomicInteger();
  private final AtomicInteger forwardingFailures = new AtomicInteger();
  private final AtomicInteger successfulDataRequests = new AtomicInteger();
  private final Map<TPipeTransferReq, AtomicInteger> failedRequests = new ConcurrentHashMap<>();
  private final CountDownLatch release = new CountDownLatch(1);
  private final CountDownLatch releaseResponseLoss = new CountDownLatch(1);
  private final Thread serverThread;
  private volatile Mode mode;

  PipeTransferFaultInjector(final DataNodeWrapper receiver, final Mode mode) throws Exception {
    this.receiver = receiver;
    this.mode = mode;
    socket = new TServerSocket(new InetSocketAddress(receiver.getIp(), 0));
    server =
        new TThreadPoolServer(
            new TThreadPoolServer.Args(socket)
                .processor(this::process)
                .protocolFactory(new TBinaryProtocol.Factory())
                .transportFactory(DeepCopyRpcTransportFactory.INSTANCE)
                .executorService(executor));
    server.setServerEventHandler(
        new TServerEventHandler() {
          @Override
          public void preServe() {}

          @Override
          public ServerContext createContext(final TProtocol input, final TProtocol output) {
            senderTransports.add(input.getTransport());
            return NoopServerContext.INSTANCE;
          }

          @Override
          public void deleteContext(
              final ServerContext context, final TProtocol input, final TProtocol output) {
            // Keep receiver sessions paired with sender connections, including injected failures.
            final IoTDBSyncClient client = clients.remove(input.getTransport());
            if (client != null) {
              client.invalidate();
            }
            senderTransports.remove(input.getTransport());
          }

          @Override
          public void processContext(
              final ServerContext context, final TTransport input, final TTransport output) {}
        });
    serverThread = new Thread(server::serve, "pipe-transfer-fault-injector");
    serverThread.setDaemon(true);
    serverThread.start();
    try {
      await().atMost(10, TimeUnit.SECONDS).until(server::isServing);
    } catch (final RuntimeException | Error e) {
      try {
        close();
      } catch (final Exception cleanupFailure) {
        e.addSuppressed(cleanupFailure);
      }
      throw e;
    }
  }

  String getEndpoint() {
    return receiver.getIp() + ":" + socket.getServerSocket().getLocalPort();
  }

  void passThrough() {
    mode = Mode.PASS;
    release.countDown();
    releaseResponseLoss.countDown();
  }

  void allowResponseLoss() {
    releaseResponseLoss.countDown();
  }

  void setMode(final Mode mode) {
    this.mode = mode;
  }

  int getPausedRequests() {
    return pausedRequests.get();
  }

  int getRejectedRequests() {
    return rejectedRequests.get();
  }

  int getLostResponses() {
    return lostResponses.get();
  }

  int getCommittedResponses() {
    return committedResponses.get();
  }

  int getForwardingFailures() {
    return forwardingFailures.get();
  }

  int getSuccessfulDataRequests() {
    return successfulDataRequests.get();
  }

  boolean hasRetriedFailedRequest() {
    return failedRequests.values().stream().anyMatch(attempts -> attempts.get() > 1);
  }

  private void recordFailedRequest(final TPipeTransferReq request) {
    // Serialization may advance body buffers; immutable copies preserve the retry identity.
    failedRequests
        .computeIfAbsent(request.deepCopy(), ignored -> new AtomicInteger())
        .incrementAndGet();
  }

  private void process(final TProtocol input, final TProtocol output) throws TException {
    final TMessage message = input.readMessageBegin();
    if (!"pipeTransfer".equals(message.name)) {
      throw new TException("Only pipeTransfer is supported by this test relay");
    }
    final IClientRPCService.pipeTransfer_args args = new IClientRPCService.pipeTransfer_args();
    args.read(input);
    input.readMessageEnd();
    final TPipeTransferReq request = args.getReq();
    final TPipeTransferReq requestIdentity = request.deepCopy();
    // Pause/reject data only. Handshake and schema requests must still reach the real receiver.
    final boolean dataRequest =
        request.getType() >= PipeRequestType.TRANSFER_TABLET_INSERT_NODE.getType()
            && request.getType() <= PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType();
    final Mode requestMode = mode;
    if (dataRequest && requestMode == Mode.PAUSE_BEFORE_FORWARD) {
      pausedRequests.incrementAndGet();
      try {
        if (!release.await(120, TimeUnit.SECONDS)) {
          throw new TTransportException("Timed out waiting for the test to release a data request");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TTransportException(e);
      }
    }
    if (dataRequest && requestMode == Mode.REJECT_BEFORE_FORWARD) {
      rejectedRequests.incrementAndGet();
      recordFailedRequest(requestIdentity);
      input.getTransport().close();
      throw new TTransportException("Injected failure before forwarding the data request");
    }

    IoTDBSyncClient client = clients.get(input.getTransport());
    final TPipeTransferResp response;
    try {
      if (client == null) {
        client =
            new IoTDBSyncClient(
                new ThriftClientProperty.Builder().setConnectionTimeoutMs(5000).build(),
                receiver.getIp(),
                receiver.getPort(),
                false,
                null,
                null);
        clients.put(input.getTransport(), client);
      }
      response = client.pipeTransfer(request);
    } catch (final TException e) {
      forwardingFailures.incrementAndGet();
      if (client != null) {
        client.invalidate();
        clients.remove(input.getTransport());
      }
      throw e;
    }
    final boolean successfulDataRequest =
        dataRequest
            && response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    if (successfulDataRequest) {
      successfulDataRequests.incrementAndGet();
    }
    if (successfulDataRequest && requestMode == Mode.LOSE_RESPONSE) {
      // Hold the successful response until the test has flushed and reconciled the receiver.
      // Only then lose the response, making the persisted-write/absent-ACK boundary deterministic.
      committedResponses.incrementAndGet();
      try {
        if (!releaseResponseLoss.await(120, TimeUnit.SECONDS)) {
          throw new TTransportException("Timed out waiting to lose a committed response");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TTransportException(e);
      }
      lostResponses.incrementAndGet();
      recordFailedRequest(requestIdentity);
      input.getTransport().close();
      throw new TTransportException("Injected response loss after receiver success");
    }
    final IClientRPCService.pipeTransfer_result result =
        new IClientRPCService.pipeTransfer_result().setSuccess(response);
    output.writeMessageBegin(new TMessage(message.name, TMessageType.REPLY, message.seqid));
    result.write(output);
    output.writeMessageEnd();
    output.getTransport().flush();
  }

  @Override
  public void close() throws Exception {
    passThrough();
    server.stop();
    socket.close();
    for (final TTransport senderTransport : senderTransports) {
      senderTransport.close();
    }
    executor.shutdownNow();
    try {
      serverThread.join(TimeUnit.SECONDS.toMillis(10));
      Assert.assertFalse("Test relay did not stop", serverThread.isAlive());
      Assert.assertTrue(
          "Test relay workers did not stop", executor.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      clients.values().forEach(IoTDBSyncClient::invalidate);
      clients.clear();
    }
  }
}
