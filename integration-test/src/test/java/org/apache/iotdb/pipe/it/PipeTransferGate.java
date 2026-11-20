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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.awaitility.Awaitility;
import org.junit.Assert;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;

/**
 * A test-only, framed binary Thrift relay. Each downstream connection owns one upstream connection,
 * so handshakes, receiver identity and cleanup RPCs retain their real server-side lifecycle. No
 * responses are fabricated and no credentials or payloads are retained in diagnostics.
 */
public final class PipeTransferGate implements AutoCloseable {
  private final DataNodeWrapper receiver;
  private final int port;
  private final ServerSocket listener;
  private final Object lifecycleLock = new Object();
  private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  private final ExecutorService workers = Executors.newCachedThreadPool();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final AtomicReference<Hold> hold = new AtomicReference<>();
  private final AtomicInteger completedDataRequests = new AtomicInteger();
  private final AtomicInteger completedPieces = new AtomicInteger();
  private final AtomicInteger completedSeals = new AtomicInteger();
  private final AtomicInteger rejectedRequests = new AtomicInteger();
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, AtomicInteger>>
      rejectedStatusCodes = new ConcurrentHashMap<>();
  private volatile boolean closed;

  public PipeTransferGate(final DataNodeWrapper receiver) throws IOException {
    this.receiver = receiver;
    port = EnvUtils.searchAvailablePorts()[0];
    ServerSocket opened = null;
    try {
      opened = new ServerSocket();
      opened.bind(new InetSocketAddress(receiver.getIp(), port));
      listener = opened;
      workers.execute(
          () -> {
            while (!closed) {
              try {
                final Socket downstream = listener.accept();
                synchronized (lifecycleLock) {
                  if (closed) {
                    downstream.close();
                    return;
                  }
                  sockets.add(downstream);
                  try {
                    workers.execute(() -> relay(downstream));
                  } catch (final RuntimeException e) {
                    sockets.remove(downstream);
                    downstream.close();
                    throw e;
                  }
                }
              } catch (final IOException | RuntimeException e) {
                if (!closed) {
                  failure.compareAndSet(null, e);
                }
                return;
              }
            }
          });
    } catch (final IOException | RuntimeException | Error e) {
      workers.shutdownNow();
      try {
        if (opened != null) {
          opened.close();
        }
      } catch (final IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      try {
        Files.deleteIfExists(Paths.get(EnvUtils.getLockFilePath(port)));
      } catch (final IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  public int getPort() {
    return port;
  }

  public Hold holdNext(final IntPredicate requestType, final boolean afterResponse) {
    final Hold next = new Hold(requestType, afterResponse);
    synchronized (lifecycleLock) {
      Assert.assertFalse("The relay is closed", closed);
      Assert.assertTrue("An earlier gate is still armed", hold.compareAndSet(null, next));
    }
    return next;
  }

  public static boolean isTablet(final int type) {
    return type == PipeRequestType.TRANSFER_TABLET_INSERT_NODE.getType()
        || type == PipeRequestType.TRANSFER_TABLET_RAW.getType()
        || type == PipeRequestType.TRANSFER_TABLET_BINARY.getType()
        || type == PipeRequestType.TRANSFER_TABLET_BATCH.getType()
        || type == PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType()
        || type == PipeRequestType.TRANSFER_TABLET_RAW_V2.getType()
        || type == PipeRequestType.TRANSFER_TABLET_BINARY_V2.getType()
        || type == PipeRequestType.TRANSFER_TABLET_BATCH_V2.getType();
  }

  public static boolean isSeal(final int type) {
    return type == PipeRequestType.TRANSFER_TS_FILE_SEAL.getType()
        || type == PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD.getType();
  }

  public int completedDataRequests() {
    assertHealthy();
    return completedDataRequests.get();
  }

  public int completedPieces() {
    assertHealthy();
    return completedPieces.get();
  }

  public int rejectedRequests() {
    assertHealthy();
    return rejectedRequests.get();
  }

  public int rejectedRequests(final int statusCode) {
    assertHealthy();
    final ConcurrentHashMap<Integer, AtomicInteger> counts = rejectedStatusCodes.get(statusCode);
    return counts == null ? 0 : counts.values().stream().mapToInt(AtomicInteger::get).sum();
  }

  public int rejectedRequests(final IntPredicate requestType, final int statusCode) {
    assertHealthy();
    final ConcurrentHashMap<Integer, AtomicInteger> counts = rejectedStatusCodes.get(statusCode);
    return counts == null
        ? 0
        : counts.entrySet().stream()
            .filter(entry -> requestType.test(entry.getKey()))
            .mapToInt(entry -> entry.getValue().get())
            .sum();
  }

  public int completedSeals() {
    assertHealthy();
    return completedSeals.get();
  }

  public int openConnections() {
    assertHealthy();
    return sockets.size();
  }

  public void assertHealthy() {
    final Throwable error = failure.get();
    if (error != null) {
      throw new AssertionError("Thrift relay failed", error);
    }
  }

  private void relay(final Socket downstream) {
    final Socket upstream = new Socket();
    try (Socket client = downstream;
        upstream) {
      synchronized (lifecycleLock) {
        if (closed) {
          return;
        }
        sockets.add(upstream);
      }
      upstream.connect(new InetSocketAddress(receiver.getIp(), receiver.getPort()), 5000);
      upstream.setSoTimeout(120000);
      final DataInputStream requests = new DataInputStream(client.getInputStream());
      final DataOutputStream forwards = new DataOutputStream(upstream.getOutputStream());
      final DataInputStream responses = new DataInputStream(upstream.getInputStream());
      final DataOutputStream replies = new DataOutputStream(client.getOutputStream());
      while (!closed) {
        final byte[] request = readFrame(requests);
        final TBinaryProtocol requestProtocol =
            new TBinaryProtocol(new TMemoryInputTransport(request));
        Assert.assertEquals("pipeTransfer", requestProtocol.readMessageBegin().name);
        final IClientRPCService.pipeTransfer_args args = new IClientRPCService.pipeTransfer_args();
        args.read(requestProtocol);
        final int type = args.getReq().getType();
        final Hold candidate = hold.get();
        final Hold selected =
            candidate != null && candidate.requestType.test(type) ? candidate : null;
        if (selected != null && !selected.afterResponse) {
          if (!selected.block(client, upstream)) {
            return;
          }
        }
        writeFrame(forwards, request);
        final byte[] response = readFrame(responses);
        final TBinaryProtocol responseProtocol =
            new TBinaryProtocol(new TMemoryInputTransport(response));
        responseProtocol.readMessageBegin();
        final IClientRPCService.pipeTransfer_result result =
            new IClientRPCService.pipeTransfer_result();
        result.read(responseProtocol);
        final int code = result.getSuccess().getStatus().getCode();
        if (code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          if (isTablet(type) || isSeal(type)) {
            completedDataRequests.incrementAndGet();
          }
          if (isSeal(type)) {
            completedSeals.incrementAndGet();
          }
          if (type == PipeRequestType.TRANSFER_TS_FILE_PIECE.getType()
              || type == PipeRequestType.TRANSFER_TS_FILE_PIECE_WITH_MOD.getType()) {
            completedPieces.incrementAndGet();
          }
        } else {
          rejectedRequests.incrementAndGet();
          recordRejectedStatus(type, result.getSuccess().getStatus());
        }
        if (selected != null && selected.afterResponse) {
          Assert.assertEquals(
              "Only a successful receiver response establishes the lost-ACK boundary",
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              code);
          if (!selected.block(client, upstream)) {
            return;
          }
        }
        writeFrame(replies, response);
      }
    } catch (final EOFException | SocketException e) {
      // STOP/DROP, the held disconnect and node restarts deliberately close these connections.
    } catch (final Throwable e) {
      if (!closed) {
        failure.compareAndSet(null, e);
      }
    } finally {
      sockets.remove(upstream);
      sockets.remove(downstream);
    }
  }

  private void recordRejectedStatus(final int requestType, final TSStatus status) {
    rejectedStatusCodes
        .computeIfAbsent(status.getCode(), ignored -> new ConcurrentHashMap<>())
        .computeIfAbsent(requestType, ignored -> new AtomicInteger())
        .incrementAndGet();
    if (status.isSetSubStatus()) {
      status.getSubStatus().forEach(subStatus -> recordRejectedStatus(requestType, subStatus));
    }
  }

  private static byte[] readFrame(final DataInputStream input) throws IOException {
    final int size = input.readInt();
    Assert.assertTrue("Invalid Thrift frame size: " + size, size > 0 && size <= 64 * 1024 * 1024);
    final byte[] frame = new byte[size];
    input.readFully(frame);
    return frame;
  }

  private static void writeFrame(final DataOutputStream output, final byte[] frame)
      throws IOException {
    output.writeInt(frame.length);
    output.write(frame);
    output.flush();
  }

  @Override
  public void close() throws Exception {
    final ArrayList<Socket> toClose;
    synchronized (lifecycleLock) {
      if (closed) {
        return;
      }
      closed = true;
      toClose = new ArrayList<>(sockets);
    }
    IOException closeFailure = null;
    try {
      listener.close();
    } catch (final IOException e) {
      closeFailure = e;
    }
    for (final Socket socket : toClose) {
      try {
        socket.close();
      } catch (final IOException e) {
        if (closeFailure == null) {
          closeFailure = e;
        } else {
          closeFailure.addSuppressed(e);
        }
      }
    }
    try {
      workers.shutdownNow();
      Assert.assertTrue(
          "Relay workers did not stop", workers.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      Files.deleteIfExists(Paths.get(EnvUtils.getLockFilePath(port)));
    }
    if (closeFailure != null) {
      throw closeFailure;
    }
    assertHealthy();
  }

  public final class Hold implements AutoCloseable {
    private final IntPredicate requestType;
    private final boolean afterResponse;
    private final CountDownLatch reached = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private final Set<Socket> heldSockets = ConcurrentHashMap.newKeySet();
    private boolean finished;
    private boolean disconnected;

    private Hold(final IntPredicate requestType, final boolean afterResponse) {
      this.requestType = requestType;
      this.afterResponse = afterResponse;
    }

    private boolean block(final Socket downstream, final Socket upstream) throws Exception {
      synchronized (this) {
        if (finished) {
          return !disconnected;
        }
        heldSockets.add(downstream);
        heldSockets.add(upstream);
        reached.countDown();
      }
      Assert.assertTrue("Held request was never released", released.await(120, TimeUnit.SECONDS));
      synchronized (this) {
        return !disconnected;
      }
    }

    public void awaitReached() {
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertHealthy();
                Assert.assertEquals("Expected RPC boundary was not reached", 0, reached.getCount());
              });
    }

    /** Discard the request/response at the selected boundary while keeping the relay available. */
    public synchronized void disconnect() throws IOException {
      Assert.assertEquals(0, reached.getCount());
      if (disconnected) {
        return;
      }
      Assert.assertFalse("The held request was already released", finished);
      finished = true;
      disconnected = true;
      hold.compareAndSet(this, null);
      IOException closeFailure = null;
      try {
        for (final Socket socket : heldSockets) {
          try {
            socket.close();
          } catch (final IOException e) {
            if (closeFailure == null) {
              closeFailure = e;
            } else {
              closeFailure.addSuppressed(e);
            }
          }
        }
      } finally {
        released.countDown();
      }
      if (closeFailure != null) {
        throw closeFailure;
      }
    }

    @Override
    public synchronized void close() {
      if (finished) {
        return;
      }
      finished = true;
      hold.compareAndSet(this, null);
      released.countDown();
    }
  }
}
