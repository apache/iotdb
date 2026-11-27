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

package org.apache.iotdb.db.pipe.receiver.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapUdpProtocol;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiverAgent;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class IoTDBAirGapReceiverAgentTest {

  @Test(timeout = 150_000)
  public void testUdpSocketIsDrainedWhileRequestHandlingIsBlocked() throws Exception {
    verifyUdpQueue(false);
  }

  @Test(timeout = 150_000)
  public void testStoppingUdpReceiverReleasesQueuedPackets() throws Exception {
    verifyUdpQueue(true);
  }

  private void verifyUdpQueue(final boolean stopWithPendingPackets) throws Exception {
    final CountDownLatch handlingStarted = new CountDownLatch(1);
    final CountDownLatch resumeHandling = new CountDownLatch(1);
    final CountDownLatch handled = new CountDownLatch(17);
    final List<Integer> order = Collections.synchronizedList(new ArrayList<>());
    final IoTDBAirGapReceiverAgent agent =
        new IoTDBAirGapReceiverAgent() {
          @Override
          void handleUdpPacket(final DatagramPacket packet) {
            handlingStarted.countDown();
            try {
              resumeHandling.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            order.add((int) packet.getData()[0]);
            handled.countDown();
          }
        };
    try (final DatagramSocket receiver = new DatagramSocket(0, InetAddress.getLoopbackAddress());
        final DatagramSocket sender = new DatagramSocket()) {
      setAgentField(agent, "datagramSocket", receiver);
      ((AtomicBoolean) getAgentField(agent, "allowSubmitListen")).set(true);
      final ExecutorService executor = (ExecutorService) getAgentField(agent, "listenExecutor");
      @SuppressWarnings("unchecked")
      final BlockingQueue<DatagramPacket> queue =
          (BlockingQueue<DatagramPacket>) getAgentField(agent, "udpRequests");
      final Semaphore bytes = (Semaphore) getAgentField(agent, "udpRequestBytes");
      final int initialPermits = bytes.availablePermits();
      executor.submit(agent::listenUdp);
      executor.submit(agent::processUdpRequests);
      try {
        final byte[] data = new byte[60 * 1024];
        sender.send(new DatagramPacket(data, data.length, receiver.getLocalSocketAddress()));
        Assert.assertTrue(handlingStarted.await(30, TimeUnit.SECONDS));
        final long queueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        for (int i = 1; i <= 16; i++) {
          data[0] = (byte) i;
          sender.send(new DatagramPacket(data, data.length, receiver.getLocalSocketAddress()));
          while (queue.size() < i && System.nanoTime() < queueDeadline) {
            Thread.sleep(1);
          }
          Assert.assertEquals(i, queue.size());
        }
        Assert.assertEquals(initialPermits - 17 * data.length, bytes.availablePermits());
        if (!stopWithPendingPackets) {
          resumeHandling.countDown();
          Assert.assertTrue(handled.await(30, TimeUnit.SECONDS));
          for (int i = 0; i < 17; i++) {
            Assert.assertEquals(i, order.get(i).intValue());
          }
        }
      } finally {
        agent.stop();
        resumeHandling.countDown();
        Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      }
      Assert.assertTrue(queue.isEmpty());
      Assert.assertEquals(initialPermits, bytes.availablePermits());
      Assert.assertTrue(((Map<?, ?>) getAgentField(agent, "udpPendingRequestCounts")).isEmpty());
    }
  }

  private static Object getAgentField(final Object target, final String name) throws Exception {
    final Field field = IoTDBAirGapReceiverAgent.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void setAgentField(final Object target, final String name, final Object value)
      throws Exception {
    final Field field = IoTDBAirGapReceiverAgent.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static long expiredUdpSessionAccessTimeNanos() {
    final long timeoutNanos =
        TimeUnit.MILLISECONDS.toNanos(
            Math.max(1, PipeConfig.getInstance().getPipeSinkTransferTimeoutMs()));
    return System.nanoTime() - timeoutNanos - 1;
  }

  @Test
  public void testExpiredUdpClientSessionIsRemoved() throws Exception {
    final IoTDBAirGapReceiverAgent agent = new IoTDBAirGapReceiverAgent();
    final String sessionKey = "expired-client";
    final IoTDBAirGapReceiverAgent.UdpClientSession session =
        new IoTDBAirGapReceiverAgent.UdpClientSession(
            new ClientSession(new Socket()), expiredUdpSessionAccessTimeNanos());
    agent.udpClientSessions.put(sessionKey, session);

    final AtomicBoolean receiverExited = new AtomicBoolean(false);
    final IoTDBDataNodeReceiverAgent receiverAgent = PipeDataNodeAgent.receiver().thrift();
    final Field receiverMapField = IoTDBDataNodeReceiverAgent.class.getDeclaredField("receiverMap");
    receiverMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IoTDBReceiver> receiverMap =
        (Map<String, IoTDBReceiver>) receiverMapField.get(receiverAgent);
    receiverMap.put(
        sessionKey,
        new IoTDBReceiver() {
          @Override
          public TPipeTransferResp receive(final TPipeTransferReq req) {
            return new TPipeTransferResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
          }

          @Override
          public void handleExit() {
            receiverExited.set(true);
          }

          @Override
          public IoTDBSinkRequestVersion getVersion() {
            return IoTDBSinkRequestVersion.VERSION_1;
          }
        });

    try {
      agent.removeExpiredUdpClientSessions(System.nanoTime());

      Assert.assertTrue(agent.udpClientSessions.isEmpty());
      Assert.assertTrue(receiverExited.get());
    } finally {
      receiverMap.remove(sessionKey);
    }
  }

  @Test
  public void testQueuedUdpRetrySurvivesAnotherClientRegistration() throws Exception {
    final IoTDBAirGapReceiverAgent agent = new IoTDBAirGapReceiverAgent();
    final String queuedReceiverKey = "queued-client-" + UUID.randomUUID();
    final IoTDBAirGapReceiverAgent.UdpClientSession queuedSession =
        new IoTDBAirGapReceiverAgent.UdpClientSession(
            new ClientSession(new Socket()), expiredUdpSessionAccessTimeNanos());
    final byte[] fingerprint = new byte[] {99};
    queuedSession.markCompleted(7L, fingerprint);
    agent.udpClientSessions.put(queuedReceiverKey, queuedSession);
    agent.markUdpRequestPending(queuedReceiverKey);

    try (final DatagramSocket otherClient =
        new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      final boolean[] registeredSession = {false};
      Assert.assertTrue(
          agent.registerUdpSessionIfNecessary(
              new DatagramPacket(
                  new byte[1], 1, InetAddress.getLoopbackAddress(), otherClient.getLocalPort()),
              registeredSession));

      Assert.assertTrue(registeredSession[0]);
      Assert.assertSame(queuedSession, agent.udpClientSessions.get(queuedReceiverKey));
      Assert.assertTrue(queuedSession.isCompleted(7L, fingerprint));
    } finally {
      SessionManager.getInstance().removeCurrSession();
      agent.releaseUdpRequestPending(queuedReceiverKey);
      agent.udpClientSessions.clear();
    }
  }

  @Test
  public void testUdpSessionCapacityRejectsWhenAllSessionsHaveQueuedRequests() throws Exception {
    final IoTDBAirGapReceiverAgent agent = new IoTDBAirGapReceiverAgent();
    final Field capacityField =
        IoTDBAirGapReceiverAgent.class.getDeclaredField("UDP_MAX_CLIENT_SESSIONS");
    capacityField.setAccessible(true);
    final int capacity = capacityField.getInt(null);
    final long nowNanos = System.nanoTime();
    final List<String> receiverKeys = new ArrayList<>(capacity);
    final ClientSession session = new ClientSession(new Socket());

    for (int i = 0; i < capacity; i++) {
      final String receiverKey = "queued-client-" + i;
      receiverKeys.add(receiverKey);
      agent.udpClientSessions.put(
          receiverKey, new IoTDBAirGapReceiverAgent.UdpClientSession(session, nowNanos));
      agent.markUdpRequestPending(receiverKey);
    }

    try (final DatagramSocket otherClient =
        new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      final DatagramPacket packet =
          new DatagramPacket(new byte[1], 1, otherClient.getLocalSocketAddress());
      final boolean[] registeredSession = {false};
      Assert.assertFalse(agent.registerUdpSessionIfNecessary(packet, registeredSession));

      Assert.assertFalse(registeredSession[0]);
      Assert.assertEquals(capacity, agent.udpClientSessions.size());

      final String drainedReceiverKey = receiverKeys.get(0);
      agent.releaseUdpRequestPending(drainedReceiverKey);
      Assert.assertTrue(agent.registerUdpSessionIfNecessary(packet, registeredSession));
      Assert.assertTrue(registeredSession[0]);
      Assert.assertEquals(capacity, agent.udpClientSessions.size());
      Assert.assertFalse(agent.udpClientSessions.containsKey(drainedReceiverKey));
      Assert.assertTrue(agent.udpClientSessions.containsKey(packet.getSocketAddress().toString()));
    } finally {
      SessionManager.getInstance().removeCurrSession();
      receiverKeys.forEach(agent::releaseUdpRequestPending);
      agent.udpClientSessions.clear();
    }
  }

  @Test(timeout = 10_000)
  public void testExpiredUdpClientSessionIsRemovedDuringContinuousInvalidTraffic()
      throws Exception {
    final IoTDBAirGapReceiverAgent agent =
        new IoTDBAirGapReceiverAgent() {
          @Override
          void handleUdpPacket(final DatagramPacket packet) throws IOException {
            try {
              super.handleUdpPacket(packet);
            } finally {
              try {
                Thread.sleep(1_100);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
            }
          }
        };
    final IoTDBDataNodeReceiverAgent receiverAgent = PipeDataNodeAgent.receiver().thrift();
    final Field receiverMapField = IoTDBDataNodeReceiverAgent.class.getDeclaredField("receiverMap");
    receiverMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IoTDBReceiver> receiverMap =
        (Map<String, IoTDBReceiver>) receiverMapField.get(receiverAgent);
    final CountDownLatch receiverExited = new CountDownLatch(1);
    final IoTDBAirGapReceiverAgent.UdpClientSession session =
        new IoTDBAirGapReceiverAgent.UdpClientSession(
            new ClientSession(new Socket()), expiredUdpSessionAccessTimeNanos());

    try (final DatagramSocket datagramSocket = new DatagramSocket();
        final DatagramSocket sender = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      final String sessionKey = "idle-client-" + UUID.randomUUID();
      agent.udpClientSessions.put(sessionKey, session);
      final IoTDBReceiver previousReceiver =
          receiverMap.put(
              sessionKey,
              new IoTDBReceiver() {
                @Override
                public IoTDBSinkRequestVersion getVersion() {
                  return IoTDBSinkRequestVersion.VERSION_1;
                }

                @Override
                public TPipeTransferResp receive(final TPipeTransferReq req) {
                  return new TPipeTransferResp(
                      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
                }

                @Override
                public void handleExit() {
                  receiverExited.countDown();
                }
              });

      try {
        setAgentField(agent, "datagramSocket", datagramSocket);
        ((AtomicBoolean) getAgentField(agent, "allowSubmitListen")).set(true);
        @SuppressWarnings("unchecked")
        final BlockingQueue<DatagramPacket> udpRequests =
            (BlockingQueue<DatagramPacket>) getAgentField(agent, "udpRequests");
        final Semaphore udpRequestBytes = (Semaphore) getAgentField(agent, "udpRequestBytes");
        for (int packetIndex = 0; packetIndex < 2; packetIndex++) {
          final DatagramPacket malformedPacket =
              new DatagramPacket(new byte[] {1}, 1, sender.getLocalSocketAddress());
          udpRequestBytes.acquire(malformedPacket.getLength());
          agent.markUdpRequestPending(malformedPacket.getSocketAddress().toString());
          udpRequests.put(malformedPacket);
        }

        final ExecutorService executor = (ExecutorService) getAgentField(agent, "listenExecutor");
        executor.submit(agent::processUdpRequests);
        try {
          Assert.assertTrue(receiverExited.await(5, TimeUnit.SECONDS));
          Assert.assertFalse(agent.udpClientSessions.containsKey(sessionKey));
        } finally {
          agent.stop();
          Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
      } finally {
        if (previousReceiver == null) {
          receiverMap.remove(sessionKey);
        } else {
          receiverMap.put(sessionKey, previousReceiver);
        }
      }
    }
  }

  @Test
  public void testQueuedUdpRetryRefreshesSessionBeforeExpiration() throws Exception {
    final IoTDBAirGapReceiverAgent agent = new IoTDBAirGapReceiverAgent();
    final IoTDBDataNodeReceiverAgent receiverAgent = PipeDataNodeAgent.receiver().thrift();
    final Field receiverMapField = IoTDBDataNodeReceiverAgent.class.getDeclaredField("receiverMap");
    receiverMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IoTDBReceiver> receiverMap =
        (Map<String, IoTDBReceiver>) receiverMapField.get(receiverAgent);
    final AtomicInteger receiveCount = new AtomicInteger();

    try (final DatagramSocket datagramSocket = new DatagramSocket();
        final DatagramSocket sender = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      setAgentField(agent, "datagramSocket", datagramSocket);
      final long requestId = 7L;
      final byte[] request =
          ByteBuffer.allocate(Byte.BYTES + Short.BYTES + 3)
              .put((byte) 1)
              .putShort((short) 123)
              .put(new byte[] {1, 2, 3})
              .array();
      final byte[] datagramBytes = toUdpDatagram(requestId, request);
      final long timeoutNanos =
          TimeUnit.MILLISECONDS.toNanos(
              Math.max(1, PipeConfig.getInstance().getPipeSinkTransferTimeoutMs()));
      final long expiredAccessTimeNanos = expiredUdpSessionAccessTimeNanos();
      final String receiverKey = sender.getLocalSocketAddress().toString();
      final IoTDBAirGapReceiverAgent.UdpClientSession session =
          new IoTDBAirGapReceiverAgent.UdpClientSession(
              new ClientSession(new Socket()), expiredAccessTimeNanos);
      session.markCompleted(
          requestId,
          IoTDBAirGapReceiver.calculateUdpRequestFingerprint(
              Arrays.copyOfRange(
                  datagramBytes, Long.BYTES + 2 * Integer.BYTES, datagramBytes.length)));
      agent.udpClientSessions.put(receiverKey, session);
      final IoTDBReceiver previousReceiver =
          receiverMap.put(
              receiverKey,
              new IoTDBReceiver() {
                @Override
                public IoTDBSinkRequestVersion getVersion() {
                  return IoTDBSinkRequestVersion.VERSION_1;
                }

                @Override
                public TPipeTransferResp receive(final TPipeTransferReq req) {
                  receiveCount.incrementAndGet();
                  return new TPipeTransferResp(
                      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
                }

                @Override
                public void handleExit() {}
              });

      try {
        agent.handleUdpPacket(
            new DatagramPacket(
                datagramBytes, datagramBytes.length, sender.getLocalSocketAddress()));

        Assert.assertEquals(0, receiveCount.get());
        Assert.assertSame(session, agent.udpClientSessions.get(receiverKey));
        Assert.assertTrue(session.lastAccessTimeNanos > expiredAccessTimeNanos);
      } finally {
        agent.udpClientSessions.remove(receiverKey);
        if (previousReceiver == null) {
          receiverMap.remove(receiverKey);
        } else {
          receiverMap.put(receiverKey, previousReceiver);
        }
      }
    }
  }

  @Test
  public void testCompletedUdpRequestIsDeduplicatedAndFailedRequestCanRetry() throws Exception {
    final IoTDBDataNodeReceiverAgent receiverAgent = PipeDataNodeAgent.receiver().thrift();
    final Field receiverMapField = IoTDBDataNodeReceiverAgent.class.getDeclaredField("receiverMap");
    receiverMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IoTDBReceiver> receiverMap =
        (Map<String, IoTDBReceiver>) receiverMapField.get(receiverAgent);
    final String receiverKey = "udp-duplicate-" + UUID.randomUUID();
    final AtomicInteger receiveCount = new AtomicInteger();
    final IoTDBReceiver previousReceiver =
        receiverMap.put(
            receiverKey,
            new IoTDBReceiver() {
              @Override
              public IoTDBSinkRequestVersion getVersion() {
                return IoTDBSinkRequestVersion.VERSION_1;
              }

              @Override
              public TPipeTransferResp receive(final TPipeTransferReq req) {
                return new TPipeTransferResp(
                    RpcUtils.getStatus(
                        receiveCount.incrementAndGet() == 1
                            ? TSStatusCode.PIPE_VERSION_ERROR
                            : TSStatusCode.SUCCESS_STATUS));
              }

              @Override
              public void handleExit() {}
            });
    final IoTDBAirGapReceiverAgent.UdpClientSession session =
        new IoTDBAirGapReceiverAgent.UdpClientSession(new ClientSession(new Socket()), 0L);

    try (final DatagramSocket datagramSocket = new DatagramSocket()) {
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 1L);
      final byte[] request =
          ByteBuffer.allocate(Byte.BYTES + Short.BYTES + 3)
              .put((byte) 1)
              .putShort((short) 123)
              .put(new byte[] {1, 2, 3})
              .array();
      final byte[] datagramBytes =
          toUdpDatagram(AirGapUdpProtocol.NO_ACK_REQUEST_ID_MASK | 7, request);
      for (int i = 0; i < 3; i++) {
        final DatagramPacket packet =
            new DatagramPacket(
                datagramBytes,
                datagramBytes.length,
                InetAddress.getLoopbackAddress(),
                datagramSocket.getLocalPort());
        receiver.receiveUdpWithAdmission(
            datagramSocket, packet, receiverKey, datagramBytes, () -> true, session);
      }
      datagramSocket.setSoTimeout(100);
      Assert.assertThrows(
          SocketTimeoutException.class,
          () -> datagramSocket.receive(new DatagramPacket(new byte[64], 64)));
    } finally {
      if (previousReceiver == null) {
        receiverMap.remove(receiverKey);
      } else {
        receiverMap.put(receiverKey, previousReceiver);
      }
    }

    Assert.assertEquals(2, receiveCount.get());
  }

  @Test
  public void testCompletedUdpRequestCacheUsesFullFingerprintAndIsBounded() {
    final IoTDBAirGapReceiverAgent.UdpClientSession session =
        new IoTDBAirGapReceiverAgent.UdpClientSession(new ClientSession(new Socket()), 0L);
    final byte[] firstData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
    final byte[] differentDataWithSameLegacyFingerprint = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 10};
    final byte[] firstFingerprint = IoTDBAirGapReceiver.calculateUdpRequestFingerprint(firstData);
    final byte[] differentFingerprint =
        IoTDBAirGapReceiver.calculateUdpRequestFingerprint(differentDataWithSameLegacyFingerprint);
    Assert.assertFalse(Arrays.equals(firstFingerprint, differentFingerprint));
    session.markCompleted(1L, firstFingerprint);
    Assert.assertTrue(session.isCompleted(1L, firstFingerprint));
    Assert.assertFalse(session.isCompleted(1L, differentFingerprint));

    for (long requestId = 2; requestId <= 257; requestId++) {
      session.markCompleted(
          requestId, ByteBuffer.allocate(Long.BYTES).putLong(requestId + 100).array());
    }

    Assert.assertFalse(session.isCompleted(1L, firstFingerprint));
    Assert.assertTrue(
        session.isCompleted(257L, ByteBuffer.allocate(Long.BYTES).putLong(357L).array()));
  }

  @Test
  public void testMalformedUdpPacketDoesNotCreateClientSession() throws Exception {
    try (final DatagramSocket datagramSocket = new DatagramSocket()) {
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 1L);
      final byte[] malformedPacket = new byte[] {1};
      final DatagramPacket packet =
          new DatagramPacket(
              malformedPacket,
              malformedPacket.length,
              InetAddress.getLoopbackAddress(),
              datagramSocket.getLocalPort());
      final AtomicBoolean sessionRegistrationAttempted = new AtomicBoolean(false);

      Assert.assertThrows(
          IOException.class,
          () ->
              receiver.receiveUdpWithAdmission(
                  datagramSocket,
                  packet,
                  "invalid-client",
                  malformedPacket,
                  () -> {
                    sessionRegistrationAttempted.set(true);
                    return true;
                  }));

      Assert.assertFalse(sessionRegistrationAttempted.get());
    }
  }

  private static byte[] toUdpDatagram(final long requestId, final byte[] request) {
    final CRC32 crc32 = new CRC32();
    crc32.update(request);
    final int dataLength = Long.BYTES + request.length;
    return ByteBuffer.allocate(Long.BYTES + 2 * Integer.BYTES + dataLength)
        .putLong(requestId)
        .putInt(dataLength)
        .putInt(dataLength)
        .putLong(crc32.getValue())
        .put(request)
        .array();
  }
}
