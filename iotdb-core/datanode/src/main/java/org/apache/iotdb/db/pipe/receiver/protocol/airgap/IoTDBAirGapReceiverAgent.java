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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IoTDBAirGapReceiverAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiverAgent.class);
  private static final int UDP_PACKET_MAX_SIZE_IN_BYTES = 65_507;
  private static final int UDP_SESSION_CLEANUP_INTERVAL_MS = 60_000;
  private static final int UDP_MAX_CLIENT_SESSIONS = 1_024;
  private static final int LISTENER_THREAD_COUNT = 2;

  private final ExecutorService listenExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          LISTENER_THREAD_COUNT, ThreadName.PIPE_RECEIVER_AIR_GAP_AGENT.getName());
  private final AtomicBoolean allowSubmitListen = new AtomicBoolean(false);

  private ServerSocket serverSocket;
  private DatagramSocket datagramSocket;

  private final AtomicLong receiverId = new AtomicLong(0);
  final Map<String, UdpClientSession> udpClientSessions = new ConcurrentHashMap<>();

  public void listen() {
    try {
      final Socket socket = serverSocket.accept();
      final long airGapReceiverId = receiverId.incrementAndGet();
      final Thread airGapReceiverThread =
          new Thread(new IoTDBAirGapReceiver(socket, airGapReceiverId));
      airGapReceiverThread.setName(
          ThreadName.PIPE_AIR_GAP_RECEIVER.getName() + "-" + airGapReceiverId);
      airGapReceiverThread.start();
    } catch (final IOException e) {
      if (allowSubmitListen.get()) {
        LOGGER.warn(DataNodePipeMessages.UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER, e);
      }
    }

    if (allowSubmitListen.get()) {
      listenExecutor.submit(this::listen);
    }
  }

  public void listenUdp() {
    while (allowSubmitListen.get() && !datagramSocket.isClosed()) {
      removeExpiredUdpClientSessions(System.nanoTime());
      final byte[] buffer = new byte[UDP_PACKET_MAX_SIZE_IN_BYTES];
      final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      try {
        datagramSocket.receive(packet);

        final long airGapReceiverId = receiverId.incrementAndGet();
        final IoTDBAirGapReceiver receiver =
            new IoTDBAirGapReceiver(new Socket(), airGapReceiverId);
        final String receiverKey = packet.getSocketAddress().toString();
        final boolean[] registeredSession = {false};
        try {
          receiver.receiveUdp(
              datagramSocket,
              packet,
              receiverKey,
              buffer,
              () -> registeredSession[0] = registerUdpSessionIfNecessary(packet));
        } finally {
          if (registeredSession[0]) {
            SessionManager.getInstance().removeCurrSession();
          }
        }
      } catch (final SocketTimeoutException e) {
        removeExpiredUdpClientSessions(System.nanoTime());
      } catch (final IOException e) {
        if (allowSubmitListen.get()) {
          LOGGER.warn(DataNodePipeMessages.UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER, e);
        }
      } catch (final Exception e) {
        LOGGER.warn(DataNodePipeMessages.UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER, e);
      }
    }
  }

  boolean registerUdpSessionIfNecessary(final DatagramPacket packet) {
    final String receiverKey = packet.getSocketAddress().toString();
    final long now = System.nanoTime();
    removeExpiredUdpClientSessions(now);

    UdpClientSession clientSession = udpClientSessions.get(receiverKey);
    if (clientSession == null) {
      ensureUdpClientSessionCapacity();
      clientSession =
          new UdpClientSession(
              new ClientSession(new DatagramClientSocket(packet.getAddress(), packet.getPort())),
              now);
      final UdpClientSession existing = udpClientSessions.putIfAbsent(receiverKey, clientSession);
      if (existing != null) {
        clientSession = existing;
      }
    }
    clientSession.lastAccessTimeNanos = now;
    return SessionManager.getInstance().registerSession(clientSession.session);
  }

  private void ensureUdpClientSessionCapacity() {
    if (udpClientSessions.size() < UDP_MAX_CLIENT_SESSIONS) {
      return;
    }

    String leastRecentlyUsedKey = null;
    UdpClientSession leastRecentlyUsedSession = null;
    for (final Map.Entry<String, UdpClientSession> entry : udpClientSessions.entrySet()) {
      if (leastRecentlyUsedSession == null
          || entry.getValue().lastAccessTimeNanos < leastRecentlyUsedSession.lastAccessTimeNanos) {
        leastRecentlyUsedKey = entry.getKey();
        leastRecentlyUsedSession = entry.getValue();
      }
    }
    if (leastRecentlyUsedKey != null) {
      removeUdpClientSession(leastRecentlyUsedKey, leastRecentlyUsedSession);
    }
  }

  void removeExpiredUdpClientSessions(final long nowNanos) {
    final long idleTimeoutNanos =
        TimeUnit.MILLISECONDS.toNanos(
            Math.max(1, PipeConfig.getInstance().getPipeSinkTransferTimeoutMs()));
    udpClientSessions.forEach(
        (key, session) -> {
          if (nowNanos - session.lastAccessTimeNanos >= idleTimeoutNanos) {
            removeUdpClientSession(key, session);
          }
        });
  }

  private void removeUdpClientSession(final String key, final UdpClientSession session) {
    if (!udpClientSessions.remove(key, session)) {
      return;
    }

    final boolean registeredSession = SessionManager.getInstance().registerSession(session.session);
    try {
      PipeDataNodeAgent.receiver().thrift().handleClientExit(key);
    } finally {
      if (registeredSession) {
        SessionManager.getInstance().removeCurrSession();
      }
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      serverSocket = new ServerSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
      datagramSocket = new DatagramSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
      datagramSocket.setSoTimeout(UDP_SESSION_CLEANUP_INTERVAL_MS);
    } catch (final IOException e) {
      if (Objects.nonNull(serverSocket)) {
        try {
          serverSocket.close();
        } catch (final IOException closeException) {
          e.addSuppressed(closeException);
        }
      }
      throw new StartupException(e);
    }

    allowSubmitListen.set(true);
    listenExecutor.submit(this::listen);
    listenExecutor.submit(this::listenUdp);

    LOGGER.info(DataNodePipeMessages.IOTDBAIRGAPRECEIVERAGENT_STARTED, serverSocket);
  }

  @Override
  public void stop() {
    allowSubmitListen.set(false);

    try {
      if (Objects.nonNull(serverSocket)) {
        serverSocket.close();
      }
      if (Objects.nonNull(datagramSocket)) {
        datagramSocket.close();
      }
    } catch (final IOException e) {
      LOGGER.warn(DataNodePipeMessages.FAILED_TO_CLOSE_IOTDBAIRGAPRECEIVERAGENT_S_SERVER_SOCKET, e);
    }

    udpClientSessions.forEach(this::removeUdpClientSession);
    listenExecutor.shutdown();

    LOGGER.info(DataNodePipeMessages.IOTDBAIRGAPRECEIVERAGENT_STOPPED, serverSocket);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.AIR_GAP_SERVICE;
  }

  private static class DatagramClientSocket extends Socket {

    private final InetAddress address;
    private final int port;

    private DatagramClientSocket(final InetAddress address, final int port) {
      this.address = address;
      this.port = port;
    }

    @Override
    public InetAddress getInetAddress() {
      return address;
    }

    @Override
    public int getPort() {
      return port;
    }
  }

  static class UdpClientSession {

    private final ClientSession session;
    volatile long lastAccessTimeNanos;

    UdpClientSession(final ClientSession session, final long lastAccessTimeNanos) {
      this.session = session;
      this.lastAccessTimeNanos = lastAccessTimeNanos;
    }
  }
}
