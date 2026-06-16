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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IoTDBAirGapReceiverAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiverAgent.class);
  private static final int UDP_PACKET_MAX_SIZE_IN_BYTES = 65_507;

  private final ExecutorService listenExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.PIPE_RECEIVER_AIR_GAP_AGENT.getName());
  private final ExecutorService udpListenExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.PIPE_RECEIVER_AIR_GAP_AGENT.getName() + "-UDP");
  private final AtomicBoolean allowSubmitListen = new AtomicBoolean(false);

  private ServerSocket serverSocket;
  private DatagramSocket datagramSocket;

  private final AtomicLong receiverId = new AtomicLong(0);
  private final Map<String, ClientSession> udpClientSessions = new ConcurrentHashMap<>();

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
      final byte[] buffer = new byte[UDP_PACKET_MAX_SIZE_IN_BYTES];
      final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      try {
        datagramSocket.receive(packet);

        final long airGapReceiverId = receiverId.incrementAndGet();
        final IoTDBAirGapReceiver receiver =
            new IoTDBAirGapReceiver(new Socket(), airGapReceiverId);
        final String receiverKey = packet.getSocketAddress().toString();
        final boolean registeredSession = registerUdpSessionIfNecessary(packet);
        try {
          receiver.receiveUdp(datagramSocket, packet, receiverKey, buffer);
        } finally {
          if (registeredSession) {
            SessionManager.getInstance().removeCurrSession();
          }
        }
      } catch (final IOException e) {
        if (allowSubmitListen.get()) {
          LOGGER.warn(DataNodePipeMessages.UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER, e);
        }
      } catch (final Exception e) {
        LOGGER.warn(DataNodePipeMessages.UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER, e);
      }
    }
  }

  private boolean registerUdpSessionIfNecessary(final DatagramPacket packet) {
    final String receiverKey = packet.getSocketAddress().toString();
    final ClientSession session =
        udpClientSessions.computeIfAbsent(
            receiverKey,
            key ->
                new ClientSession(new DatagramClientSocket(packet.getAddress(), packet.getPort())));
    return SessionManager.getInstance().registerSession(session);
  }

  @Override
  public void start() throws StartupException {
    try {
      serverSocket = new ServerSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
      datagramSocket = new DatagramSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
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
    udpListenExecutor.submit(this::listenUdp);

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

    udpClientSessions.forEach(
        (key, session) -> {
          final boolean registeredSession = SessionManager.getInstance().registerSession(session);
          try {
            PipeDataNodeAgent.receiver().thrift().handleClientExit(key);
          } finally {
            if (registeredSession) {
              SessionManager.getInstance().removeCurrSession();
            }
          }
        });
    udpClientSessions.clear();
    listenExecutor.shutdown();
    udpListenExecutor.shutdown();

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
}
