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

package org.apache.iotdb.commons.pipe.connector.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;
import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;

public abstract class IoTDBAirGapConnector extends IoTDBConnector {

  protected static class AirGapSocket extends Socket {

    private final TEndPoint endPoint;

    public AirGapSocket(String ip, int port) {
      this.endPoint = new TEndPoint(ip, port);
    }

    public TEndPoint getEndPoint() {
      return endPoint;
    }

    @Override
    public String toString() {
      return "AirGapSocket{" + "endPoint=" + endPoint + "} (" + super.toString() + ")";
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapConnector.class);

  protected static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  protected final List<AirGapSocket> sockets = new ArrayList<>();
  protected final List<Boolean> isSocketAlive = new ArrayList<>();

  private LoadBalancer loadBalancer;
  private long currentClientIndex = 0;

  private int handshakeTimeoutMs;

  private boolean eLanguageEnable;

  // The air gap connector does not use clientManager thus we put handshake type here
  protected boolean supportModsIfIsDataNodeReceiver = true;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    if (isTabletBatchModeEnabled) {
      LOGGER.warn(
          "Batch mode is enabled by the given parameters. "
              + "IoTDBAirGapConnector does not support batch mode. "
              + "Disable batch mode.");
    }

    for (int i = 0; i < nodeUrls.size(); i++) {
      isSocketAlive.add(false);
      sockets.add(null);
    }

    switch (loadBalanceStrategy) {
      case CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY:
        loadBalancer = new RoundRobinLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY:
        loadBalancer = new RandomLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY:
        loadBalancer = new PriorityLoadBalancer();
        break;
      default:
        LOGGER.warn(
            "Unknown load balance strategy: {}, use round-robin strategy instead.",
            loadBalanceStrategy);
        loadBalancer = new RoundRobinLoadBalancer();
    }

    handshakeTimeoutMs =
        parameters.getIntOrDefault(
            Arrays.asList(
                CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY, SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY),
            CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE);
    LOGGER.info(
        "IoTDBAirGapConnector is customized with handshakeTimeoutMs: {}.", handshakeTimeoutMs);

    eLanguageEnable =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY, SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY),
            CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE);
    LOGGER.info("IoTDBAirGapConnector is customized with eLanguageEnable: {}.", eLanguageEnable);
  }

  @Override
  @SuppressWarnings("java:S2095")
  public void handshake() throws Exception {
    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // Close the socket if necessary
      if (sockets.get(i) != null) {
        try {
          sockets.set(i, null).close();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to close socket with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      final AirGapSocket socket = new AirGapSocket(ip, port);

      try {
        socket.connect(new InetSocketAddress(ip, port), handshakeTimeoutMs);
        socket.setKeepAlive(true);
        sockets.set(i, socket);
        LOGGER.info("Successfully connected to target server ip: {}, port: {}.", ip, port);
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to connect to target server ip: {}, port: {}, because: {}. Ignore it.",
            ip,
            port,
            e.getMessage());
        continue;
      }

      sendHandshakeReq(socket);
      isSocketAlive.set(i, true);
    }

    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", nodeUrls));
  }

  protected void sendHandshakeReq(final AirGapSocket socket) throws IOException {
    socket.setSoTimeout(handshakeTimeoutMs);
    // Try to handshake by PipeTransferHandshakeV2Req. If failed, retry to handshake by
    // PipeTransferHandshakeV1Req. If failed again, throw PipeConnectionException.
    if (!send(socket, generateHandShakeV2Payload())) {
      supportModsIfIsDataNodeReceiver = false;
      if (!send(socket, generateHandShakeV1Payload())) {
        throw new PipeConnectionException("Handshake error with target server, socket: " + socket);
      }
    } else {
      supportModsIfIsDataNodeReceiver = true;
    }
    socket.setSoTimeout(PIPE_CONFIG.getPipeConnectorTransferTimeoutMs());
    LOGGER.info("Handshake success. Socket: {}", socket);
  }

  protected abstract byte[] generateHandShakeV1Payload() throws IOException;

  protected abstract byte[] generateHandShakeV2Payload() throws IOException;

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
    }
  }

  protected void transferFilePieces(
      final String pipeName,
      final long creationTime,
      final File file,
      final AirGapSocket socket,
      final boolean isMultiFile)
      throws PipeException, IOException {
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] payload =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        if (!send(
            pipeName,
            creationTime,
            socket,
            isMultiFile
                ? getTransferMultiFilePieceBytes(file.getName(), position, payload)
                : getTransferSingleFilePieceBytes(file.getName(), position, payload))) {
          final String errorMessage =
              String.format("Transfer file %s error. Socket %s.", file, socket);
          if (mayNeedHandshakeWhenFail()) {
            // Send handshake because we don't know whether the receiver side configNode
            // has set up a new one
            sendHandshakeReq(socket);
          }
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              file.toString());
        } else {
          position += readLength;
        }
      }
    }
  }

  protected abstract boolean mayNeedHandshakeWhenFail();

  protected abstract byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException;

  protected abstract byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException;

  protected int nextSocketIndex() {
    return loadBalancer.nextSocketIndex();
  }

  protected boolean send(
      final String pipeName, final long creationTime, final AirGapSocket socket, byte[] bytes)
      throws IOException {
    if (!socket.isConnected()) {
      return false;
    }

    bytes = compressIfNeeded(bytes);

    rateLimitIfNeeded(pipeName, creationTime, socket.getEndPoint(), bytes.length);

    final BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
    bytes = enrichWithLengthAndChecksum(bytes);
    outputStream.write(eLanguageEnable ? enrichWithELanguage(bytes) : bytes);
    outputStream.flush();

    final byte[] response = new byte[1];
    final int size = socket.getInputStream().read(response);
    return size > 0 && Arrays.equals(AirGapOneByteResponse.OK, response);
  }

  protected boolean send(final AirGapSocket socket, final byte[] bytes) throws IOException {
    return send(null, 0, socket, bytes);
  }

  private byte[] enrichWithLengthAndChecksum(final byte[] bytes) {
    // Length of checksum and bytes payload
    final byte[] length = BytesUtils.intToBytes(bytes.length + LONG_LEN);

    final CRC32 crc32 = new CRC32();
    crc32.update(bytes, 0, bytes.length);

    // Double length as simple checksum
    return BytesUtils.concatByteArrayList(
        Arrays.asList(length, length, BytesUtils.longToBytes(crc32.getValue()), bytes));
  }

  private byte[] enrichWithELanguage(final byte[] bytes) {
    return BytesUtils.concatByteArrayList(
        Arrays.asList(
            AirGapELanguageConstant.E_LANGUAGE_PREFIX,
            bytes,
            AirGapELanguageConstant.E_LANGUAGE_SUFFIX));
  }

  @Override
  public void close() {
    for (int i = 0; i < sockets.size(); ++i) {
      try {
        if (sockets.get(i) != null) {
          sockets.set(i, null).close();
        }
      } catch (final Exception e) {
        LOGGER.warn("Failed to close client {}.", i, e);
      } finally {
        isSocketAlive.set(i, false);
      }
    }

    super.close();
  }

  /////////////////////// Strategies for load balance //////////////////////////

  private interface LoadBalancer {
    int nextSocketIndex();
  }

  private class RoundRobinLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      final int socketSize = sockets.size();
      // Round-robin, find the next alive client
      for (int tryCount = 0; tryCount < socketSize; ++tryCount) {
        final int clientIndex = (int) (currentClientIndex++ % socketSize);
        if (Boolean.TRUE.equals(isSocketAlive.get(clientIndex))) {
          return clientIndex;
        }
      }

      throw new PipeConnectionException(
          "All sockets are dead, please check the connection to the receiver.");
    }
  }

  private class RandomLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      final int socketSize = sockets.size();
      final int clientIndex = (int) (Math.random() * socketSize);
      if (Boolean.TRUE.equals(isSocketAlive.get(clientIndex))) {
        return clientIndex;
      }

      // Random, find the next alive client
      for (int tryCount = 0; tryCount < socketSize - 1; ++tryCount) {
        final int nextClientIndex = (clientIndex + tryCount + 1) % socketSize;
        if (Boolean.TRUE.equals(isSocketAlive.get(nextClientIndex))) {
          return nextClientIndex;
        }
      }

      throw new PipeConnectionException(
          "All sockets are dead, please check the connection to the receiver.");
    }
  }

  private class PriorityLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      // Priority, find the first alive client
      final int socketSize = sockets.size();
      for (int i = 0; i < socketSize; ++i) {
        if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
          return i;
        }
      }

      throw new PipeConnectionException(
          "All sockets are dead, please check the connection to the receiver.");
    }
  }
}
