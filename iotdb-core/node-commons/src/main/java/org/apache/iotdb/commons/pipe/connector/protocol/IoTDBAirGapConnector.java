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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
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
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;
import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;

public abstract class IoTDBAirGapConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapConnector.class);

  protected static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  protected final List<Socket> sockets = new ArrayList<>();
  protected final List<Boolean> isSocketAlive = new ArrayList<>();

  private int handshakeTimeoutMs;
  private boolean eLanguageEnable;

  private long currentClientIndex = 0;

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
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
  public void handshake() throws Exception {
    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // close the socket if necessary
      if (sockets.get(i) != null) {
        try {
          sockets.set(i, null).close();
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close socket with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      final Socket socket = new Socket();

      try {
        socket.connect(new InetSocketAddress(ip, port), handshakeTimeoutMs);
        socket.setKeepAlive(true);
        socket.setSoTimeout(handshakeTimeoutMs);
        sockets.set(i, socket);
        LOGGER.info("Successfully connected to target server ip: {}, port: {}.", ip, port);
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to connect to target server ip: {}, port: {}, because: {}. Ignore it.",
            ip,
            port,
            e.getMessage());
        continue;
      }

      // Try to handshake by PipeTransferHandshakeV2Req. If failed, retry to handshake by
      // PipeTransferHandshakeV1Req. If failed again, throw PipeConnectionException.
      if (!send(socket, generateHandShakeV2Payload())
          && !send(socket, generateHandShakeV1Payload())) {
        throw new PipeConnectionException(
            "Handshake error with target server ip: " + ip + ", port: " + port);
      } else {
        isSocketAlive.set(i, true);
        socket.setSoTimeout((int) PIPE_CONFIG.getPipeConnectorTransferTimeoutMs());
        LOGGER.info("Handshake success. Target server ip: {}, port: {}", ip, port);
      }
    }

    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", nodeUrls));
  }

  protected abstract byte[] generateHandShakeV1Payload() throws IOException;

  protected abstract byte[] generateHandShakeV2Payload() throws IOException;

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
    }
  }

  protected int nextSocketIndex() {
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

  protected boolean send(Socket socket, byte[] bytes) throws IOException {
    if (!socket.isConnected()) {
      return false;
    }

    final BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
    bytes = enrichWithLengthAndChecksum(bytes);
    outputStream.write(eLanguageEnable ? enrichWithELanguage(bytes) : bytes);
    outputStream.flush();

    final byte[] response = new byte[1];
    final int size = socket.getInputStream().read(response);
    return size > 0 && Arrays.equals(AirGapOneByteResponse.OK, response);
  }

  private byte[] enrichWithLengthAndChecksum(byte[] bytes) {
    // length of checksum and bytes payload
    final byte[] length = BytesUtils.intToBytes(bytes.length + LONG_LEN);

    final CRC32 crc32 = new CRC32();
    crc32.update(bytes, 0, bytes.length);

    // double length as simple checksum
    return BytesUtils.concatByteArrayList(
        Arrays.asList(length, length, BytesUtils.longToBytes(crc32.getValue()), bytes));
  }

  private byte[] enrichWithELanguage(byte[] bytes) {
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
      } catch (Exception e) {
        LOGGER.warn("Failed to close client {}.", i, e);
      } finally {
        isSocketAlive.set(i, false);
      }
    }
  }
}
