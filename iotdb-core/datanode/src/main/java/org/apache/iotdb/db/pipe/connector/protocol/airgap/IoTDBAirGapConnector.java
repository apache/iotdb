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

package org.apache.iotdb.db.pipe.connector.protocol.airgap;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.protocol.IoTDBConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.utils.BytesUtils;

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

import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;

public class IoTDBAirGapConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapConnector.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final List<Socket> sockets = new ArrayList<>();
  private final List<Boolean> isSocketAlive = new ArrayList<>();

  private int handshakeTimeoutMs;

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
            CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY,
            CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE);
    LOGGER.info(
        "IoTDBAirGapConnector is customized with handshakeTimeoutMs: {}.", handshakeTimeoutMs);
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

      if (!send(
          socket,
          PipeTransferHandshakeReq.toTransferHandshakeBytes(
              CommonDescriptor.getInstance().getConfig().getTimestampPrecision()))) {
        throw new PipeException("Handshake error with target server ip: " + ip + ", port: " + port);
      } else {
        isSocketAlive.set(i, true);
        socket.setSoTimeout((int) PIPE_CONFIG.getPipeConnectorTimeoutMs());
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

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBAirGapConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tabletInsertionEvent).shouldParsePatternOrTime()) {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        transfer(
            ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).parseEventWithPattern());
      } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
        transfer(((PipeRawTabletInsertionEvent) tabletInsertionEvent).parseEventWithPattern());
      }
      return;
    }

    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransfer(socket, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else {
        doTransfer(socket, (PipeRawTabletInsertionEvent) tabletInsertionEvent);
      }
    } catch (IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBAirGapConnector only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tsFileInsertionEvent).shouldParsePatternOrTime()) {
      for (final TabletInsertionEvent event : tsFileInsertionEvent.toTabletInsertionEvents()) {
        transfer(event);
      }
      return;
    }

    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    try {
      doTransfer(socket, (PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) {
    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn("IoTDBAirGapConnector does not support transfer generic event: {}.", event);
    }
  }

  private void doTransfer(
      Socket socket, PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    final byte[] bytes =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible() == null
            ? PipeTransferTabletBinaryReq.toTransferInsertNodeBytes(
                pipeInsertNodeTabletInsertionEvent.getByteBuffer())
            : PipeTransferTabletInsertNodeReq.toTransferInsertNodeBytes(
                pipeInsertNodeTabletInsertionEvent.getInsertNode());

    if (!send(socket, bytes)) {
      throw new PipeException(
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error. Socket: %s",
              pipeInsertNodeTabletInsertionEvent, socket));
    }
  }

  private void doTransfer(Socket socket, PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    if (!send(
        socket,
        PipeTransferTabletRawReq.toTPipeTransferTabletBytes(
            pipeRawTabletInsertionEvent.convertToTablet(),
            pipeRawTabletInsertionEvent.isAligned()))) {
      throw new PipeException(
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error. Socket: %s.",
              pipeRawTabletInsertionEvent, socket));
    }
  }

  private void doTransfer(Socket socket, PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, InterruptedException, IOException {
    pipeTsFileInsertionEvent.waitForTsFileClose();

    final File tsFile = pipeTsFileInsertionEvent.getTsFile();

    // 1. Transfer file piece by piece
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        if (!send(
            socket,
            PipeTransferFilePieceReq.toTPipeTransferBytes(
                tsFile.getName(),
                position,
                readLength == readFileBufferSize
                    ? readBuffer
                    : Arrays.copyOfRange(readBuffer, 0, readLength)))) {
          throw new PipeException(
              String.format("Transfer file %s error. Socket %s.", tsFile, socket));
        } else {
          position += readLength;
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    if (!send(
        socket,
        PipeTransferFileSealReq.toTPipeTransferFileSealBytes(tsFile.getName(), tsFile.length()))) {
      throw new PipeException(String.format("Seal file %s error. Socket %s.", tsFile, socket));
    } else {
      LOGGER.info("Successfully transferred file {}.", tsFile);
    }
  }

  private int nextSocketIndex() {
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

  private boolean send(Socket socket, byte[] bytes) throws IOException {
    if (!socket.isConnected()) {
      return false;
    }

    final BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
    outputStream.write(enrichWithLengthAndChecksum(bytes));
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
