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

package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportTsFileRemotely extends AbstractTsFileProcessTool implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportTsFileRemotely.class);
  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String MODS = ".mods";
  private static final String LOAD_STRATEGY = "sync";

  private static final AtomicInteger CONNECTION_TIMEOUT_MS =
      new AtomicInteger(PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());

  private IoTDBSyncClient client;

  private static String host;
  private static String port;

  public ImportTsFileRemotely() {
    initClientAndStatus();
  }

  @Override
  public void run() {
    try {
      sendHandshake();
      loadTsFile();
    } catch (final Exception e) {
      LOGGER.warn("Error occurred during transfer tsfile.", e);
    }
  }

  @Override
  public void loadTsFile() {
    String filePath;
    try {
      while ((filePath = IoTDBTsFileScanAndProcessTool.getFilePath()) != null) {
        final File tsFile = new File(filePath);

        try {
          if (IoTDBTsFileScanAndProcessTool.isContainModsFile(filePath + MODS)) {
            doTransfer(tsFile, new File(filePath + MODS));
          } else {
            doTransfer(tsFile, null);
          }

          processSuccessFile(filePath);
        } catch (final Exception e) {
          processFailFile(filePath, e);
        }
      }
    } catch (final Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    } finally {
      close();
    }
  }

  public void sendHandshake() {
    try {
      final Map<String, String> params = new HashMap<>();
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
          CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, getClusterId());
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
          Boolean.toString(true));
      params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, LOAD_STRATEGY);

      TPipeTransferResp resp =
          client.pipeTransfer(PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params));

      if (resp.getStatus().getCode() == TSStatusCode.PIPE_TYPE_ERROR.getStatusCode()) {
        LOGGER.info(
            "Handshake error with target server ip: {}, port: {}, because: {}. "
                + "Retry to handshake by PipeTransferHandshakeV1Req.",
            client.getIpAddress(),
            client.getPort(),
            resp.getStatus());
        resp =
            client.pipeTransfer(
                PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                    CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      }

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "Handshake error with target server ip: {}, port: {}, because: {}.",
            client.getIpAddress(),
            client.getPort(),
            resp.getStatus());
      } else {
        client.setTimeout(CONNECTION_TIMEOUT_MS.get());
        LOGGER.info(
            "Handshake success. Target server ip: {}, port: {}",
            client.getIpAddress(),
            client.getPort());
      }
    } catch (final Exception e) {
      ioTPrinter.println(
          String.format(
              "Handshake error with target server ip: %s, port: %s, because: %s.",
              client.getIpAddress(), client.getPort(), e.getMessage()));
    }
  }

  public void doTransfer(final File tsFile, final File modFile) throws PipeException, IOException {
    final TPipeTransferResp resp;
    final TPipeTransferReq req;

    if (Objects.nonNull(modFile)) {
      transferFilePieces(modFile, true);
      transferFilePieces(tsFile, true);

      req =
          PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
              modFile.getName(), modFile.length(), tsFile.getName(), tsFile.length());
    } else {
      transferFilePieces(tsFile, false);

      req = PipeTransferTsFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length());
    }

    try {
      resp = client.pipeTransfer(req);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format("Network error when seal file %s, because %s.", tsFile, e.getMessage()), e);
    }

    final TSStatus status = resp.getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      try (IoTDBDataRegionSyncConnector connector = new IoTDBDataRegionSyncConnector()) {
        connector
            .statusHandler()
            .handle(
                status,
                String.format("Seal file %s error, result status %s.", tsFile, status),
                tsFile.getName());
      }
    }

    ioTPrinter.println("Successfully transferred file " + tsFile);
  }

  private void transferFilePieces(final File file, final boolean isMultiFile)
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

        final byte[] payLoad =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        final PipeTransferFilePieceResp resp;
        try {
          final TPipeTransferReq req =
              isMultiFile
                  ? getTransferMultiFilePieceReq(file.getName(), position, payLoad)
                  : getTransferSingleFilePieceReq(file.getName(), position, payLoad);
          resp = PipeTransferFilePieceResp.fromTPipeTransferResp(client.pipeTransfer(req));
        } catch (final Exception e) {
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer file %s, because %s.", file, e.getMessage()),
              e);
        }

        position += readLength;

        final TSStatus status = resp.getStatus();
        if (status.getCode() == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect file position to {}.", position);
          continue;
        }

        if (status.getCode()
            == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
          sendHandshake();
        }
        // Only handle the failed statuses to avoid string format performance overhead
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          try (IoTDBDataRegionSyncConnector connector = new IoTDBDataRegionSyncConnector()) {
            connector
                .statusHandler()
                .handle(
                    status,
                    String.format("Transfer file %s error, result status %s.", file, status),
                    file.getName());
          }
        }
      }
    }
  }

  private PipeTransferFilePieceReq getTransferMultiFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  private PipeTransferFilePieceReq getTransferSingleFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  private void initClientAndStatus() {
    try {
      this.client =
          new IoTDBSyncClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(
                      PipeConfig.getInstance().getPipeConnectorHandshakeTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PipeConfig.getInstance().isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              getEndPoint().getIp(),
              getEndPoint().getPort(),
              false,
              "",
              "");
    } catch (final TTransportException e) {
      ioTPrinter.println("sync client init error because " + e.getMessage());
    }
  }

  private TEndPoint getEndPoint() {
    return new TEndPoint(host, Integer.parseInt(port));
  }

  private String getClusterId() {
    final SecureRandom random = new SecureRandom();
    final byte[] bytes = new byte[32]; // 32 bytes = 256 bits
    random.nextBytes(bytes);
    return UUID.nameUUIDFromBytes(bytes).toString();
  }

  public void close() {
    try {
      this.client.close();
    } catch (final Exception e) {
      ioTPrinter.println("failed to close client because " + e.getMessage());
    }
  }

  public static void setHost(final String host) {
    ImportTsFileRemotely.host = host;
  }

  public static void setPort(final String port) {
    ImportTsFileRemotely.port = port;
  }
}
