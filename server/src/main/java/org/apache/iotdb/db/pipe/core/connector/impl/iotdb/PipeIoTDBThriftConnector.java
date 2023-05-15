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

package org.apache.iotdb.db.pipe.core.connector.impl.iotdb;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.core.receiver.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.core.receiver.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.core.receiver.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.core.receiver.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.core.receiver.request.PipeValidateHandshakeReq;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.dml.deletion.DeletionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class PipeIoTDBThriftConnector implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeIoTDBThriftConnector.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final String PIPE_VERSION = PipeConfig.getInstance().getPipeVersion();
  private static final String IP_ARGUMENT_NAME = "ip";
  private static final String PORT_ARGUMENT_NAME = "port";

  private String ipAddress;
  private int port;
  private PipeClientRPCServiceClient client;

  public PipeIoTDBThriftConnector() {}

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator
        .validateRequiredAttribute(IP_ARGUMENT_NAME)
        .validateRequiredAttribute(PORT_ARGUMENT_NAME);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.ipAddress = parameters.getString(IP_ARGUMENT_NAME);
    this.port = parameters.getInt(PORT_ARGUMENT_NAME);
  }

  @Override
  public void handshake() throws Exception {
    if (client != null) {
      client.close();
    }

    client =
        new PipeClientRPCServiceClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                .build(),
            ipAddress,
            port);
    TPipeHandshakeResp resp =
        client.pipeHandshake(
            new PipeValidateHandshakeReq(
                    PIPE_VERSION, CONFIG.getIoTDBMajorVersion(), CONFIG.getTimestampPrecision())
                .toTPipeHandshakeReq());
    if (!resp.status.equals(RpcUtils.SUCCESS_STATUS)) {
      throw new PipeException(String.format("Handshake error, result status %s.", resp.status));
    }
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    TPipeTransferResp resp =
        client.pipeTransfer(
            new PipeTransferInsertNodeReq(
                    PIPE_VERSION, ((PipeTabletInsertionEvent) tabletInsertionEvent).getInsertNode())
                .toTPipeTransferReq());
    if (!resp.status.equals(RpcUtils.SUCCESS_STATUS)) {
      throw new PipeException(
          String.format(
              "Transfer tablet insertion event %s error, result status %s",
              tabletInsertionEvent, resp.status));
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    File tsFile = ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile();
    byte[] readBuffer = new byte[PipeConfig.getInstance().getReadFileBufferSize()];

    long position = 0;
    try (RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        PipeTransferFilePieceResp resp =
            PipeTransferFilePieceResp.fromTPipeTransferResp(
                client.pipeTransfer(
                    new PipeTransferFilePieceReq(
                            PIPE_VERSION,
                            ByteBuffer.wrap(readBuffer, 0, readLength),
                            tsFile.getName(),
                            position)
                        .toTPipeTransferReq()));
        position += readLength;

        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_REDIRECTION.getStatusCode()) {
          position = resp.getEndOffset();
          reader.seek(position);
          LOGGER.info(String.format("Redirect file position to %s.", position));
        } else if (!resp.getStatus().equals(RpcUtils.SUCCESS_STATUS)) {
          throw new PipeException(
              String.format("Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
        }
      }
    }

    TPipeTransferResp resp =
        client.pipeTransfer(
            new PipeTransferFileSealReq(PIPE_VERSION, tsFile.getName(), tsFile.length())
                .toTPipeTransferReq());
    if (!resp.getStatus().equals(RpcUtils.SUCCESS_STATUS)) {
      throw new PipeException(
          String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()));
    }
  }

  @Override
  public void transfer(DeletionEvent deletionEvent) throws Exception {
    throw new NotImplementedException("Not implement for deletion event.");
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
