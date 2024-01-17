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

package org.apache.iotdb.db.pipe.connector.protocol.legacy;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.legacy.TsFilePipeData;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SYNC_CONNECTOR_VERSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_USER_KEY;

public class IoTDBLegacyPipeConnector implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLegacyPipeConnector.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private String ipAddress;
  private int port;

  private boolean useSSL;
  private String trustStore;
  private String trustStorePwd;

  private String user;
  private String password;

  private String syncConnectorVersion;

  private String pipeName;
  private Long creationTime;

  private IoTDBThriftSyncConnectorClient client;

  private SessionPool sessionPool;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
    Set<TEndPoint> givenNodeUrls = parseNodeUrls(validator.getParameters());

    validator
        .validate(
            args ->
                ((boolean) args[0] && (boolean) args[1])
                    || ((boolean) args[2] && (boolean) args[3]),
            String.format(
                "Either %s:%s or %s:%s must be specified",
                CONNECTOR_IOTDB_IP_KEY,
                CONNECTOR_IOTDB_PORT_KEY,
                SINK_IOTDB_IP_KEY,
                SINK_IOTDB_PORT_KEY),
            parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY),
            parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY),
            parameters.hasAttribute(SINK_IOTDB_IP_KEY),
            parameters.hasAttribute(SINK_IOTDB_PORT_KEY))
        .validate(
            empty -> {
              try {
                // Ensure the sink doesn't point to the legacy receiver on DataNode itself
                return !NodeUrlUtils.containsLocalAddress(
                    givenNodeUrls.stream()
                        .filter(tEndPoint -> tEndPoint.getPort() == ioTDBConfig.getRpcPort())
                        .map(TEndPoint::getIp)
                        .collect(Collectors.toList()));
              } catch (UnknownHostException e) {
                LOGGER.warn("Unknown host when checking pipe sink IP.", e);
                return false;
              }
            },
            String.format(
                "One of the endpoints %s of the receivers is pointing back to the legacy receiver %s on sender itself, or unknown host when checking pipe sink IP.",
                givenNodeUrls,
                new TEndPoint(ioTDBConfig.getRpcAddress(), ioTDBConfig.getRpcPort())))
        .validate(
            args -> !((boolean) args[0]) || ((boolean) args[1] && (boolean) args[2]),
            String.format(
                "When %s is specified to true, %s and %s must be specified",
                SINK_IOTDB_SSL_ENABLE_KEY,
                SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY,
                SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY),
            parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
  }

  private Set<TEndPoint> parseNodeUrls(PipeParameters parameters) {
    final Set<TEndPoint> givenNodeUrls = new HashSet<>();

    if (parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY)
        && parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY)) {
      givenNodeUrls.add(
          new TEndPoint(
              parameters.getStringByKeys(CONNECTOR_IOTDB_IP_KEY),
              parameters.getIntByKeys(CONNECTOR_IOTDB_PORT_KEY)));
    }

    if (parameters.hasAttribute(SINK_IOTDB_IP_KEY)
        && parameters.hasAttribute(SINK_IOTDB_PORT_KEY)) {
      givenNodeUrls.add(
          new TEndPoint(
              parameters.getStringByKeys(SINK_IOTDB_IP_KEY),
              parameters.getIntByKeys(SINK_IOTDB_PORT_KEY)));
    }

    return givenNodeUrls;
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    ipAddress = parameters.getStringByKeys(CONNECTOR_IOTDB_IP_KEY, SINK_IOTDB_IP_KEY);
    port = parameters.getIntByKeys(CONNECTOR_IOTDB_PORT_KEY, SINK_IOTDB_PORT_KEY);

    user =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_USER_KEY, SINK_IOTDB_USER_KEY),
            CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    password =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_PASSWORD_KEY, SINK_IOTDB_PASSWORD_KEY),
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);

    syncConnectorVersion =
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_KEY, SINK_IOTDB_SYNC_CONNECTOR_VERSION_KEY),
            CONNECTOR_IOTDB_SYNC_CONNECTOR_VERSION_DEFAULT_VALUE);

    pipeName = configuration.getRuntimeEnvironment().getPipeName();
    creationTime = configuration.getRuntimeEnvironment().getCreationTime();

    useSSL = parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false);
    trustStore = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY);
    trustStorePwd = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY);
  }

  @Override
  public void handshake() throws Exception {
    close();

    try {
      client =
          new IoTDBThriftSyncConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                  .build(),
              ipAddress,
              port,
              useSSL,
              trustStore,
              trustStorePwd);
      final TSyncIdentityInfo identityInfo =
          new TSyncIdentityInfo(
              pipeName, creationTime, syncConnectorVersion, IoTDBConstant.PATH_ROOT);
      final TSStatus status = client.handshake(identityInfo);
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        String errorMsg =
            String.format(
                "The receiver %s:%s rejected the pipe task because %s",
                ipAddress, port, status.message);
        LOGGER.warn(errorMsg);
        throw new PipeRuntimeCriticalException(errorMsg);
      }
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(PipeConnectionException.CONNECTION_ERROR_FORMATTER, ipAddress, port), e);
    }

    sessionPool =
        new SessionPool.Builder()
            .host(ipAddress)
            .port(port)
            .user(user)
            .password(password)
            .maxSize(1)
            .useSSL(useSSL)
            .trustStore(trustStore)
            .trustStorePwd(trustStorePwd)
            .build();
  }

  @Override
  public void heartbeat() throws Exception {
    // do nothing
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      doTransfer((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      doTransfer((PipeRawTabletInsertionEvent) tabletInsertionEvent);
    } else {
      throw new NotImplementedException(
          "IoTDBLegacyPipeConnector only support "
              + "PipeInsertNodeInsertionEvent and PipeTabletInsertionEvent.");
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      throw new NotImplementedException(
          "IoTDBLegacyPipeConnector only support PipeTsFileInsertionEvent.");
    }
    if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile());
      return;
    }

    try {
      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsFile insertion event: %s.", tsFileInsertionEvent),
          e);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBLegacyPipeConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer(PipeInsertNodeTabletInsertionEvent pipeInsertNodeInsertionEvent)
      throws IoTDBConnectionException, StatementExecutionException {
    final Tablet tablet = pipeInsertNodeInsertionEvent.convertToTablet();
    if (pipeInsertNodeInsertionEvent.isAligned()) {
      sessionPool.insertAlignedTablet(tablet);
    } else {
      sessionPool.insertTablet(tablet);
    }
  }

  private void doTransfer(PipeRawTabletInsertionEvent pipeTabletInsertionEvent)
      throws PipeException, IoTDBConnectionException, StatementExecutionException {
    final Tablet tablet = pipeTabletInsertionEvent.convertToTablet();
    if (pipeTabletInsertionEvent.isAligned()) {
      sessionPool.insertAlignedTablet(tablet);
    } else {
      sessionPool.insertTablet(tablet);
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, IOException {
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    transportSingleFilePieceByPiece(tsFile);
    client.sendPipeData(ByteBuffer.wrap(new TsFilePipeData("", tsFile.getName(), -1).serialize()));
  }

  private void transportSingleFilePieceByPiece(File file) throws IOException {
    // Cut the file into pieces to send
    long position = 0;

    // Try small piece to rebase the file position.
    final byte[] buffer = new byte[PipeConfig.getInstance().getPipeConnectorReadFileBufferSize()];
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      while (true) {
        final int dataLength = randomAccessFile.read(buffer);
        if (dataLength == -1) {
          break;
        }

        final ByteBuffer buffToSend = ByteBuffer.wrap(buffer, 0, dataLength);
        final TSyncTransportMetaInfo metaInfo =
            new TSyncTransportMetaInfo(file.getName(), position);

        final TSStatus status = client.sendFile(metaInfo, buffToSend);

        if ((status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
          // Success
          position += dataLength;
        } else if (status.code == TSStatusCode.SYNC_FILE_REDIRECTION_ERROR.getStatusCode()) {
          position = Long.parseLong(status.message);
          randomAccessFile.seek(position);
          LOGGER.info("Redirect to position {} in transferring tsFile {}.", position, file);
        } else if (status.code == TSStatusCode.SYNC_FILE_ERROR.getStatusCode()) {
          String errorMsg =
              String.format("Network failed to receive tsFile %s, status: %s", file, status);
          LOGGER.warn(errorMsg);
          throw new PipeConnectionException(errorMsg);
        }
      }
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(
              "Cannot send pipe data to receiver %s:%s, because: %s.",
              ipAddress, port, e.getMessage()),
          e);
    }
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }
    if (sessionPool != null) {
      sessionPool.close();
      sessionPool = null;
    }
  }
}
