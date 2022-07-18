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
package org.apache.iotdb.db.sync.transport.client;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.service.transport.thrift.IdentityInfo;
import org.apache.iotdb.service.transport.thrift.TransportService;
import org.apache.iotdb.service.transport.thrift.TransportStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.sync.transport.conf.TransportConstant.SUCCESS_CODE;

public class ClientWrapper {
  private static final Logger logger = LoggerFactory.getLogger(ClientWrapper.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private TTransport transport = null;
  private volatile TransportService.Client serviceClient = null;

  /* remote IP address*/
  private final String ipAddress;
  /* remote port */
  private final int port;
  /* local IP address*/
  private final String localIP;

  private final Pipe pipe;

  public ClientWrapper(Pipe pipe, String ipAddress, int port, String localIP) {
    this.pipe = pipe;
    this.ipAddress = ipAddress;
    this.port = port;
    this.localIP = localIP;
  }

  public TransportService.Client getClient() {
    return serviceClient;
  }

  /**
   * create connection and handshake before sending messages
   *
   * @return true if success; false if failed to check IoTDB version.
   * @throws SyncConnectionException cannot create connection to receiver
   */
  public boolean handshakeWithVersion() throws SyncConnectionException {
    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              new TSocket(
                  TConfigurationConst.defaultTConfiguration,
                  ipAddress,
                  port,
                  SyncConstant.SOCKET_TIMEOUT_MILLISECONDS,
                  SyncConstant.CONNECT_TIMEOUT_MILLISECONDS));
      TProtocol protocol;
      if (config.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      serviceClient = new TransportService.Client(protocol);

      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }

      IdentityInfo identityInfo =
          new IdentityInfo(
              localIP, pipe.getName(), pipe.getCreateTime(), config.getIoTDBMajorVersion());
      TransportStatus status = serviceClient.handshake(identityInfo);
      if (status.code != SUCCESS_CODE) {
        logger.error("The receiver rejected the synchronization task because {}", status.msg);
        return false;
      }
    } catch (TException e) {
      logger.warn("Cannot connect to the receiver because {}", e.getMessage());
      throw new SyncConnectionException(
          String.format("Cannot connect to the receiver because %s.", e.getMessage()));
    }
    return true;
  }

  public void close() {
    if (transport != null) {
      transport.close();
      transport = null;
    }
  }
}
