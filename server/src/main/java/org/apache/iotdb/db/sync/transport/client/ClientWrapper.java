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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWrapper {
  private static final Logger logger = LoggerFactory.getLogger(ClientWrapper.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private TTransport transport = null;
  private volatile IClientRPCService.Client serviceClient = null;

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

  public IClientRPCService.Client getClient() {
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
      serviceClient = new IClientRPCService.Client(protocol);

      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }

      TSyncIdentityInfo identityInfo =
          new TSyncIdentityInfo(
              localIP, pipe.getName(), pipe.getCreateTime(), config.getIoTDBMajorVersion());
      TSStatus status = serviceClient.handshake(identityInfo);
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.error("The receiver rejected the synchronization task because {}", status.message);
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
