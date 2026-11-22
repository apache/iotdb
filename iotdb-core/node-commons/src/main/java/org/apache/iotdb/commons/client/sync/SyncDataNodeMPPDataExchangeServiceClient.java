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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.ServiceMessages;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcSslUtils;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import com.google.common.net.InetAddresses;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.external.commons.lang3.StringUtils;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;

public class SyncDataNodeMPPDataExchangeServiceClient extends MPPDataExchangeService.Client
    implements ThriftClient, AutoCloseable {

  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private final ThriftClientProperty property;
  private final int defaultTimeoutMs;
  private final TEndPoint endpoint;
  private final MPPDataExchangeClientManager clientManager;
  private boolean invalidated;
  private boolean restoreTimeout;

  // The pool allocates the client before connecting so a stalled connect cannot block other
  // borrowers inside Commons Pool's per-key makeObjectCountLock.
  protected SyncDataNodeMPPDataExchangeServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      MPPDataExchangeClientManager clientManager) {
    super(null);
    this.property = property;
    this.defaultTimeoutMs = Math.max(0, property.getConnectionTimeoutMs());
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  final void prepare(long deadlineMs) throws TTransportException, IOException {
    if (iprot_ == null) {
      openProtocol(deadlineMs);
    }
    if (deadlineMs != Long.MAX_VALUE) {
      setTimeout(rpcTimeout(deadlineMs));
    }
  }

  // On success TSocket (or its auto-closing TLS socket) owns the channel until invalidation.
  @SuppressWarnings("java:S2095")
  private void openProtocol(long deadlineMs) throws IOException, TTransportException {
    SocketChannel channel = SocketChannel.open();
    try {
      // A channel-backed socket makes blocking connects, writes and reads interruptible.
      TTransport transport = connect(channel.socket(), deadlineMs);
      try {
        iprot_ = oprot_ = property.getProtocolFactory().getProtocol(transport);
      } catch (RuntimeException | Error e) {
        closeOnFailure(transport, e);
        throw e;
      }
    } catch (IOException | TTransportException | RuntimeException | Error e) {
      // Also close the channel if closing the TLS wrapper failed.
      closeOnFailure(channel, e);
      throw e;
    }
  }

  private static void closeOnFailure(Closeable resource, Throwable failure) {
    try {
      resource.close();
    } catch (IOException | RuntimeException | Error closeFailure) {
      if (closeFailure != failure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  private record Peer(InetSocketAddress address, SSLSocketFactory sslFactory) {}

  private Peer resolvePeer(long deadlineMs) throws TTransportException {
    String host = endpoint.getIp();
    int port = endpoint.getPort();
    boolean ssl = commonConfig.isEnableInternalSSL();
    if (!ssl && InetAddresses.isInetAddress(host)) {
      return new Peer(new InetSocketAddress(InetAddresses.forString(host), port), null);
    }
    return clientManager.prepareConnection(
        () -> new Peer(new InetSocketAddress(host, port), ssl ? sslSocketFactory() : null),
        rpcTimeout(deadlineMs));
  }

  final TTransport connect(Socket socket, long deadlineMs) throws IOException, TTransportException {
    try {
      Peer peer = resolvePeer(deadlineMs);
      socket.connect(peer.address(), rpcTimeout(deadlineMs));
      if (peer.sslFactory() != null) {
        SSLSocket ssl =
            (SSLSocket)
                peer.sslFactory().createSocket(socket, endpoint.getIp(), endpoint.getPort(), true);
        socket = ssl;
        SSLParameters params = ssl.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.setSSLParameters(params);
      }
      socket.setSoTimeout(rpcTimeout(deadlineMs));
      return DeepCopyRpcTransportFactory.INSTANCE.getTransport(new TSocket(socket));
    } catch (IOException | TTransportException | RuntimeException | Error e) {
      closeOnFailure(socket, e);
      throw e;
    }
  }

  private static SSLSocketFactory sslSocketFactory()
      throws GeneralSecurityException, IOException, TTransportException {
    if (StringUtils.isBlank(commonConfig.getKeyStorePath())
        && StringUtils.isBlank(commonConfig.getTrustStorePath())) {
      throw new TTransportException(ServiceMessages.KEYSTORE_OR_TRUSTSTORE_NOT_FOUND);
    }
    return RpcSslUtils.createSSLContext(
            commonConfig.getKeyStorePath(),
            commonConfig.getKeyStorePwd(),
            commonConfig.getTrustStorePath(),
            commonConfig.getTrustStorePwd(),
            commonConfig.getSslProtocol())
        .getSocketFactory();
  }

  private int rpcTimeout(long deadlineMs) {
    if (deadlineMs == Long.MAX_VALUE) {
      return defaultTimeoutMs;
    }
    long remaining = MPPDataExchangeClientManager.remainingMillis(deadlineMs);
    return (int) Math.min(remaining, defaultTimeoutMs > 0 ? defaultTimeoutMs : Integer.MAX_VALUE);
  }

  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public void setTimeout(int timeout) {
    restoreTimeout = timeout != defaultTimeoutMs;
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @Override
  public void close() {
    clientManager.returnClient(endpoint, this);
  }

  @Override
  public void invalidate() {
    if (!invalidated) {
      invalidated = true;
      if (iprot_ != null) {
        iprot_.getTransport().close();
      }
    }
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endpoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  @Override
  public String toString() {
    return String.format("SyncDataNodeMPPDataExchangeServiceClient{%s}", endpoint);
  }

  static class Factory
      extends ThriftClientFactory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

    public Factory(
        ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeMPPDataExchangeServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncDataNodeMPPDataExchangeServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              SyncDataNodeMPPDataExchangeServiceClient.class,
              SyncDataNodeMPPDataExchangeServiceClient.class.getDeclaredConstructor(
                  ThriftClientProperty.class, TEndPoint.class, MPPDataExchangeClientManager.class),
              thriftClientProperty,
              endpoint,
              clientManager));
    }

    @Override
    public void passivateObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeMPPDataExchangeServiceClient> pooledObject) {
      SyncDataNodeMPPDataExchangeServiceClient client = pooledObject.getObject();
      if (client.restoreTimeout) {
        client.setTimeout(client.defaultTimeoutMs);
      }
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeMPPDataExchangeServiceClient> pooledObject) {
      SyncDataNodeMPPDataExchangeServiceClient client = pooledObject.getObject();
      return !client.invalidated
          && (client.iprot_ == null || client.iprot_.getTransport().isOpen());
    }
  }
}
