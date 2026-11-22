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

import com.google.common.io.Closer;
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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;

public class SyncDataNodeMPPDataExchangeServiceClient extends MPPDataExchangeService.Client
    implements ThriftClient, AutoCloseable {

  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private final ThriftClientProperty property;
  private final int defaultTimeoutMs;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager;
  private boolean invalidated;
  private boolean restoreTimeout;

  public SyncDataNodeMPPDataExchangeServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager)
      throws TTransportException {
    this(property, endpoint, clientManager, true);
  }

  // Deadline-aware borrowers open new connections after Commons Pool hands out the client.
  protected SyncDataNodeMPPDataExchangeServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager,
      boolean openImmediately)
      throws TTransportException {
    super(null);
    this.property = property;
    this.defaultTimeoutMs = Math.max(0, property.getConnectionTimeoutMs());
    this.endpoint = endpoint;
    this.clientManager = clientManager;
    if (openImmediately) {
      try {
        prepare(Long.MAX_VALUE);
      } catch (IOException e) {
        throw new TTransportException(e);
      }
    }
  }

  final void prepare(long deadlineMs) throws TTransportException, IOException {
    if (iprot_ == null) {
      SocketChannel channel = SocketChannel.open();
      // TSocket owns the channel once its protocol has been installed.
      try (Closeable rollback = closeUnlessConnected(channel)) {
        connect(channel.socket(), deadlineMs);
      }
    }
    if (deadlineMs != Long.MAX_VALUE) {
      setTimeout(rpcTimeout(deadlineMs));
    }
  }

  private Closeable closeUnlessConnected(Closeable resources) {
    return () -> {
      if (iprot_ == null) {
        resources.close();
      }
    };
  }

  final void connect(Socket socket, long deadlineMs) throws IOException, TTransportException {
    Closer resources = Closer.create();
    try (Closeable rollback = closeUnlessConnected(resources)) {
      resources.register(socket);
      checkConnectionSetupActive(deadlineMs);
      String host = endpoint.getIp();
      int port = endpoint.getPort();
      // JVM hostname lookup runs on the caller and can outlast the deadline. Recheck the budget
      // before starting TCP I/O. IP literals take this direct path with or without TLS.
      InetSocketAddress address =
          InetAddresses.isInetAddress(host)
              ? new InetSocketAddress(InetAddresses.forString(host), port)
              : new InetSocketAddress(host, port);
      SSLSocketFactory sslFactory = null;
      if (commonConfig.isEnableInternalSSL()) {
        checkConnectionSetupActive(deadlineMs);
        sslFactory = sslSocketFactory();
      }
      socket.connect(address, rpcTimeout(deadlineMs));
      if (sslFactory != null) {
        socket.setSoTimeout(rpcTimeout(deadlineMs));
        SSLSocket ssl =
            resources.register((SSLSocket) sslFactory.createSocket(socket, host, port, true));
        // Match TSSLTransportFactory.createClient in libthrift 0.25, used by other internal
        // clients.
        SSLParameters params = ssl.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.setSSLParameters(params);
        socket = ssl;
      }
      socket.setSoTimeout(rpcTimeout(deadlineMs));
      TTransport transport =
          resources.register(
              DeepCopyRpcTransportFactory.INSTANCE.getTransport(new TSocket(socket)));
      iprot_ = oprot_ = property.getProtocolFactory().getProtocol(transport);
    }
  }

  private static void checkConnectionSetupActive(long deadlineMs)
      throws ClosedByInterruptException {
    MPPDataExchangeClientManager.remainingMillis(deadlineMs);
    if (Thread.currentThread().isInterrupted()) {
      throw new ClosedByInterruptException();
    }
  }

  private record StoreVersion(
      String path, String password, FileTime modified, long size, Object fileKey) {
    static StoreVersion read(String path, String password) throws IOException {
      if (StringUtils.isBlank(path)) {
        return new StoreVersion(null, null, null, 0L, null);
      }
      BasicFileAttributes attributes =
          Files.readAttributes(Path.of(path), BasicFileAttributes.class);
      return new StoreVersion(
          path, password, attributes.lastModifiedTime(), attributes.size(), attributes.fileKey());
    }

    @Override
    public String toString() {
      return String.valueOf(path);
    }
  }

  private record TlsSettings(String protocol, StoreVersion keys, StoreVersion trust) {
    static TlsSettings read() throws IOException, TTransportException {
      StoreVersion keys =
          StoreVersion.read(commonConfig.getKeyStorePath(), commonConfig.getKeyStorePwd());
      StoreVersion trust =
          StoreVersion.read(commonConfig.getTrustStorePath(), commonConfig.getTrustStorePwd());
      if (keys.path() == null && trust.path() == null) {
        throw new TTransportException(ServiceMessages.KEYSTORE_OR_TRUSTSTORE_NOT_FOUND);
      }
      return new TlsSettings(
          RpcSslUtils.resolveProtocol(commonConfig.getSslProtocol()), keys, trust);
    }

    SSLSocketFactory load() throws IOException, TTransportException {
      try {
        return RpcSslUtils.createSSLContext(
                keys.path(), keys.password(), trust.path(), trust.password(), protocol)
            .getSocketFactory();
      } catch (GeneralSecurityException e) {
        throw new TTransportException(e);
      }
    }
  }

  private record CachedSslFactory(TlsSettings settings, SSLSocketFactory factory) {}

  private static final AtomicReference<CachedSslFactory> cachedSslFactory = new AtomicReference<>();

  static SSLSocketFactory sslSocketFactory() throws IOException, TTransportException {
    CachedSslFactory cached = cachedSslFactory.get();
    if (cached != null && cached.settings().equals(TlsSettings.read())) {
      return cached.factory();
    }
    return reloadSslSocketFactory();
  }

  private static synchronized SSLSocketFactory reloadSslSocketFactory()
      throws IOException, TTransportException {
    // Re-read after acquiring the lock in case another borrower observed a certificate rotation.
    TlsSettings current = TlsSettings.read();
    CachedSslFactory cached = cachedSslFactory.get();
    if (cached == null || !cached.settings().equals(current)) {
      cached = new CachedSslFactory(current, current.load());
      cachedSslFactory.set(cached);
    }
    return cached.factory();
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

  public static class Factory
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
                  ThriftClientProperty.class, TEndPoint.class, ClientManager.class, boolean.class),
              thriftClientProperty,
              endpoint,
              clientManager,
              !(clientManager instanceof MPPDataExchangeClientManager)));
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
