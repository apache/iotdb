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

package org.apache.iotdb.commons.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.TrustedChannelFailureHandler;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * Performs the server-side TLS handshake before the first Thrift request is processed and reports
 * handshake failures together with the raw socket peer and the local service endpoint.
 */
public class TrustedChannelAuditServerEventHandler implements TServerEventHandler {

  private final TServerEventHandler delegate;
  private final TEndPoint target;
  private final TrustedChannelFailureHandler failureHandler;

  public TrustedChannelAuditServerEventHandler(
      TServerEventHandler delegate, TEndPoint target, TrustedChannelFailureHandler failureHandler) {
    this.delegate = Objects.requireNonNull(delegate);
    this.target = Objects.requireNonNull(target);
    this.failureHandler = Objects.requireNonNull(failureHandler);
  }

  @Override
  public void preServe() {
    delegate.preServe();
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    startHandshakeIfNecessary(output);
    return delegate.createContext(input, output);
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    if (serverContext != null) {
      delegate.deleteContext(serverContext, input, output);
    }
  }

  @Override
  public void processContext(
      ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    delegate.processContext(serverContext, inputTransport, outputTransport);
  }

  private void startHandshakeIfNecessary(TProtocol output) {
    Socket socket = getSocket(output);
    if (!(socket instanceof SSLSocket)) {
      return;
    }

    try {
      ((SSLSocket) socket).startHandshake();
    } catch (IOException e) {
      if (e instanceof SSLException) {
        notifyFailure(e, socket.getRemoteSocketAddress());
      }
      try {
        socket.close();
      } catch (IOException closeFailure) {
        if (closeFailure != e) {
          e.addSuppressed(closeFailure);
        }
      }
      throw new UncheckedIOException(e);
    }
  }

  private void notifyFailure(Throwable failure, SocketAddress remoteAddress) {
    TEndPoint initiator = toEndPoint(remoteAddress);
    if (initiator == null) {
      return;
    }
    try {
      failureHandler.onFailure(failure, initiator, target);
    } catch (RuntimeException auditFailure) {
      if (auditFailure != failure) {
        failure.addSuppressed(auditFailure);
      }
    }
  }

  private static Socket getSocket(TProtocol protocol) {
    if (protocol == null || !(protocol.getTransport() instanceof TElasticFramedTransport)) {
      return null;
    }
    TTransport socketTransport = ((TElasticFramedTransport) protocol.getTransport()).getSocket();
    return socketTransport instanceof TSocket ? ((TSocket) socketTransport).getSocket() : null;
  }

  private static TEndPoint toEndPoint(SocketAddress socketAddress) {
    if (!(socketAddress instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
    String host =
        inetSocketAddress.getAddress() == null
            ? inetSocketAddress.getHostString()
            : inetSocketAddress.getAddress().getHostAddress();
    return new TEndPoint(host, inetSocketAddress.getPort());
  }
}
