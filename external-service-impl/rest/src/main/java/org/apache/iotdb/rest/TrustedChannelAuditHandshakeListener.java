/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rest;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.TrustedChannelFailureHandler;

import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslHandshakeListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

final class TrustedChannelAuditHandshakeListener implements SslHandshakeListener {

  private final TrustedChannelFailureHandler failureHandler;

  TrustedChannelAuditHandshakeListener(TrustedChannelFailureHandler failureHandler) {
    this.failureHandler = Objects.requireNonNull(failureHandler);
  }

  @Override
  public void handshakeFailed(Event event, Throwable failure) {
    recordHandshakeFailure(event.getEndPoint(), failure);
  }

  void recordHandshakeFailure(EndPoint endPoint, Throwable failure) {
    if (endPoint == null || failure == null) {
      return;
    }

    TEndPoint initiator = toEndPoint(endPoint.getRemoteSocketAddress());
    TEndPoint target = toEndPoint(endPoint.getLocalSocketAddress());
    if (initiator == null || target == null) {
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

  private static TEndPoint toEndPoint(SocketAddress socketAddress) {
    if (!(socketAddress instanceof InetSocketAddress inetSocketAddress)) {
      return null;
    }
    String host =
        inetSocketAddress.getAddress() == null
            ? inetSocketAddress.getHostString()
            : inetSocketAddress.getAddress().getHostAddress();
    return new TEndPoint(host, inetSocketAddress.getPort());
  }
}
