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
import org.junit.Test;
import org.mockito.InOrder;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;

import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrustedChannelAuditServerEventHandlerTest {

  @Test
  public void testHandshakeBeforeCreatingDelegateContext() throws Exception {
    TServerEventHandler delegate = mock(TServerEventHandler.class);
    ServerContext context = mock(ServerContext.class);
    TProtocol input = mock(TProtocol.class);
    SSLSocket socket = mock(SSLSocket.class);
    TProtocol output = createProtocol(socket);
    TEndPoint target = new TEndPoint("10.0.0.2", 10730);
    when(delegate.createContext(input, output)).thenReturn(context);

    TrustedChannelAuditServerEventHandler handler =
        new TrustedChannelAuditServerEventHandler(
            delegate, target, TrustedChannelFailureHandler.NO_OP);

    assertSame(context, handler.createContext(input, output));
    InOrder inOrder = inOrder(socket, delegate);
    inOrder.verify(socket).startHandshake();
    inOrder.verify(delegate).createContext(input, output);
  }

  @Test
  public void testHandshakeFailureReportsPeerAndSkipsDelegate() throws Exception {
    TServerEventHandler delegate = mock(TServerEventHandler.class);
    TProtocol input = mock(TProtocol.class);
    SSLSocket socket = mock(SSLSocket.class);
    TProtocol output = createProtocol(socket);
    SSLHandshakeException failure = new SSLHandshakeException("handshake failure");
    InetSocketAddress remoteAddress = new InetSocketAddress("192.0.2.10", 45123);
    TEndPoint target = new TEndPoint("10.0.0.2", 10730);
    AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> reportedInitiator = new AtomicReference<>();
    AtomicReference<TEndPoint> reportedTarget = new AtomicReference<>();
    when(socket.getRemoteSocketAddress()).thenReturn(remoteAddress);
    org.mockito.Mockito.doThrow(failure).when(socket).startHandshake();

    TrustedChannelAuditServerEventHandler handler =
        new TrustedChannelAuditServerEventHandler(
            delegate,
            target,
            (throwable, initiator, endpoint) -> {
              reportedFailure.set(throwable);
              reportedInitiator.set(initiator);
              reportedTarget.set(endpoint);
            });

    UncheckedIOException thrown =
        assertThrows(UncheckedIOException.class, () -> handler.createContext(input, output));

    assertSame(failure, thrown.getCause());
    assertSame(failure, reportedFailure.get());
    assertEquals(new TEndPoint("192.0.2.10", 45123), reportedInitiator.get());
    assertSame(target, reportedTarget.get());
    verify(socket).close();
    verify(delegate, never()).createContext(any(), any());

    handler.deleteContext(null, input, output);
    verify(delegate, never()).deleteContext(isNull(), any(), any());
  }

  private static TProtocol createProtocol(SSLSocket socket) {
    TProtocol protocol = mock(TProtocol.class);
    TElasticFramedTransport framedTransport = mock(TElasticFramedTransport.class);
    TSocket socketTransport = mock(TSocket.class);
    when(protocol.getTransport()).thenReturn(framedTransport);
    when(framedTransport.getSocket()).thenReturn(socketTransport);
    when(socketTransport.getSocket()).thenReturn(socket);
    return protocol;
  }
}
