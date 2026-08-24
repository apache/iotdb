/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.rest;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import org.eclipse.jetty.io.EndPoint;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TrustedChannelAuditHandshakeListenerTest {

  @Test
  public void testRecordSocketEndpointsOnHandshakeFailure() {
    AtomicReference<Throwable> actualFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> actualInitiator = new AtomicReference<>();
    AtomicReference<TEndPoint> actualTarget = new AtomicReference<>();
    TrustedChannelAuditHandshakeListener listener =
        new TrustedChannelAuditHandshakeListener(
            (failure, initiator, target) -> {
              actualFailure.set(failure);
              actualInitiator.set(initiator);
              actualTarget.set(target);
            });
    SSLHandshakeException failure = new SSLHandshakeException("test");
    EndPoint endPoint =
        newEndPoint(
            new InetSocketAddress("192.0.2.10", 45123), new InetSocketAddress("192.0.2.20", 18080));

    listener.recordHandshakeFailure(endPoint, failure);

    assertSame(failure, actualFailure.get());
    assertEquals(new TEndPoint("192.0.2.10", 45123), actualInitiator.get());
    assertEquals(new TEndPoint("192.0.2.20", 18080), actualTarget.get());
  }

  @Test
  public void testReportingFailureDoesNotSelfSuppressHandshakeFailure() {
    RuntimeException failure = new RuntimeException("test");
    TrustedChannelAuditHandshakeListener listener =
        new TrustedChannelAuditHandshakeListener(
            (ignoredFailure, initiator, target) -> {
              throw failure;
            });
    EndPoint endPoint =
        newEndPoint(
            new InetSocketAddress("192.0.2.10", 45123), new InetSocketAddress("192.0.2.20", 18080));

    listener.recordHandshakeFailure(endPoint, failure);

    assertEquals(0, failure.getSuppressed().length);
  }

  private static EndPoint newEndPoint(
      InetSocketAddress remoteAddress, InetSocketAddress localAddress) {
    return (EndPoint)
        Proxy.newProxyInstance(
            EndPoint.class.getClassLoader(),
            new Class<?>[] {EndPoint.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getRemoteAddress":
                case "getRemoteSocketAddress":
                  return remoteAddress;
                case "getLocalAddress":
                case "getLocalSocketAddress":
                  return localAddress;
                default:
                  return null;
              }
            });
  }
}
