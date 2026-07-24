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

package org.apache.iotdb.commons.pipe.sink.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV1Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class IoTDBSyncClientManagerTest {

  @Test
  public void sslConnectionFailureShouldBeReportedWithoutReplacingPipeFailure() {
    final TEndPoint target = new TEndPoint("127.0.0.1", 6667);
    final TestIoTDBSyncClientManager clientManager = new TestIoTDBSyncClientManager(target);

    clientManager.reconstruct(target);

    assertSame(target, clientManager.reportedTarget);
    assertTrue(clientManager.reportedFailure.getCause() instanceof SSLHandshakeException);
    assertEquals(1, clientManager.reportedFailure.getSuppressed().length);
    assertSame(clientManager.reportingFailure, clientManager.reportedFailure.getSuppressed()[0]);
  }

  private static class TestIoTDBSyncClientManager extends IoTDBSyncClientManager {

    private final RuntimeException reportingFailure = new RuntimeException("reporting failed");
    private Exception reportedFailure;
    private TEndPoint reportedTarget;

    private TestIoTDBSyncClientManager(final TEndPoint target) {
      super(
          Collections.singletonList(target),
          true,
          "trust-store",
          "trust-store-password",
          null,
          null,
          false,
          "round-robin",
          new UserEntity(0, "system", "127.0.0.1"),
          "password",
          false,
          "sync",
          false,
          true,
          false);
    }

    private void reconstruct(final TEndPoint target) {
      reconstructClient(target);
    }

    @Override
    protected IoTDBSyncClient createClient(final TEndPoint endPoint) throws TTransportException {
      throw new TTransportException(new SSLHandshakeException("handshake failed"));
    }

    @Override
    protected void onClientConnectionFailure(
        final Exception failure, final TEndPoint targetEndPoint) {
      reportedFailure = failure;
      reportedTarget = targetEndPoint;
      throw reportingFailure;
    }

    @Override
    protected PipeTransferHandshakeV1Req buildHandshakeV1Req() throws IOException {
      return null;
    }

    @Override
    protected PipeTransferHandshakeV2Req buildHandshakeV2Req(final Map<String, String> params)
        throws IOException {
      return null;
    }

    @Override
    protected String getClusterId() {
      return "test";
    }
  }
}
