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

package org.apache.iotdb.commons.client.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.datastructure.interval.IntervalManager;
import org.apache.iotdb.commons.pipe.datastructure.interval.PlainInterval;

import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SocketChannel;

import static org.mockito.Mockito.mock;

public class AsyncPipeDataTransferServiceClientTest {

  @Test
  public void testConfiguredSendPortBindsWithoutConnectingDuringConstruction() throws Exception {
    try (final ServerSocket serverSocket = new ServerSocket(0)) {
      final IntervalManager<PlainInterval> candidatePorts = new IntervalManager<>();
      candidatePorts.addInterval(new PlainInterval(0, 0));
      final TAsyncClientManager thriftClientManager = new TAsyncClientManager();
      AsyncPipeDataTransferServiceClient client = null;
      try {
        client =
            new AsyncPipeDataTransferServiceClient(
                new ThriftClientProperty.Builder().setConnectionTimeoutMs(1000).build(),
                new TEndPoint("127.0.0.1", serverSocket.getLocalPort()),
                thriftClientManager,
                mock(ClientManager.class),
                candidatePorts);

        final Field transportField = TAsyncClient.class.getDeclaredField("___transport");
        transportField.setAccessible(true);
        final TNonblockingSocket nonblockingSocket =
            (TNonblockingSocket) transportField.get(client);
        final SocketChannel channel = nonblockingSocket.getSocketChannel();
        Assert.assertFalse(channel.isConnected());
        Assert.assertTrue(((InetSocketAddress) channel.getLocalAddress()).getPort() > 0);
      } finally {
        if (client != null) {
          client.close();
        }
        thriftClientManager.stop();
      }
    }
  }
}
