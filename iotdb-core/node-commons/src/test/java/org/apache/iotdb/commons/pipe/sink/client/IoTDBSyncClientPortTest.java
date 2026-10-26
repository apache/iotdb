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

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.datastructure.interval.IntervalManager;
import org.apache.iotdb.commons.pipe.datastructure.interval.PlainInterval;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.transport.TSocket;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.net.Socket;

public class IoTDBSyncClientPortTest {

  @Test
  public void testDefaultSendPortsKeepsSyncConnectionOpen() throws Exception {
    try (final ServerSocket receiver = new ServerSocket(0)) {
      final IntervalManager<PlainInterval> candidatePorts = new IntervalManager<>();
      try (final IoTDBSyncClient client =
              new IoTDBSyncClient(
                  new ThriftClientProperty.Builder().setConnectionTimeoutMs(1000).build(),
                  "127.0.0.1",
                  receiver.getLocalPort(),
                  false,
                  null,
                  null,
                  null,
                  null,
                  candidatePorts);
          final Socket accepted = receiver.accept()) {
        Assert.assertTrue(client.getInputProtocol().getTransport().isOpen());
      }
    }
  }

  @Test
  public void testConfiguredSendPortIsUsedForSyncConnection() throws Exception {
    try (final ServerSocket receiver = new ServerSocket(0);
        final ServerSocket sendPortReservation = new ServerSocket(0)) {
      final int sendPort = sendPortReservation.getLocalPort();
      sendPortReservation.close();

      final IntervalManager<PlainInterval> candidatePorts = new IntervalManager<>();
      candidatePorts.addInterval(new PlainInterval(sendPort, sendPort));
      try (final IoTDBSyncClient client =
              new IoTDBSyncClient(
                  new ThriftClientProperty.Builder().setConnectionTimeoutMs(1000).build(),
                  "127.0.0.1",
                  receiver.getLocalPort(),
                  false,
                  null,
                  null,
                  null,
                  null,
                  candidatePorts);
          final Socket accepted = receiver.accept()) {
        final TSocket thriftSocket =
            (TSocket)
                ((TElasticFramedTransport) client.getInputProtocol().getTransport()).getSocket();
        Assert.assertEquals(sendPort, thriftSocket.getSocket().getLocalPort());
      }
    }
  }
}
