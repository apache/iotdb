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

package org.apache.iotdb.db.pipe.receiver.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiverAgent;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBAirGapReceiverAgentTest {

  @Test
  public void testExpiredUdpClientSessionIsRemoved() throws Exception {
    final IoTDBAirGapReceiverAgent agent = new IoTDBAirGapReceiverAgent();
    final String sessionKey = "expired-client";
    final IoTDBAirGapReceiverAgent.UdpClientSession session =
        new IoTDBAirGapReceiverAgent.UdpClientSession(new ClientSession(new Socket()), 0L);
    agent.udpClientSessions.put(sessionKey, session);

    final AtomicBoolean receiverExited = new AtomicBoolean(false);
    final IoTDBDataNodeReceiverAgent receiverAgent = PipeDataNodeAgent.receiver().thrift();
    final Field receiverMapField = IoTDBDataNodeReceiverAgent.class.getDeclaredField("receiverMap");
    receiverMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, IoTDBReceiver> receiverMap =
        (Map<String, IoTDBReceiver>) receiverMapField.get(receiverAgent);
    receiverMap.put(
        sessionKey,
        new IoTDBReceiver() {
          @Override
          public TPipeTransferResp receive(final TPipeTransferReq req) {
            return new TPipeTransferResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
          }

          @Override
          public void handleExit() {
            receiverExited.set(true);
          }

          @Override
          public IoTDBSinkRequestVersion getVersion() {
            return IoTDBSinkRequestVersion.VERSION_1;
          }
        });

    try {
      final long timeoutNanos =
          TimeUnit.MILLISECONDS.toNanos(PipeConfig.getInstance().getPipeSinkTransferTimeoutMs());
      agent.removeExpiredUdpClientSessions(timeoutNanos + 1);

      Assert.assertTrue(agent.udpClientSessions.isEmpty());
      Assert.assertTrue(receiverExited.get());
    } finally {
      receiverMap.remove(sessionKey);
    }
  }

  @Test
  public void testMalformedUdpPacketDoesNotCreateClientSession() throws Exception {
    try (final DatagramSocket datagramSocket = new DatagramSocket()) {
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 1L);
      final byte[] malformedPacket = new byte[] {1};
      final DatagramPacket packet =
          new DatagramPacket(
              malformedPacket,
              malformedPacket.length,
              InetAddress.getLoopbackAddress(),
              datagramSocket.getLocalPort());
      final AtomicBoolean sessionRegistrationAttempted = new AtomicBoolean(false);

      Assert.assertThrows(
          IOException.class,
          () ->
              receiver.receiveUdp(
                  datagramSocket,
                  packet,
                  "invalid-client",
                  malformedPacket,
                  () -> sessionRegistrationAttempted.set(true)));

      Assert.assertFalse(sessionRegistrationAttempted.get());
    }
  }
}
