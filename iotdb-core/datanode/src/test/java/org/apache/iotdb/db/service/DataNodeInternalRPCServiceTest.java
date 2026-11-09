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

package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataNodeInternalRPCServiceTest {

  @Test
  public void testMultiplexedClientAndDefaultInternalRoutes() throws Exception {
    final IClientRPCService.Iface clientHandler = mock(IClientRPCService.Iface.class);
    final IDataNodeRPCService.Iface controlHandler = mock(IDataNodeRPCService.Iface.class);
    final TSOpenSessionResp openSessionResp =
        new TSOpenSessionResp(
                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
                TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
            .setSessionId(10);
    when(clientHandler.openSession(any())).thenReturn(openSessionResp);
    when(controlHandler.testConnectionEmptyRPC())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    final TMultiplexedProcessor processor = new TMultiplexedProcessor();
    processor.registerProcessor(
        RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE,
        new IClientRPCService.Processor<>(clientHandler));
    processor.registerDefault(new IDataNodeRPCService.Processor<>(controlHandler));

    final TMemoryBuffer clientRequest = new TMemoryBuffer(1024);
    final TMemoryBuffer clientResponse = new TMemoryBuffer(1024);
    final IClientRPCService.Client client =
        new IClientRPCService.Client(
            new TMultiplexedProtocol(
                new TBinaryProtocol(clientResponse),
                RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE),
            new TMultiplexedProtocol(
                new TBinaryProtocol(clientRequest),
                RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE));
    client.send_openSession(
        new TSOpenSessionReq()
            .setClient_protocol(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3)
            .setUsername("root")
            .setZoneId("UTC"));

    processor.process(new TBinaryProtocol(clientRequest), new TBinaryProtocol(clientResponse));

    assertEquals(10, client.recv_openSession().getSessionId());
    verify(clientHandler).openSession(any());
    verify(controlHandler, never()).testConnectionEmptyRPC();

    final TMemoryBuffer controlRequest = new TMemoryBuffer(1024);
    final TMemoryBuffer controlResponse = new TMemoryBuffer(1024);
    final IDataNodeRPCService.Client controlClient =
        new IDataNodeRPCService.Client(
            new TBinaryProtocol(controlResponse), new TBinaryProtocol(controlRequest));
    controlClient.send_testConnectionEmptyRPC();

    processor.process(new TBinaryProtocol(controlRequest), new TBinaryProtocol(controlResponse));

    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        controlClient.recv_testConnectionEmptyRPC().getCode());
    verify(controlHandler).testConnectionEmptyRPC();
  }
}
