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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class PipeTransferTabletInsertNodeEventHandlerTest {

  @Test
  public void testUpdateLeaderCacheFromMultiDeviceRedirectStatus() {
    final PipeInsertNodeTabletInsertionEvent event =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(event.getDeviceId()).thenReturn(null);
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final PipeTransferTabletInsertNodeEventHandler handler =
        new PipeTransferTabletInsertNodeEventHandler(event, null, sink);

    final TEndPoint firstEndPoint = new TEndPoint("127.0.0.2", 6667);
    final TEndPoint secondEndPoint = new TEndPoint("127.0.0.3", 6667);
    handler.updateLeaderCache(
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
            .setSubStatus(
                Arrays.asList(
                    redirectStatus("root.sg.device1", firstEndPoint),
                    RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                    redirectStatus("root.sg.device2", secondEndPoint))));

    Mockito.verify(sink).updateLeaderCache("root.sg.device1", firstEndPoint);
    Mockito.verify(sink).updateLeaderCache("root.sg.device2", secondEndPoint);
    Mockito.verifyNoMoreInteractions(sink);
  }

  @Test
  public void testUpdateLeaderCacheFromSingleDeviceRedirectStatus() {
    final PipeInsertNodeTabletInsertionEvent event =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(event.getDeviceId()).thenReturn("root.sg.device");
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final PipeTransferTabletInsertNodeEventHandler handler =
        new PipeTransferTabletInsertNodeEventHandler(event, null, sink);
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.4", 6667);

    handler.updateLeaderCache(
        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS).setRedirectNode(redirectEndPoint));

    Mockito.verify(sink).updateLeaderCache("root.sg.device", redirectEndPoint);
  }

  @Test
  public void testOnCompleteUpdatesMultiDeviceLeaderCache() {
    final PipeInsertNodeTabletInsertionEvent event =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(event.getDeviceId()).thenReturn(null);
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final PipeTransferTabletInsertNodeEventHandler handler =
        new PipeTransferTabletInsertNodeEventHandler(event, null, sink);
    final TEndPoint redirectEndPoint = new TEndPoint("127.0.0.5", 6667);

    handler.onCompleteInternal(
        new TPipeTransferResp(
            RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)
                .setSubStatus(
                    Arrays.asList(
                        RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
                        redirectStatus("root.sg.device3", redirectEndPoint)))));

    Mockito.verify(sink).updateLeaderCache("root.sg.device3", redirectEndPoint);
    Mockito.verify(event)
        .decreaseReferenceCount(PipeTransferTabletInsertionEventHandler.class.getName(), true);
  }

  private static TSStatus redirectStatus(final String deviceId, final TEndPoint endPoint) {
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
        .setMessage(deviceId)
        .setRedirectNode(endPoint);
  }
}
