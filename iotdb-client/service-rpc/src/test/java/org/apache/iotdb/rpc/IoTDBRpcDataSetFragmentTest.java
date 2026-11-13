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

package org.apache.iotdb.rpc;

import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBRpcDataSetFragmentTest {

  private static final String COLUMN = "s1";

  @Test
  public void testAssembleOneOversizedTsBlockAcrossResponses() throws Exception {
    byte[] bytes = serializeInt64Block();
    int totalLength = bytes.length;
    int split = totalLength / 2;

    IClientRPCService.Iface client = mockClient();
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenReturn(
            fragmentResponse(ByteBuffer.wrap(bytes, 0, split), split, totalLength, true),
            fragmentResponse(ByteBuffer.wrap(bytes, split, totalLength - split), -1, -1, false));

    IoTDBRpcDataSet dataSet = newDataSet(client);

    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertFalse(dataSet.next());

    ArgumentCaptor<TSFetchResultsReq> captor = ArgumentCaptor.forClass(TSFetchResultsReq.class);
    verify(client, times(2)).fetchResultsV2(captor.capture());
    assertEquals(0, captor.getAllValues().get(0).getOffset());
    assertEquals(split, captor.getAllValues().get(1).getOffset());
  }

  @Test
  public void testCompleteBlocksAndTrailingFragmentInOneResponse() throws Exception {
    byte[] first = serializeInt64Block();
    byte[] second = serializeInt64Block();
    int split = second.length / 2;

    IClientRPCService.Iface client = mockClient();
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenReturn(
            response(
                List.of(ByteBuffer.wrap(first), ByteBuffer.wrap(second, 0, split)),
                split,
                second.length,
                true),
            fragmentResponse(ByteBuffer.wrap(second, split, second.length - split), -1, -1, false));

    IoTDBRpcDataSet dataSet = newDataSet(client);

    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertFalse(dataSet.next());
  }

  @Test
  public void testLegacyResponseWithoutOffset() throws Exception {
    byte[] bytes = serializeInt64Block();
    IClientRPCService.Iface client = mockClient();
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenReturn(response(List.of(ByteBuffer.wrap(bytes)), -1, -1, false));

    IoTDBRpcDataSet dataSet = newDataSet(client);

    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertFalse(dataSet.next());
  }

  @Test
  public void testBlocksInExecuteResponseAreConsumedBeforeFetching() throws Exception {
    byte[] bytes = serializeInt64Block();
    IClientRPCService.Iface client = mockClient();
    IoTDBRpcDataSet dataSet =
        new IoTDBRpcDataSet(
            "",
            List.of(COLUMN),
            List.of(TSDataType.INT64.name()),
            null,
            true,
            false,
            1L,
            2L,
            client,
            3L,
            List.of(ByteBuffer.wrap(bytes)),
            100,
            0,
            ZoneId.systemDefault(),
            RpcUtils.DEFAULT_TIME_FORMAT,
            1_000_000,
            true,
            new ArrayList<>(List.of(0)));

    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertFalse(dataSet.next());
    verify(client, times(0)).fetchResultsV2(any(TSFetchResultsReq.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectMissingTotalLengthForFirstFragment() throws Exception {
    byte[] bytes = serializeInt64Block();
    IClientRPCService.Iface client = mockClient();
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenReturn(
            fragmentResponse(
                ByteBuffer.wrap(bytes, 0, bytes.length / 2), bytes.length / 2, -1, true));

    IoTDBRpcDataSet dataSet = newDataSet(client);
    dataSet.next();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectNonIncreasingFragmentOffset() throws Exception {
    byte[] bytes = serializeInt64Block();
    int split = bytes.length / 2;
    IClientRPCService.Iface client = mockClient();
    // the offset must equal the accumulated length, otherwise the dataset must refuse to continue
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenReturn(
            fragmentResponse(ByteBuffer.wrap(bytes, 0, split), split - 1, bytes.length, true));

    IoTDBRpcDataSet dataSet = newDataSet(client);
    dataSet.next();
  }

  private static IoTDBRpcDataSet newDataSet(IClientRPCService.Iface client) {
    return new IoTDBRpcDataSet(
        "",
        List.of(COLUMN),
        List.of(TSDataType.INT64.name()),
        null,
        true,
        true,
        1L,
        2L,
        client,
        3L,
        null,
        100,
        0,
        ZoneId.systemDefault(),
        RpcUtils.DEFAULT_TIME_FORMAT,
        1_000_000,
        true,
        new ArrayList<>(List.of(0)));
  }

  private static IClientRPCService.Iface mockClient() throws Exception {
    IClientRPCService.Iface client = mock(IClientRPCService.Iface.class);
    when(client.closeOperation(any(TSCloseOperationReq.class)))
        .thenReturn(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    return client;
  }

  private static TSFetchResultsResp fragmentResponse(
      ByteBuffer fragment, int nextOffset, int totalLength, boolean moreData) {
    return response(Collections.singletonList(fragment), nextOffset, totalLength, moreData);
  }

  private static TSFetchResultsResp response(
      List<ByteBuffer> blocks, int nextOffset, int totalLength, boolean moreData) {
    TSFetchResultsResp resp =
        new TSFetchResultsResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS), true, true);
    resp.setQueryResult(blocks);
    resp.setMoreData(moreData);
    if (nextOffset >= 0) {
      resp.setOffset(nextOffset);
    }
    if (totalLength >= 0) {
      resp.setTotalLength(totalLength);
    }
    return resp;
  }

  /** [time=1, s1=10], [time=2, s1=20] as an INT64 column. */
  private static byte[] serializeInt64Block() throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(10L);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeLong(20L);
    builder.declarePosition();
    TsBlock tsBlock = builder.build();

    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);
    byte[] bytes = new byte[serialized.remaining()];
    serialized.duplicate().get(bytes);
    return bytes;
  }
}
