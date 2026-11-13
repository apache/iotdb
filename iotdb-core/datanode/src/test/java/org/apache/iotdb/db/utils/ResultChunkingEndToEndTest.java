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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.PendingResultFragment;
import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end verification of the fragmented query result protocol: the server side produces
 * bounded-payload chunks via {@link QueryDataSetUtils} and the client side ({@code
 * IoTDBRpcDataSet}) assembles them back into the original rows.
 */
public class ResultChunkingEndToEndTest {

  private static final String COLUMN = "s1";
  private static final String TEXT_COLUMN = "s2";
  private static final int LARGE_VALUE_SIZE = 512 * 1024;
  private static final int MAX_PAYLOAD_SIZE = 256 * 1024;

  @Test(timeout = 60_000)
  public void testFragmentedResultRoundTrip() throws Exception {
    TsBlock tsBlock = buildTsBlockWithLargeValue();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);
    int totalLength = serialized.remaining();
    assertTrue(
        "the block must be larger than the payload to trigger fragmentation",
        totalLength > MAX_PAYLOAD_SIZE);

    ServerSideSimulation server =
        new ServerSideSimulation(serialized.duplicate(), tsBlock.getPositionCount(), totalLength);

    IClientRPCService.Iface client = mock(IClientRPCService.Iface.class);
    when(client.closeOperation(any(TSCloseOperationReq.class)))
        .thenReturn(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenAnswer(invocation -> server.handleFetchResults(invocation.getArgument(0)));

    IoTDBRpcDataSet dataSet =
        new IoTDBRpcDataSet(
            "",
            List.of(COLUMN, TEXT_COLUMN),
            List.of(TSDataType.INT64.name(), TSDataType.TEXT.name()),
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
            new ArrayList<>(Arrays.asList(0, 1)));

    // two rows: [time=1, v=10, text="small"] and [time=2, v=20, text=<512KB>]
    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertEquals("small", dataSet.getString(TEXT_COLUMN));
    assertTrue(dataSet.next());
    assertEquals(20L, dataSet.getLong(COLUMN));
    assertEquals(LARGE_VALUE_SIZE, dataSet.getString(TEXT_COLUMN).length());
    assertFalse(dataSet.next());

    assertTrue(
        "the result must have been transferred in several responses", server.rpcCount.get() > 1);
    for (int payload : server.payloadSizes) {
      assertTrue(
          String.format("one response payload %d exceeds the bound %d", payload, MAX_PAYLOAD_SIZE),
          payload <= MAX_PAYLOAD_SIZE);
    }
    // every fragmented block was fully released
    assertTrue(server.releaseCount.get() > 0);
    assertEquals("the pending fragment must be cleared in the end", null, server.getPendingBlock());
  }

  @Test(timeout = 60_000)
  public void testSmallResultUsesOneResponseWithoutFragment() throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(10L);
    builder.declarePosition();
    TsBlock tsBlock = builder.build();
    ByteBuffer serialized = new TsBlockSerde().serialize(tsBlock);

    ServerSideSimulation server =
        new ServerSideSimulation(serialized.duplicate(), tsBlock.getPositionCount(), 0);

    IClientRPCService.Iface client = mock(IClientRPCService.Iface.class);
    when(client.closeOperation(any(TSCloseOperationReq.class)))
        .thenReturn(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    when(client.fetchResultsV2(any(TSFetchResultsReq.class)))
        .thenAnswer(invocation -> server.handleFetchResults(invocation.getArgument(0)));
    // the whole result fits into the execute response
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
            List.of(serialized.duplicate()),
            100,
            0,
            ZoneId.systemDefault(),
            RpcUtils.DEFAULT_TIME_FORMAT,
            1_000_000,
            true,
            new ArrayList<>(List.of(0)));

    assertTrue(dataSet.next());
    assertEquals(10L, dataSet.getLong(COLUMN));
    assertFalse(dataSet.next());
    assertEquals("a small result must not trigger any fetch", 0, server.rpcCount.get());
  }

  /** Mimics ClientRPCServiceImpl#fetchResultsV2 with bounded payloads. */
  private static class ServerSideSimulation {
    private final ByteBuffer serialized;
    private final int positionCount;
    private final int totalLength;

    private final AtomicInteger rpcCount = new AtomicInteger();
    private final AtomicInteger releaseCount = new AtomicInteger();
    private final List<Integer> payloadSizes = new ArrayList<>();
    private final List<ByteBuffer> remainingBlocks = new ArrayList<>();
    private final PendingResultFragment pendingFragment =
        new PendingResultFragment(releaseCount::incrementAndGet);
    private final IQueryExecution queryExecution;

    private ServerSideSimulation(ByteBuffer serialized, int positionCount, int totalLength)
        throws Exception {
      this.serialized = serialized;
      this.positionCount = positionCount;
      this.totalLength = totalLength;
      this.remainingBlocks.add(serialized);
      this.queryExecution = Mockito.mock(IQueryExecution.class);
      Mockito.when(queryExecution.getPendingResultFragment()).thenReturn(pendingFragment);
      Mockito.when(queryExecution.getByteBufferBatchResult(true))
          .thenAnswer(
              invocation ->
                  remainingBlocks.isEmpty()
                      ? Optional.empty()
                      : Optional.of(remainingBlocks.remove(0)));
      Mockito.when(queryExecution.hasNextResult())
          .thenAnswer(
              invocation -> !remainingBlocks.isEmpty() || pendingFragment.getBlock() != null);
    }

    private TSFetchResultsResp handleFetchResults(TSFetchResultsReq req) throws Exception {
      rpcCount.incrementAndGet();
      QueryDataSetUtils.ResultChunk chunk =
          QueryDataSetUtils.convertQueryResultByFetchSize(
              queryExecution,
              Math.max(positionCount, 1),
              req.isSetOffset() ? req.getOffset() : 0,
              MAX_PAYLOAD_SIZE);

      int payloadSize = 0;
      for (ByteBuffer block : chunk.getQueryResult()) {
        payloadSize += block.remaining();
      }
      payloadSizes.add(payloadSize);

      TSFetchResultsResp resp =
          new TSFetchResultsResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS), true, true);
      resp.setQueryResult(chunk.getQueryResult());
      resp.setMoreData(!chunk.isFinished());
      if (chunk.hasNextOffset()) {
        resp.setOffset(chunk.getNextOffset());
        if (chunk.hasTotalLength()) {
          resp.setTotalLength(chunk.getTotalLength());
        }
      }
      return resp;
    }

    private ByteBuffer getPendingBlock() {
      return pendingFragment.getBlock();
    }
  }

  /** [time=1, v=10, text="small"], [time=2, v=20, text=<LARGE_VALUE_SIZE bytes>]. */
  private static TsBlock buildTsBlockWithLargeValue() {
    byte[] largeValue = new byte[LARGE_VALUE_SIZE];
    Arrays.fill(largeValue, (byte) 'x');

    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT64, TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(10L);
    builder.getColumnBuilder(1).writeBinary(new Binary("small", TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeLong(20L);
    builder.getColumnBuilder(1).writeBinary(new Binary(largeValue));
    builder.declarePosition();
    return builder.build();
  }
}
