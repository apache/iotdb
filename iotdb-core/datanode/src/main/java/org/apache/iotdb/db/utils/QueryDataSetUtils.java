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

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.PendingResultFragment;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** TimeValuePairUtils to convert between thrift format and TsFile format. */
public class QueryDataSetUtils {

  private static final int FLAG = 0x01;

  private static final TSFileConfig TSFLE_CONFIG = TSFileDescriptor.getInstance().getConfig();

  // default return 8 MB each time
  private static final long MAX_RETURN_SIZE = 8 * 1024 * 1024;

  private QueryDataSetUtils() {}

  public static Pair<TSQueryDataSet, Boolean> convertTsBlockByFetchSize(
      IQueryExecution queryExecution, int fetchSize) throws IOException, IoTDBException {
    fetchSize = fetchSize > 0 ? fetchSize : TSFLE_CONFIG.getMaxTsBlockLineNumber();
    boolean finished = false;
    int columnNum = queryExecution.getOutputValueColumnCount();
    // one time column and each value column has an actual value buffer and a bitmap value to
    // indicate whether it is a null
    int columnNumWithTime = columnNum * 2 + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    PublicBAOS[] byteArrayOutputStreams = new PublicBAOS[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new PublicBAOS();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;

    // used to record a bitmap for every 8 points
    int[] bitmaps = new int[columnNum];
    while (rowCount < fetchSize) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();
      if (!tsBlock.isEmpty()) {
        int currentCount = tsBlock.getPositionCount();
        serializeTsBlock(rowCount, currentCount, tsBlock, columnNum, dataOutputStreams, bitmaps);
        rowCount += currentCount;
      }
    }

    fillRemainingBitMap(rowCount, columnNum, dataOutputStreams, bitmaps);

    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    fillTimeColumn(byteArrayOutputStreams, tsQueryDataSet);

    fillValueColumnsAndBitMaps(byteArrayOutputStreams, tsQueryDataSet);

    return new Pair<>(tsQueryDataSet, finished);
  }

  public static TSQueryDataSet convertTsBlockByFetchSize(List<TsBlock> tsBlocks)
      throws IOException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    // one time column and each value column has an actual value buffer and a bitmap value to
    // indicate whether it is a null
    int columnNum = 1;
    int columnNumWithTime = columnNum * 2 + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    PublicBAOS[] byteArrayOutputStreams = new PublicBAOS[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new PublicBAOS();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;

    // used to record a bitmap for every 8 points
    int[] bitmaps = new int[columnNum];
    for (TsBlock tsBlock : tsBlocks) {
      if (tsBlock.isEmpty()) {
        continue;
      }

      int currentCount = tsBlock.getPositionCount();
      serializeTsBlock(rowCount, currentCount, tsBlock, columnNum, dataOutputStreams, bitmaps);
      rowCount += currentCount;
    }
    // feed the remaining bitmap
    int remaining = rowCount % 8;
    for (int k = 0; k < columnNum; k++) {
      if (remaining != 0) {
        DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (k + 1)];
        dataBitmapOutputStream.writeByte(bitmaps[k] << (8 - remaining));
      }
    }

    // calculate the time buffer size
    tsQueryDataSet.setTime(wrapBuffer(byteArrayOutputStreams[0]));

    // calculate the bitmap buffer size
    List<ByteBuffer> bitmapList = new ArrayList<>(columnNum);
    List<ByteBuffer> valueList = new ArrayList<>(columnNum);
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      valueList.add(wrapBuffer(byteArrayOutputStreams[i]));
      bitmapList.add(wrapBuffer(byteArrayOutputStreams[i + 1]));
    }
    tsQueryDataSet.setBitmapList(bitmapList);
    tsQueryDataSet.setValueList(valueList);
    return tsQueryDataSet;
  }

  private static void serializeTsBlock(
      int rowCount,
      int currentCount,
      TsBlock tsBlock,
      int columnNum,
      DataOutputStream[] dataOutputStreams,
      int[] bitmaps)
      throws IOException {
    // serialize time column
    for (int i = 0; i < currentCount; i++) {
      // use columnOutput to write byte array
      dataOutputStreams[0].writeLong(tsBlock.getTimeByIndex(i));
    }

    // serialize each value column and its bitmap
    for (int k = 0; k < columnNum; k++) {
      // get DataOutputStream for current value column and its bitmap
      DataOutputStream dataOutputStream = dataOutputStreams[2 * k + 1];
      DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (k + 1)];

      Column column = tsBlock.getColumn(k);
      serializeColumn(rowCount, column, bitmaps, k, dataOutputStream, dataBitmapOutputStream);
    }
  }

  private static void serializeColumn(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        column.writeTo(i, dataOutputStream);
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void fillRemainingBitMap(
      int rowCount, int columnNum, DataOutputStream[] dataOutputStreams, int[] bitmaps)
      throws IOException {
    // feed the remaining bitmap
    int remaining = rowCount % 8;
    for (int k = 0; k < columnNum; k++) {
      if (remaining != 0) {
        DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (k + 1)];
        dataBitmapOutputStream.writeByte(bitmaps[k] << (8 - remaining));
      }
    }
  }

  private static void fillTimeColumn(
      PublicBAOS[] byteArrayOutputStreams, TSQueryDataSet tsQueryDataSet) {
    tsQueryDataSet.setTime(wrapBuffer(byteArrayOutputStreams[0]));
  }

  private static void fillValueColumnsAndBitMaps(
      PublicBAOS[] byteArrayOutputStreams, TSQueryDataSet tsQueryDataSet) {
    int columnNum = byteArrayOutputStreams.length / 2;
    List<ByteBuffer> bitmapList = new ArrayList<>(columnNum);
    List<ByteBuffer> valueList = new ArrayList<>(columnNum);
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      valueList.add(wrapBuffer(byteArrayOutputStreams[i]));
      bitmapList.add(wrapBuffer(byteArrayOutputStreams[i + 1]));
    }
    tsQueryDataSet.setBitmapList(bitmapList);
    tsQueryDataSet.setValueList(valueList);
  }

  private static ByteBuffer wrapBuffer(PublicBAOS outputStream) {
    return ByteBuffer.wrap(outputStream.getBuf(), 0, outputStream.size());
  }

  /**
   * To fetch required amounts of data and combine them through List
   *
   * @param queryExecution used to get TsBlock from and judge whether there is more data.
   * @param fetchSize wanted row size
   * @return pair.left is serialized TsBlock pair.right indicates if the read finished
   * @throws IoTDBException IoTDBException may be thrown if error happened while getting TsBlock
   *     from IQueryExecution
   */
  public static Pair<List<ByteBuffer>, Boolean> convertQueryResultByFetchSize(
      IQueryExecution queryExecution, int fetchSize) throws IoTDBException {
    fetchSize = fetchSize > 0 ? fetchSize : TSFLE_CONFIG.getMaxTsBlockLineNumber();
    int rowCount = 0;
    long memorySize = 0;
    List<ByteBuffer> res = new ArrayList<>();
    while (rowCount < fetchSize && memorySize < MAX_RETURN_SIZE) {
      Optional<ByteBuffer> optionalByteBuffer = queryExecution.getByteBufferBatchResult();
      if (!optionalByteBuffer.isPresent()) {
        break;
      }
      ByteBuffer byteBuffer = optionalByteBuffer.get();
      byteBuffer.mark();
      int valueColumnCount = byteBuffer.getInt();
      for (int i = 0; i < valueColumnCount; i++) {
        byteBuffer.get();
      }
      int positionCount = byteBuffer.getInt();
      byteBuffer.reset();
      if (positionCount != 0) {
        res.add(byteBuffer);
      }
      rowCount += positionCount;
      memorySize += byteBuffer.limit();
    }
    return new Pair<>(res, !queryExecution.hasNextResult());
  }

  /**
   * Fetches query results with a bounded payload size. A serialized TsBlock larger than {@code
   * maxPayloadSize} is cut into fragments: the response only carries the bytes that fit into the
   * payload, and the remaining bytes are kept in {@link PendingResultFragment} on the query
   * execution, to be sent by the following request carrying the returned offset.
   *
   * @param queryExecution used to get TsBlock from and judge whether there is more data.
   * @param fetchSize wanted row size
   * @param requestOffset the offset of the pending fragment to continue, 0 if there is none
   * @param maxPayloadSize the maximum total bytes of {@link ResultChunk#getQueryResult()}
   */
  public static ResultChunk convertQueryResultByFetchSize(
      IQueryExecution queryExecution, int fetchSize, int requestOffset, int maxPayloadSize)
      throws IoTDBException {
    fetchSize = fetchSize > 0 ? fetchSize : TSFLE_CONFIG.getMaxTsBlockLineNumber();
    List<ByteBuffer> res = new ArrayList<>();
    int remainingPayloadSize = maxPayloadSize;
    int rowCount = 0;
    long memorySize = 0;

    // Continue the oversized TsBlock left by the previous response first
    PendingResultFragment pendingFragment = queryExecution.getPendingResultFragment();
    ByteBuffer pendingBlock = pendingFragment.getBlock();
    if (pendingBlock != null) {
      int nextOffset = pendingFragment.getNextOffset();
      if (requestOffset != nextOffset) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages
                    .EXCEPTION_RESULT_FRAGMENT_REQUEST_OFFSET_ARG_DOES_NOT_MATCH_PENDING_FRAGMENT_OFFSET_ARG_A1B13DBC,
                requestOffset,
                nextOffset));
      }
      int fragmentSize = Math.min(remainingPayloadSize, pendingBlock.limit() - nextOffset);
      res.add(slice(pendingBlock, nextOffset, fragmentSize));
      if (nextOffset + fragmentSize == pendingBlock.limit()) {
        pendingFragment.clear();
        // The fragmented block is now complete. Do not fetch new blocks in the same response,
        // otherwise the response would carry the tail of the big block plus up to fetchSize new
        // rows, whose row count is not accounted for. One response either starts/continues a
        // fragmented block or delivers new blocks, never both.
        return ResultChunk.complete(res, !queryExecution.hasNextResult());
      }
      pendingFragment.reset(pendingBlock, nextOffset + fragmentSize);
      return ResultChunk.fragment(res, nextOffset + fragmentSize, -1, false);
    } else if (requestOffset != 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_RESULT_FRAGMENT_REQUEST_OFFSET_ARG_IS_INVALID_WITHOUT_PENDING_FRAGMENT_C403454F,
              requestOffset));
    }

    while (rowCount < fetchSize && memorySize < MAX_RETURN_SIZE && remainingPayloadSize > 0) {
      Optional<ByteBuffer> optionalByteBuffer = queryExecution.getByteBufferBatchResult();
      if (!optionalByteBuffer.isPresent()) {
        break;
      }
      ByteBuffer byteBuffer = optionalByteBuffer.get();
      int positionCount = readPositionCount(byteBuffer);
      if (positionCount == 0) {
        continue;
      }
      // offsets kept in PendingResultFragment are absolute positions of the buffer
      int blockStart = byteBuffer.position();
      int blockSize = byteBuffer.remaining();
      rowCount += positionCount;
      if (blockSize <= remainingPayloadSize) {
        res.add(byteBuffer);
        remainingPayloadSize -= blockSize;
        memorySize += blockSize;
      } else {
        // The block exceeds the remaining payload, send a leading fragment and keep the rest
        res.add(slice(byteBuffer, blockStart, remainingPayloadSize));
        pendingFragment.reset(byteBuffer, blockStart + remainingPayloadSize);
        return ResultChunk.fragment(
            res, blockStart + remainingPayloadSize, blockStart == 0 ? blockSize : -1, false);
      }
    }
    return ResultChunk.complete(res, !queryExecution.hasNextResult());
  }

  private static int readPositionCount(ByteBuffer byteBuffer) {
    byteBuffer.mark();
    int valueColumnCount = byteBuffer.getInt();
    for (int i = 0; i < valueColumnCount; i++) {
      byteBuffer.get();
    }
    int positionCount = byteBuffer.getInt();
    byteBuffer.reset();
    return positionCount;
  }

  /** Returns a view of {@code [offset, offset + length)} bytes of the buffer without copying. */
  private static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.position(offset);
    duplicate.limit(offset + length);
    return duplicate.slice();
  }

  /** The bounded-payload result of one query result RPC response. */
  public static class ResultChunk {

    private final List<ByteBuffer> queryResult;
    private final int nextOffset;
    private final int totalLength;
    private final boolean finished;

    private ResultChunk(
        List<ByteBuffer> queryResult, int nextOffset, int totalLength, boolean finished) {
      this.queryResult = queryResult;
      this.nextOffset = nextOffset;
      this.totalLength = totalLength;
      this.finished = finished;
    }

    /** The response ends with a fragment, which starts at {@code nextOffset} of its TsBlock. */
    public static ResultChunk fragment(
        List<ByteBuffer> queryResult, int nextOffset, int totalLength, boolean finished) {
      return new ResultChunk(queryResult, nextOffset, totalLength, finished);
    }

    public static ResultChunk complete(List<ByteBuffer> queryResult, boolean finished) {
      return new ResultChunk(queryResult, -1, -1, finished);
    }

    public List<ByteBuffer> getQueryResult() {
      return queryResult;
    }

    public boolean hasNextOffset() {
      return nextOffset >= 0;
    }

    public int getNextOffset() {
      return nextOffset;
    }

    /** The total serialized length of the last block, set only when the fragment starts it. */
    public boolean hasTotalLength() {
      return totalLength >= 0;
    }

    public int getTotalLength() {
      return totalLength;
    }

    public boolean isFinished() {
      return finished;
    }
  }

  public static long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    long[] times = new long[size];
    for (int i = 0; i < size; i++) {
      times[i] = buffer.getLong();
    }
    return times;
  }

  public static long[] readTimesFromStream(DataInputStream stream, int size) throws IOException {
    long[] times = new long[size];
    for (int i = 0; i < size; i++) {
      times[i] = stream.readLong();
    }
    return times;
  }

  public static Optional<BitMap[]> readBitMapsFromBuffer(ByteBuffer buffer, int columns, int size) {
    if (!buffer.hasRemaining()) {
      return Optional.empty();
    }
    BitMap[] bitMaps = new BitMap[columns];
    for (int i = 0; i < columns; i++) {
      boolean hasBitMap = BytesUtils.byteToBool(buffer.get());
      if (hasBitMap) {
        byte[] bytes = new byte[BitMap.getSizeOfBytes(size)];
        buffer.get(bytes);
        bitMaps[i] = new BitMap(size, bytes);
      }
    }
    return Optional.of(bitMaps);
  }

  public static Optional<BitMap[]> readBitMapsFromStream(
      DataInputStream stream, int columns, int size) throws IOException {
    if (stream.available() <= 0) {
      return Optional.empty();
    }
    BitMap[] bitMaps = new BitMap[columns];
    for (int i = 0; i < columns; i++) {
      boolean hasBitMap = BytesUtils.byteToBool(stream.readByte());
      if (hasBitMap) {
        byte[] bytes = new byte[BitMap.getSizeOfBytes(size)];
        stream.readFully(bytes);
        bitMaps[i] = new BitMap(size, bytes);
      }
    }
    return Optional.of(bitMaps);
  }

  public static Object[] readTabletValuesFromBuffer(
      ByteBuffer buffer, List<Integer> types, int columns, int size) {
    TSDataType[] dataTypes = new TSDataType[types.size()];
    for (int i = 0; i < dataTypes.length; i++) {
      dataTypes[i] = TSDataType.values()[types.get(i)];
    }
    return readTabletValuesFromBuffer(buffer, dataTypes, columns, size);
  }

  /**
   * Deserialize Tablet Values From Buffer
   *
   * @param buffer data values
   * @param columns column number
   * @param size value count in each column
   * @throws UnSupportedDataTypeException if TSDataType is unknown, UnSupportedDataTypeException
   *     will be thrown.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Object[] readTabletValuesFromBuffer(
      ByteBuffer buffer, TSDataType[] types, int columns, int size) {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      values[i] =
          TypeServices.StorageEngine.RAW_ARRAY_BYTE_BUFFER_DESERIALIZER_SERVICE
              .call(Type.fromTsDataType(types[i]))
              .apply(buffer, size);
    }
    return values;
  }

  public static Object[] readTabletValuesFromStream(
      DataInputStream stream, TSDataType[] types, int columns, int size) throws IOException {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      values[i] =
          TypeServices.StorageEngine.RAW_ARRAY_INPUT_STREAM_DESERIALIZER_SERVICE
              .call(Type.fromTsDataType(types[i]))
              .deserialize(stream, size);
    }
    return values;
  }
}
