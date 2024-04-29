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
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.AggrWindowIterator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TimeDuration;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.AVG_DAILY_DRIVING_DURATION_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.AVG_DAILY_DRIVING_SESSION_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.AVG_LOAD_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.AVG_VS_PROJ_FUEL_CONSUMPTION_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.BREAKDOWN_FREQUENCY_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.DAILY_ACTIVITY_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.DRIVER_LEVEL;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.FLEET_LEVEL;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.HIGH_LOAD_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.LONG_DRIVING_SESSIONS_DATA_TYPES;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.MODEL_LEVEL;
import static org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl.NAME_LEVEL;
import static org.apache.iotdb.db.utils.TimestampPrecisionUtils.convertToCurrPrecision;

/** TimeValuePairUtils to convert between thrift format and TsFile format. */
public class QueryDataSetUtils {

  private static final int FLAG = 0x01;

  private QueryDataSetUtils() {}

  public static Pair<TSQueryDataSet, Boolean> convertTsBlockByFetchSize(
      IQueryExecution queryExecution, int fetchSize) throws IOException, IoTDBException {
    boolean finished = false;
    int columnNum = queryExecution.getOutputValueColumnCount();
    // one time column and each value column has an actual value buffer and a bitmap value to
    // indicate whether it is a null
    int columnNumWithTime = columnNum * 2 + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;
    int[] valueOccupation = new int[columnNum];

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
        serializeTsBlock(
            rowCount,
            currentCount,
            tsBlock,
            columnNum,
            dataOutputStreams,
            valueOccupation,
            bitmaps);
        rowCount += currentCount;
      }
    }

    fillRemainingBitMap(rowCount, columnNum, dataOutputStreams, bitmaps);

    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    fillTimeColumn(rowCount, byteArrayOutputStreams, tsQueryDataSet);

    fillValueColumnsAndBitMaps(rowCount, byteArrayOutputStreams, valueOccupation, tsQueryDataSet);

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
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;
    int[] valueOccupation = new int[columnNum];

    // used to record a bitmap for every 8 points
    int[] bitmaps = new int[columnNum];
    for (TsBlock tsBlock : tsBlocks) {
      if (tsBlock.isEmpty()) {
        continue;
      }

      int currentCount = tsBlock.getPositionCount();
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
        TSDataType type = column.getDataType();
        switch (type) {
          case INT32:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                dataOutputStream.writeInt(column.getInt(i));
                valueOccupation[k] += 4;
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          case INT64:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                dataOutputStream.writeLong(column.getLong(i));
                valueOccupation[k] += 8;
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          case FLOAT:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                dataOutputStream.writeFloat(column.getFloat(i));
                valueOccupation[k] += 4;
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          case DOUBLE:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                dataOutputStream.writeDouble(column.getDouble(i));
                valueOccupation[k] += 8;
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          case BOOLEAN:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                dataOutputStream.writeBoolean(column.getBoolean(i));
                valueOccupation[k] += 1;
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          case TEXT:
            for (int i = 0; i < currentCount; i++) {
              rowCount++;
              if (column.isNull(i)) {
                bitmaps[k] = bitmaps[k] << 1;
              } else {
                bitmaps[k] = (bitmaps[k] << 1) | FLAG;
                Binary binary = column.getBinary(i);
                dataOutputStream.writeInt(binary.getLength());
                dataOutputStream.write(binary.getValues());
                valueOccupation[k] = valueOccupation[k] + 4 + binary.getLength();
              }
              if (rowCount != 0 && rowCount % 8 == 0) {
                dataBitmapOutputStream.writeByte(bitmaps[k]);
                // we should clear the bitmap every 8 points
                bitmaps[k] = 0;
              }
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
        if (k != columnNum - 1) {
          rowCount -= currentCount;
        }
      }
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
    int timeOccupation = rowCount * 8;
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeOccupation);
    timeBuffer.put(byteArrayOutputStreams[0].toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    // calculate the bitmap buffer size
    int bitmapOccupation = (rowCount + 7) / 8;

    List<ByteBuffer> bitmapList = new LinkedList<>();
    List<ByteBuffer> valueList = new LinkedList<>();
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation[(i - 1) / 2]);
      valueBuffer.put(byteArrayOutputStreams[i].toByteArray());
      valueBuffer.flip();
      valueList.add(valueBuffer);

      ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapOccupation);
      bitmapBuffer.put(byteArrayOutputStreams[i + 1].toByteArray());
      bitmapBuffer.flip();
      bitmapList.add(bitmapBuffer);
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
      int[] valueOccupation,
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
      TSDataType type = column.getDataType();
      switch (type) {
        case INT32:
          doWithInt32Column(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        case INT64:
          doWithInt64Column(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        case FLOAT:
          doWithFloatColumn(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        case DOUBLE:
          doWithDoubleColumn(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        case BOOLEAN:
          doWithBooleanColumn(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        case TEXT:
          doWithTextColumn(
              rowCount,
              column,
              bitmaps,
              k,
              dataOutputStream,
              valueOccupation,
              dataBitmapOutputStream);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", type));
      }
    }
  }

  private static void doWithInt32Column(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        dataOutputStream.writeInt(column.getInt(i));
        valueOccupation[columnIndex] += 4;
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void doWithInt64Column(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        dataOutputStream.writeLong(column.getLong(i));
        valueOccupation[columnIndex] += 8;
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void doWithFloatColumn(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        dataOutputStream.writeFloat(column.getFloat(i));
        valueOccupation[columnIndex] += 4;
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void doWithDoubleColumn(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        dataOutputStream.writeDouble(column.getDouble(i));
        valueOccupation[columnIndex] += 8;
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void doWithBooleanColumn(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        dataOutputStream.writeBoolean(column.getBoolean(i));
        valueOccupation[columnIndex] += 1;
      }
      if (rowCount != 0 && rowCount % 8 == 0) {
        dataBitmapOutputStream.writeByte(bitmaps[columnIndex]);
        // we should clear the bitmap every 8 points
        bitmaps[columnIndex] = 0;
      }
    }
  }

  private static void doWithTextColumn(
      int rowCount,
      Column column,
      int[] bitmaps,
      int columnIndex,
      DataOutputStream dataOutputStream,
      int[] valueOccupation,
      DataOutputStream dataBitmapOutputStream)
      throws IOException {
    for (int i = 0, size = column.getPositionCount(); i < size; i++) {
      rowCount++;
      if (column.isNull(i)) {
        bitmaps[columnIndex] = bitmaps[columnIndex] << 1;
      } else {
        bitmaps[columnIndex] = (bitmaps[columnIndex] << 1) | FLAG;
        Binary binary = column.getBinary(i);
        dataOutputStream.writeInt(binary.getLength());
        dataOutputStream.write(binary.getValues());
        valueOccupation[columnIndex] = valueOccupation[columnIndex] + 4 + binary.getLength();
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
      int rowCount, ByteArrayOutputStream[] byteArrayOutputStreams, TSQueryDataSet tsQueryDataSet) {
    // calculate the time buffer size
    int timeOccupation = rowCount * 8;
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeOccupation);
    timeBuffer.put(byteArrayOutputStreams[0].toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);
  }

  private static void fillValueColumnsAndBitMaps(
      int rowCount,
      ByteArrayOutputStream[] byteArrayOutputStreams,
      int[] valueOccupation,
      TSQueryDataSet tsQueryDataSet) {
    // calculate the bitmap buffer size
    int bitmapOccupation = (rowCount + 7) / 8;

    List<ByteBuffer> bitmapList = new LinkedList<>();
    List<ByteBuffer> valueList = new LinkedList<>();
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation[(i - 1) / 2]);
      valueBuffer.put(byteArrayOutputStreams[i].toByteArray());
      valueBuffer.flip();
      valueList.add(valueBuffer);

      ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapOccupation);
      bitmapBuffer.put(byteArrayOutputStreams[i + 1].toByteArray());
      bitmapBuffer.flip();
      bitmapList.add(bitmapBuffer);
    }
    tsQueryDataSet.setBitmapList(bitmapList);
    tsQueryDataSet.setValueList(valueList);
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
    int rowCount = 0;
    List<ByteBuffer> res = new ArrayList<>();
    while (rowCount < fetchSize) {
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
    }
    return new Pair<>(res, !queryExecution.hasNextResult());
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
        byte[] bytes = new byte[size / Byte.SIZE + 1];
        for (int j = 0; j < bytes.length; j++) {
          bytes[j] = buffer.get();
        }
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
        byte[] bytes = new byte[size / Byte.SIZE + 1];
        for (int j = 0; j < bytes.length; j++) {
          bytes[j] = stream.readByte();
        }
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
      switch (types[i]) {
        case BOOLEAN:
          boolean[] boolValues = new boolean[size];
          for (int index = 0; index < size; index++) {
            boolValues[index] = BytesUtils.byteToBool(buffer.get());
          }
          values[i] = boolValues;
          break;
        case INT32:
          int[] intValues = new int[size];
          for (int index = 0; index < size; index++) {
            intValues[index] = buffer.getInt();
          }
          values[i] = intValues;
          break;
        case INT64:
          long[] longValues = new long[size];
          for (int index = 0; index < size; index++) {
            longValues[index] = buffer.getLong();
          }
          values[i] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[size];
          for (int index = 0; index < size; index++) {
            floatValues[index] = buffer.getFloat();
          }
          values[i] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[size];
          for (int index = 0; index < size; index++) {
            doubleValues[index] = buffer.getDouble();
          }
          values[i] = doubleValues;
          break;
        case TEXT:
          Binary[] binaryValues = new Binary[size];
          for (int index = 0; index < size; index++) {
            int binarySize = buffer.getInt();
            byte[] binaryValue = new byte[binarySize];
            buffer.get(binaryValue);
            binaryValues[index] = new Binary(binaryValue);
          }
          values[i] = binaryValues;
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("data type %s is not supported when convert data at client", types[i]));
      }
    }
    return values;
  }

  public static Object[] readTabletValuesFromStream(
      DataInputStream stream, TSDataType[] types, int columns, int size) throws IOException {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      switch (types[i]) {
        case BOOLEAN:
          parseBooleanColumn(size, stream, values, i);
          break;
        case INT32:
          parseInt32Column(size, stream, values, i);
          break;
        case INT64:
          parseInt64Column(size, stream, values, i);
          break;
        case FLOAT:
          parseFloatColumn(size, stream, values, i);
          break;
        case DOUBLE:
          parseDoubleColumn(size, stream, values, i);
          break;
        case TEXT:
          parseTextColumn(size, stream, values, i);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("data type %s is not supported when convert data at client", types[i]));
      }
    }
    return values;
  }

  private static void parseBooleanColumn(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    boolean[] boolValues = new boolean[size];
    for (int index = 0; index < size; index++) {
      boolValues[index] = BytesUtils.byteToBool(stream.readByte());
    }
    values[columnIndex] = boolValues;
  }

  private static void parseInt32Column(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    int[] intValues = new int[size];
    for (int index = 0; index < size; index++) {
      intValues[index] = stream.readInt();
    }
    values[columnIndex] = intValues;
  }

  private static void parseInt64Column(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    long[] longValues = new long[size];
    for (int index = 0; index < size; index++) {
      longValues[index] = stream.readLong();
    }
    values[columnIndex] = longValues;
  }

  private static void parseFloatColumn(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    float[] floatValues = new float[size];
    for (int index = 0; index < size; index++) {
      floatValues[index] = stream.readFloat();
    }
    values[columnIndex] = floatValues;
  }

  private static void parseDoubleColumn(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    double[] doubleValues = new double[size];
    for (int index = 0; index < size; index++) {
      doubleValues[index] = stream.readDouble();
    }
    values[columnIndex] = doubleValues;
  }

  private static void parseTextColumn(
      int size, DataInputStream stream, Object[] values, int columnIndex) throws IOException {
    Binary[] binaryValues = new Binary[size];
    for (int index = 0; index < size; index++) {
      int binarySize = stream.readInt();
      byte[] binaryValue = new byte[binarySize];
      int actualReadSize = stream.read(binaryValue);
      if (actualReadSize != binarySize) {
        throw new IllegalStateException(
            "Expect to read " + binarySize + " bytes, actually read " + actualReadSize + "bytes.");
      }
      binaryValues[index] = new Binary(binaryValue);
    }
    values[columnIndex] = binaryValues;
  }

  public static Pair<List<ByteBuffer>, Boolean> constructHighLoadResult(
      IQueryExecution queryExecution,
      Map<String, ClientRPCServiceImpl.DeviceAttributes> deviceAttributesMap,
      TsBlockSerde serde)
      throws IoTDBException, IOException {
    boolean finished;

    TsBlockBuilder builder = new TsBlockBuilder(HIGH_LOAD_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder nameColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder driverColumnBuilder = builder.getColumnBuilder(1);
    ColumnBuilder currentLoadColumnBuilder = builder.getColumnBuilder(2);
    ColumnBuilder loadCapacityColumnBuilder = builder.getColumnBuilder(3);

    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        Column timeSeriesColumn = tsBlock.getColumn(0);
        Column valueColumn = tsBlock.getColumn(1);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          String timeSeries = timeSeriesColumn.getBinary(i).getStringValue(StandardCharsets.UTF_8);
          String[] seriesArray = timeSeries.split("\\.");
          String name = seriesArray[NAME_LEVEL];
          String driver = seriesArray[DRIVER_LEVEL];
          double value =
              Double.parseDouble(valueColumn.getBinary(i).getStringValue(StandardCharsets.UTF_8));
          double loadCapacity = deviceAttributesMap.get(name).loadCapacity;

          if (value >= 0.9 * loadCapacity) {
            builder.declarePosition();
            // time column
            timeColumnBuilder.writeLong(tsBlock.getTimeByIndex(i));

            // name column
            nameColumnBuilder.writeBinary(new Binary(name, StandardCharsets.UTF_8));

            // driver column
            driverColumnBuilder.writeBinary(new Binary(driver, StandardCharsets.UTF_8));

            // current_load column
            currentLoadColumnBuilder.writeDouble(value);

            // load_capacity column
            loadCapacityColumnBuilder.writeDouble(loadCapacity);
          }
        }
      }
    }

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  public static Pair<List<ByteBuffer>, Boolean> constructLongDrivingSessionsResult(
      IQueryExecution queryExecution, TsBlockSerde serde, int threshold)
      throws IoTDBException, IOException {
    boolean finished;

    TsBlockBuilder builder = new TsBlockBuilder(LONG_DRIVING_SESSIONS_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder nameColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder driverColumnBuilder = builder.getColumnBuilder(1);

    Binary previousDevice = new Binary("", StandardCharsets.UTF_8);
    int count = 0;
    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        Column deviceColumn = tsBlock.getColumn(0);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          Binary deviceId = deviceColumn.getBinary(i);
          // TODO ty optimize Binary equals method
          if (previousDevice.equals(deviceId)) {
            count++;
          } else {
            // device changed, we need to decide whether to output previous device
            if (count > threshold) {
              String[] seriesArray =
                  previousDevice.getStringValue(StandardCharsets.UTF_8).split("\\.");
              String name = seriesArray[NAME_LEVEL];
              String driver = seriesArray[DRIVER_LEVEL];
              builder.declarePosition();
              timeColumnBuilder.writeLong(0);
              // name column
              nameColumnBuilder.writeBinary(new Binary(name, StandardCharsets.UTF_8));
              // driver column
              driverColumnBuilder.writeBinary(new Binary(driver, StandardCharsets.UTF_8));
            }
            previousDevice = deviceId;
            count = 1;
          }
        }
      }
    }

    // last device, we need to decide whether to output previous device
    if (count > 22) {
      String[] seriesArray = previousDevice.getStringValue(StandardCharsets.UTF_8).split("\\.");
      String name = seriesArray[NAME_LEVEL];
      String driver = seriesArray[DRIVER_LEVEL];
      builder.declarePosition();
      timeColumnBuilder.writeLong(0);
      // name column
      nameColumnBuilder.writeBinary(new Binary(name, StandardCharsets.UTF_8));
      // driver column
      driverColumnBuilder.writeBinary(new Binary(driver, StandardCharsets.UTF_8));
    }

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  public static Pair<List<ByteBuffer>, Boolean> constructAvgVsProjectedFuelConsumptionResult(
      IQueryExecution queryExecution,
      Map<String, ClientRPCServiceImpl.DeviceAttributes> deviceAttributesMap,
      TsBlockSerde serde)
      throws IoTDBException, IOException {
    boolean finished;

    Map<String, AvgIntermediateResult> map = new HashMap<>();
    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        Column deviceColumn = tsBlock.getColumn(0);
        Column sumFuelColumn = tsBlock.getColumn(1);
        Column countFuelColumn = tsBlock.getColumn(2);

        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          String deviceId = deviceColumn.getBinary(i).getStringValue(StandardCharsets.UTF_8);
          String[] seriesArray = deviceId.split("\\.");
          String model = seriesArray[MODEL_LEVEL];
          String name = seriesArray[NAME_LEVEL];
          double nominalFuelConsumption = deviceAttributesMap.get(name).nominalFuelConsumption;

          AvgIntermediateResult result =
              map.computeIfAbsent(model, k -> new AvgIntermediateResult());
          long count = countFuelColumn.getLong(i);
          result.count += count;
          result.fuelSum += sumFuelColumn.getDouble(i);
          result.nominalFuelSum += (nominalFuelConsumption * count);
        }
      }
    }

    int size = map.size();
    TsBlockBuilder builder = new TsBlockBuilder(size, AVG_VS_PROJ_FUEL_CONSUMPTION_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder fuelColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder avgFuelConsumptionColumnBuilder = builder.getColumnBuilder(1);
    ColumnBuilder nomimalFuelConsumptionColumnBuilder = builder.getColumnBuilder(2);

    map.forEach(
        (k, v) -> {
          timeColumnBuilder.writeLong(0);
          fuelColumnBuilder.writeBinary(new Binary(k, StandardCharsets.UTF_8));
          avgFuelConsumptionColumnBuilder.writeDouble(v.fuelSum / v.count);
          nomimalFuelConsumptionColumnBuilder.writeDouble(v.nominalFuelSum / v.count);
        });

    builder.declarePositions(size);

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  private static class AvgIntermediateResult {
    long count = 0;
    double fuelSum = 0.0d;

    double nominalFuelSum = 0.0d;
  }

  public static Pair<List<ByteBuffer>, Boolean> constructAvgDailyDrivingDurationResult(
      IQueryExecution queryExecution, TsBlockSerde serde, long startTime, long endTime)
      throws IoTDBException, IOException {
    boolean finished;

    TsBlockBuilder builder = new TsBlockBuilder(AVG_DAILY_DRIVING_DURATION_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder fleetColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder nameColumnBuilder = builder.getColumnBuilder(1);
    ColumnBuilder driverColumnBuilder = builder.getColumnBuilder(2);
    ColumnBuilder avgDailyHoursColumnBuilder = builder.getColumnBuilder(3);

    Binary previousDevice = new Binary("", StandardCharsets.UTF_8);
    Binary fleet = null;
    Binary name = null;
    Binary driver = null;

    TimeDuration interval = new TimeDuration(0, convertToCurrPrecision(1, TimeUnit.DAYS));
    AggrWindowIterator timeRangeIterator =
        new AggrWindowIterator(startTime, endTime, interval, interval, true, true);
    TimeRange currentTimeRange = timeRangeIterator.nextTimeRange();
    int count = 0;
    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        TimeColumn timeColumn = tsBlock.getTimeColumn();
        Column deviceColumn = tsBlock.getColumn(0);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          Binary deviceId = deviceColumn.getBinary(i);
          long currentTime = timeColumn.getLong(i);
          if (previousDevice.equals(deviceId)) {
            // calculate current dat
            if (currentTimeRange.contains(
                currentTime)) { // current time range for current device is not finished
              count++;
            } else {
              // previous day's result is done
              builder.declarePosition();
              timeColumnBuilder.writeLong(currentTimeRange.getMin());
              fleetColumnBuilder.writeBinary(fleet);
              nameColumnBuilder.writeBinary(name);
              driverColumnBuilder.writeBinary(driver);
              avgDailyHoursColumnBuilder.writeDouble(count / 6.0d);

              // move time range to next one that contains the current time
              currentTimeRange = timeRangeIterator.nextTimeRange();
              while (!currentTimeRange.contains(currentTime)) {
                builder.declarePosition();
                timeColumnBuilder.writeLong(currentTimeRange.getMin());
                fleetColumnBuilder.writeBinary(fleet);
                nameColumnBuilder.writeBinary(name);
                driverColumnBuilder.writeBinary(driver);
                avgDailyHoursColumnBuilder.writeDouble(0.0d);
                currentTimeRange = timeRangeIterator.nextTimeRange();
              }
              count = 1;
            }
          } else { // device changed, we need to decide whether to output previous device

            if (fleet != null) {
              // construct remaining data for previous device
              do {
                builder.declarePosition();
                timeColumnBuilder.writeLong(currentTimeRange.getMin());
                fleetColumnBuilder.writeBinary(fleet);
                nameColumnBuilder.writeBinary(name);
                driverColumnBuilder.writeBinary(driver);
                avgDailyHoursColumnBuilder.writeDouble(count / 6.0d);
                currentTimeRange = timeRangeIterator.nextTimeRange();
                count = 0;
              } while (timeRangeIterator.hasNextTimeRange());
            }

            // reset
            previousDevice = deviceId;
            String[] splitArray = deviceId.getStringValue(StandardCharsets.UTF_8).split("\\.");
            fleet = new Binary(splitArray[FLEET_LEVEL], StandardCharsets.UTF_8);
            name = new Binary(splitArray[NAME_LEVEL], StandardCharsets.UTF_8);
            driver = new Binary(splitArray[DRIVER_LEVEL], StandardCharsets.UTF_8);
            timeRangeIterator.reset();

            // move time range to next one that contains the current time
            currentTimeRange = timeRangeIterator.nextTimeRange();
            while (!currentTimeRange.contains(currentTime)) {
              builder.declarePosition();
              timeColumnBuilder.writeLong(currentTimeRange.getMin());
              fleetColumnBuilder.writeBinary(fleet);
              nameColumnBuilder.writeBinary(name);
              driverColumnBuilder.writeBinary(driver);
              avgDailyHoursColumnBuilder.writeDouble(0.0d);
              currentTimeRange = timeRangeIterator.nextTimeRange();
            }
            count = 1;
          }
        }
      }
    }

    // output last device
    if (fleet != null) {
      // construct remaining data for last device
      do {
        builder.declarePosition();
        timeColumnBuilder.writeLong(currentTimeRange.getMin());
        fleetColumnBuilder.writeBinary(fleet);
        nameColumnBuilder.writeBinary(name);
        driverColumnBuilder.writeBinary(driver);
        avgDailyHoursColumnBuilder.writeDouble(count / 6.0d);
        currentTimeRange = timeRangeIterator.nextTimeRange();
        count = 0;
      } while (timeRangeIterator.hasNextTimeRange());
    }

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  public static Pair<List<ByteBuffer>, Boolean> constructAvgDailyDrivingSessionResult(
      IQueryExecution queryExecution, TsBlockSerde serde, long startTime, long endTime)
      throws IoTDBException, IOException {
    boolean finished;

    TsBlockBuilder builder = new TsBlockBuilder(AVG_DAILY_DRIVING_SESSION_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder nameColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder durationColumnBuilder = builder.getColumnBuilder(1);

    int numOfTenMinutesInOneDay = 144;
    List<Double> velocityList = new ArrayList<>(numOfTenMinutesInOneDay);
    List<Long> timeList = new ArrayList<>(numOfTenMinutesInOneDay);
    long dayDuration = convertToCurrPrecision(1, TimeUnit.DAYS);
    int day = 0;
    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        TimeColumn timeColumn = tsBlock.getTimeColumn();
        Column deviceColumn = tsBlock.getColumn(0);
        Column velocityColumn = tsBlock.getColumn(1);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          long currentTime = timeColumn.getLong(i);
          double velocity = velocityColumn.getDouble(i);
          timeList.add(currentTime);
          velocityList.add(velocityColumn.isNull(i) ? null : velocity);

          if (timeList.size() == numOfTenMinutesInOneDay) {
            builder.declarePosition();
            timeColumnBuilder.writeLong(startTime + day * dayDuration);
            nameColumnBuilder.writeBinary(
                new Binary(
                    deviceColumn
                        .getBinary(i)
                        .getStringValue(StandardCharsets.UTF_8)
                        .split("\\.")[NAME_LEVEL],
                    StandardCharsets.UTF_8));

            // calculate avg driving duration for one day
            long durationSum = 0;
            long durationCount = 0;
            Long start = null;
            for (int index = 0; index < numOfTenMinutesInOneDay; index++) {
              Double v = velocityList.get(index);
              if (v != null && v > 5) {
                if (start == null) {
                  start = timeList.get(index);
                }
              } else {
                if (start != null) {
                  // one driving duration
                  durationSum += (timeList.get(index) - start);
                  durationCount++;
                  start = null;
                }
              }
            }

            if (start != null) {
              // one driving duration
              durationSum += (startTime + (day + 1) * dayDuration - start);
              durationCount++;
            }
            durationColumnBuilder.writeLong(durationSum / durationCount);

            timeList.clear();
            velocityList.clear();

            day++;
            // 10 days is done, move to next device
            if (day == 10) {
              day = 0;
            }
          }
        }
      }
    }

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  public static Pair<List<ByteBuffer>, Boolean> constructAvgLoadResult(
      IQueryExecution queryExecution,
      Map<String, ClientRPCServiceImpl.DeviceAttributes> deviceAttributesMap,
      TsBlockSerde serde)
      throws IoTDBException, IOException {
    boolean finished;

    Map<AvgLoadKey, AvgLoadIntermediateResult> map = new HashMap<>();

    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        Column deviceColumn = tsBlock.getColumn(0);
        Column avgLoadColumn = tsBlock.getColumn(1);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          if (avgLoadColumn.isNull(i)) {
            continue;
          }
          String deviceId = deviceColumn.getBinary(i).getStringValue(StandardCharsets.UTF_8);
          String[] deviceArray = deviceId.split("\\.");
          String fleet = deviceArray[FLEET_LEVEL];
          String model = deviceArray[MODEL_LEVEL];
          String name = deviceArray[NAME_LEVEL];
          double avgLoad = avgLoadColumn.getDouble(i);
          double loadCapacity = deviceAttributesMap.get(name).loadCapacity;
          AvgLoadKey key = new AvgLoadKey(fleet, model, loadCapacity);
          AvgLoadIntermediateResult intermediateResult =
              map.computeIfAbsent(key, k -> new AvgLoadIntermediateResult());
          intermediateResult.sum += (avgLoad / loadCapacity);
          intermediateResult.count++;
        }
      }
    }

    int size = map.size();

    TsBlockBuilder builder = new TsBlockBuilder(size, AVG_LOAD_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder fleetColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder modelColumnBuilder = builder.getColumnBuilder(1);
    ColumnBuilder loadCapacityColumnBuilder = builder.getColumnBuilder(2);
    ColumnBuilder avgLoadColumnBuilder = builder.getColumnBuilder(3);

    map.forEach(
        (k, v) -> {
          timeColumnBuilder.writeLong(0L);
          fleetColumnBuilder.writeBinary(new Binary(k.fleet, StandardCharsets.UTF_8));
          modelColumnBuilder.writeBinary(new Binary(k.model, StandardCharsets.UTF_8));
          loadCapacityColumnBuilder.writeDouble(k.loadCapacity);
          avgLoadColumnBuilder.writeDouble(v.sum / v.count);
        });

    builder.declarePositions(size);

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  private static class AvgLoadIntermediateResult {
    long count = 0;

    double sum = 0.0d;
  }

  private static class AvgLoadKey {
    private final String fleet;

    private final String model;

    private final double loadCapacity;

    public AvgLoadKey(String fleet, String model, double loadCapacity) {
      this.fleet = fleet;
      this.model = model;
      this.loadCapacity = loadCapacity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AvgLoadKey that = (AvgLoadKey) o;
      return Double.compare(loadCapacity, that.loadCapacity) == 0
          && Objects.equals(fleet, that.fleet)
          && Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fleet, model, loadCapacity);
    }
  }

  public static Pair<List<ByteBuffer>, Boolean> constructDailyActivityResult(
      IQueryExecution queryExecution, TsBlockSerde serde) throws IoTDBException, IOException {
    boolean finished;

    Map<DailyActivityKey, DailyActivityValue> map = new HashMap<>();

    long dayDuration = convertToCurrPrecision(1, TimeUnit.DAYS);

    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent()) {
        finished = true;
        break;
      }
      TsBlock tsBlock = optionalTsBlock.get();

      if (!tsBlock.isEmpty()) {
        TimeColumn timeColumn = tsBlock.getTimeColumn();
        Column deviceColumn = tsBlock.getColumn(0);
        int currentCount = tsBlock.getPositionCount();
        for (int i = 0; i < currentCount; i++) {
          String deviceId = deviceColumn.getBinary(i).getStringValue(StandardCharsets.UTF_8);
          String[] deviceArray = deviceId.split("\\.");
          String fleet = deviceArray[FLEET_LEVEL];
          String model = deviceArray[MODEL_LEVEL];
          long day = timeColumn.getLong(i) / dayDuration * dayDuration;
          DailyActivityKey key = new DailyActivityKey(fleet, model, day);
          DailyActivityValue value = map.computeIfAbsent(key, k -> new DailyActivityValue());
          value.count++;
        }
      }
    }

    int size = map.size();

    TsBlockBuilder builder = new TsBlockBuilder(size, DAILY_ACTIVITY_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder fleetColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder modelColumnBuilder = builder.getColumnBuilder(1);
    ColumnBuilder dailyActivityColumnBuilder = builder.getColumnBuilder(2);

    map.forEach(
        (k, v) -> {
          timeColumnBuilder.writeLong(k.day);
          fleetColumnBuilder.writeBinary(new Binary(k.fleet, StandardCharsets.UTF_8));
          modelColumnBuilder.writeBinary(new Binary(k.model, StandardCharsets.UTF_8));
          dailyActivityColumnBuilder.writeLong(v.count / 144);
        });

    builder.declarePositions(size);

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        finished);
  }

  private static class DailyActivityKey {
    private final String fleet;

    private final String model;

    private final long day;

    public DailyActivityKey(String fleet, String model, long day) {
      this.fleet = fleet;
      this.model = model;
      this.day = day;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DailyActivityKey that = (DailyActivityKey) o;
      return day == that.day
          && Objects.equals(fleet, that.fleet)
          && Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fleet, model, day);
    }
  }

  private static class DailyActivityValue {
    long count = 0;
  }

  public static Pair<List<ByteBuffer>, Boolean> constructBreakdownFrequencyResult(
      IQueryExecution queryExecution1, IQueryExecution queryExecution2, TsBlockSerde serde)
      throws IoTDBException, IOException {

    Map<String, DailyActivityValue> map = new HashMap<>();

    TsBlock totalTsBlock = null;
    boolean totalFinished = false;
    int totalIndex = 0;
    TsBlock statusTsBlock = null;
    boolean statusFinished = false;
    int statusIndex = 0;
    List<Boolean> avtiveList = new ArrayList<>(1440);

    while (!totalFinished && !statusFinished) {
      if (totalTsBlock == null || totalTsBlock.isEmpty()) {
        Optional<TsBlock> optionalTsBlock = queryExecution1.getBatchResult();
        if (!optionalTsBlock.isPresent()) {
          totalFinished = true;
        } else {
          totalTsBlock = optionalTsBlock.get();
        }
      }

      if (statusTsBlock == null || statusTsBlock.isEmpty()) {
        Optional<TsBlock> optionalTsBlock = queryExecution2.getBatchResult();
        if (!optionalTsBlock.isPresent()) {
          statusFinished = true;
        } else {
          statusTsBlock = optionalTsBlock.get();
        }
      }

      if (totalTsBlock != null
          && !totalTsBlock.isEmpty()
          && statusTsBlock != null
          && !statusTsBlock.isEmpty()) {
        Column deviceColumn = totalTsBlock.getColumn(0);
        Column totalColumn = totalTsBlock.getColumn(1);
        Column statusColumn = statusTsBlock.getColumn(1);
        while (totalIndex < totalTsBlock.getPositionCount()
            && statusIndex < statusTsBlock.getPositionCount()) {
          long currentTotal = totalColumn.getLong(totalIndex);
          long currentStatus = statusColumn.getLong(statusIndex);

          avtiveList.add(currentStatus * 1.0d / currentTotal > 0.5);
          if (avtiveList.size() == 1440) {
            String deviceId =
                deviceColumn.getBinary(totalIndex).getStringValue(StandardCharsets.UTF_8);
            String model = deviceId.split("\\.")[MODEL_LEVEL];
            DailyActivityValue value = map.computeIfAbsent(model, k -> new DailyActivityValue());
            boolean meetTrue = false;
            for (boolean active : avtiveList) {
              if (active) {
                if (!meetTrue) {
                  meetTrue = true;
                }
              } else {
                if (meetTrue) {
                  value.count++;
                  meetTrue = false;
                }
              }
            }

            if (meetTrue) {
              value.count++;
            }
            avtiveList.clear();
          }
          totalIndex++;
          statusIndex++;
        }

        if (totalIndex == totalTsBlock.getPositionCount()) {
          totalTsBlock = null;
          totalIndex = 0;
        }

        if (statusIndex == statusTsBlock.getPositionCount()) {
          statusTsBlock = null;
          statusIndex = 0;
        }
      }
    }

    int size = map.size();

    TsBlockBuilder builder = new TsBlockBuilder(size, BREAKDOWN_FREQUENCY_DATA_TYPES);
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    ColumnBuilder modelColumnBuilder = builder.getColumnBuilder(0);
    ColumnBuilder breakDownFrequencyColumnBuilder = builder.getColumnBuilder(1);

    map.forEach(
        (k, v) -> {
          timeColumnBuilder.writeLong(0);
          modelColumnBuilder.writeBinary(new Binary(k, StandardCharsets.UTF_8));
          breakDownFrequencyColumnBuilder.writeLong(v.count);
        });

    builder.declarePositions(size);

    return new Pair<>(
        builder.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(serde.serialize(builder.build())),
        true);
  }
}
