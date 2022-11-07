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
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/** TimeValuePairUtils to convert between thrift format and TsFile format. */
public class QueryDataSetUtils {

  private static final int FLAG = 0x01;

  private QueryDataSetUtils() {}

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static TSQueryDataSet convertQueryDataSetByFetchSize(
      QueryDataSet queryDataSet, int fetchSize, WatermarkEncoder watermarkEncoder)
      throws IOException {
    int columnNum = queryDataSet.getColumnNum();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    // one time column and each value column has a actual value buffer and a bitmap value to
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
    // used to record a bitmap for every 8 row record
    int[] bitmap = new int[columnNum];
    for (int i = 0; i < fetchSize; i++) {
      if (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        // filter rows whose columns are null according to the rule
        if (queryDataSet.withoutNullFilter(rowRecord)) {
          // if the current RowRecord doesn't satisfy, we should also decrease
          // AlreadyReturnedRowNum
          queryDataSet.decreaseAlreadyReturnedRowNum();
          i--;
          continue;
        }

        if (watermarkEncoder != null) {
          rowRecord = watermarkEncoder.encodeRecord(rowRecord);
        }
        // use columnOutput to write byte array
        dataOutputStreams[0].writeLong(rowRecord.getTimestamp());
        List<Field> fields = rowRecord.getFields();
        for (int k = 0; k < fields.size(); k++) {
          Field field = fields.get(k);
          DataOutputStream dataOutputStream = dataOutputStreams[2 * k + 1]; // DO NOT FORGET +1
          if (field == null || field.getDataType() == null) {
            bitmap[k] = (bitmap[k] << 1);
          } else {
            bitmap[k] = (bitmap[k] << 1) | FLAG;
            TSDataType type = field.getDataType();
            switch (type) {
              case INT32:
                dataOutputStream.writeInt(field.getIntV());
                valueOccupation[k] += 4;
                break;
              case INT64:
                dataOutputStream.writeLong(field.getLongV());
                valueOccupation[k] += 8;
                break;
              case FLOAT:
                dataOutputStream.writeFloat(field.getFloatV());
                valueOccupation[k] += 4;
                break;
              case DOUBLE:
                dataOutputStream.writeDouble(field.getDoubleV());
                valueOccupation[k] += 8;
                break;
              case BOOLEAN:
                dataOutputStream.writeBoolean(field.getBoolV());
                valueOccupation[k] += 1;
                break;
              case TEXT:
                dataOutputStream.writeInt(field.getBinaryV().getLength());
                dataOutputStream.write(field.getBinaryV().getValues());
                valueOccupation[k] = valueOccupation[k] + 4 + field.getBinaryV().getLength();
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }
        }
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int j = 0; j < bitmap.length; j++) {
            DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (j + 1)];
            dataBitmapOutputStream.writeByte(bitmap[j]);
            // we should clear the bitmap every 8 row record
            bitmap[j] = 0;
          }
        }
      } else {
        break;
      }
    }

    // feed the remaining bitmap
    int remaining = rowCount % 8;
    if (remaining != 0) {
      for (int j = 0; j < bitmap.length; j++) {
        DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (j + 1)];
        dataBitmapOutputStream.writeByte(bitmap[j] << (8 - remaining));
      }
    }

    // calculate the time buffer size
    int timeOccupation = rowCount * 8;
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeOccupation);
    timeBuffer.put(byteArrayOutputStreams[0].toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    // calculate the bitmap buffer size
    int bitmapOccupation = rowCount / 8 + (rowCount % 8 == 0 ? 0 : 1);

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

  public static Pair<TSQueryDataSet, Boolean> convertTsBlockByFetchSize(
      IQueryExecution queryExecution, int fetchSize) throws IOException, IoTDBException {
    boolean finished = false;
    int columnNum = queryExecution.getOutputValueColumnCount();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
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
    return new Pair<>(tsQueryDataSet, finished);
  }

  /** pair.left is serialized TsBlock pair.right indicates if the query finished */
  // To fetch required amounts of data and combine them through List
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

  public static BitMap[] readBitMapsFromBuffer(ByteBuffer buffer, int columns, int size) {
    if (!buffer.hasRemaining()) {
      return null;
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
    return bitMaps;
  }

  public static BitMap[] readBitMapsFromStream(DataInputStream stream, int columns, int size)
      throws IOException {
    if (stream.available() <= 0) {
      return null;
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
    return bitMaps;
  }

  public static Object[] readTabletValuesFromBuffer(
      ByteBuffer buffer, List<Integer> types, int columns, int size) {
    TSDataType[] dataTypes = new TSDataType[types.size()];
    for (int i = 0; i < dataTypes.length; i++) {
      dataTypes[i] = TSDataType.values()[types.get(i)];
    }
    return readTabletValuesFromBuffer(buffer, dataTypes, columns, size);
  }

  public static Object[] readTabletValuesFromStream(
      DataInputStream stream, List<Integer> types, int columns, int size) throws IOException {
    TSDataType[] dataTypes = new TSDataType[types.size()];
    for (int i = 0; i < dataTypes.length; i++) {
      dataTypes[i] = TSDataType.values()[types.get(i)];
    }
    return readTabletValuesFromStream(stream, dataTypes, columns, size);
  }

  /**
   * @param buffer data values
   * @param columns column number
   * @param size value count in each column
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
          boolean[] boolValues = new boolean[size];
          for (int index = 0; index < size; index++) {
            boolValues[index] = BytesUtils.byteToBool(stream.readByte());
          }
          values[i] = boolValues;
          break;
        case INT32:
          int[] intValues = new int[size];
          for (int index = 0; index < size; index++) {
            intValues[index] = stream.readInt();
          }
          values[i] = intValues;
          break;
        case INT64:
          long[] longValues = new long[size];
          for (int index = 0; index < size; index++) {
            longValues[index] = stream.readLong();
          }
          values[i] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[size];
          for (int index = 0; index < size; index++) {
            floatValues[index] = stream.readFloat();
          }
          values[i] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[size];
          for (int index = 0; index < size; index++) {
            doubleValues[index] = stream.readDouble();
          }
          values[i] = doubleValues;
          break;
        case TEXT:
          Binary[] binaryValues = new Binary[size];
          for (int index = 0; index < size; index++) {
            int binarySize = stream.readInt();
            byte[] binaryValue = new byte[binarySize];
            stream.read(binaryValue);
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
}
