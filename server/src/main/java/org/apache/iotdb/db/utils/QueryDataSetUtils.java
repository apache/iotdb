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

import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/** TimeValuePairUtils to convert between thrift format and TsFile format. */
public class QueryDataSetUtils {

  private static final int flag = 0x01;

  private QueryDataSetUtils() {}

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static TSQueryDataSet convertQueryDataSetByFetchSize(
      QueryDataSet queryDataSet, int fetchSize, WatermarkEncoder watermarkEncoder)
      throws IOException {
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    int columnNum = dataTypes.size();
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
        if ((queryDataSet.isWithoutAllNull() && rowRecord.isAllNull())
            || (queryDataSet.isWithoutAnyNull() && rowRecord.hasNullField())) {
          // if the current RowRecord doesn't satisfy, we should also decrease AlreadyReturnedRowNum
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
            bitmap[k] = (bitmap[k] << 1) | flag;
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

  public static long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    long[] times = new long[size];
    for (int i = 0; i < size; i++) {
      times[i] = buffer.getLong();
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

  public static Object[] readValuesFromBuffer(
      ByteBuffer buffer, List<Integer> types, int columns, int size) {
    TSDataType[] dataTypes = new TSDataType[types.size()];
    for (int i = 0; i < dataTypes.length; i++) {
      dataTypes[i] = TSDataType.values()[types.get(i)];
    }
    return readValuesFromBuffer(buffer, dataTypes, columns, size);
  }

  /**
   * @param buffer data values
   * @param columns column number
   * @param size value count in each column
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Object[] readValuesFromBuffer(
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
}
