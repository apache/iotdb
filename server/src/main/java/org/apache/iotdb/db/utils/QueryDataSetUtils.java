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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

/**
 * TimeValuePairUtils to convert between thrift format and TsFile format.
 */
public class QueryDataSetUtils {

  private QueryDataSetUtils() {
  }

  /**
   * convert query data set by fetch size.
   *
   * @param queryDataSet -query dataset
   * @param fetchSize -fetch size
   * @return -convert query dataset
   */
  public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet,
      int fetchSize) throws IOException {
    return convertQueryDataSetByFetchSize(queryDataSet, fetchSize, null);
  }

  public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet,
      int fetchSize, WatermarkEncoder watermarkEncoder) throws IOException {
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    int columnNum = dataTypes.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    int columnNumWithTime = columnNum + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }

    int rowCount = 0;
    int valueOccupation = 0;
    for (int i = 0; i < fetchSize; i++) {
      if (queryDataSet.hasNext()) {
        rowCount++;
        RowRecord rowRecord = queryDataSet.next();
        if (watermarkEncoder != null) {
          rowRecord = watermarkEncoder.encodeRecord(rowRecord);
        }
        // use columnOutput to write byte array
        dataOutputStreams[0].writeLong(rowRecord.getTimestamp());
        List<Field> fields = rowRecord.getFields();
        for (int k = 0; k < fields.size(); k++) {
          Field field = fields.get(k);
          DataOutputStream dataOutputStream = dataOutputStreams[k + 1]; // DO NOT FORGET +1
          if (field.getDataType() == null) {
            dataOutputStream.writeBoolean(true); // is_empty true
          } else {
            dataOutputStream.writeBoolean(false); // is_empty false
            TSDataType type = field.getDataType();
            switch (type) {
              case INT32:
                dataOutputStream.writeInt(field.getIntV());
                valueOccupation += 4;
                break;
              case INT64:
                dataOutputStream.writeLong(field.getLongV());
                valueOccupation += 8;
                break;
              case FLOAT:
                dataOutputStream.writeFloat(field.getFloatV());
                valueOccupation += 4;
                break;
              case DOUBLE:
                dataOutputStream.writeDouble(field.getDoubleV());
                valueOccupation += 8;
                break;
              case BOOLEAN:
                dataOutputStream.writeBoolean(field.getBoolV());
                valueOccupation += 1;
                break;
              case TEXT:
                dataOutputStream.writeInt(field.getBinaryV().getLength());
                dataOutputStream.write(field.getBinaryV().getValues());
                valueOccupation = valueOccupation + 4 + field.getBinaryV().getLength();
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }
        }
      } else {
        break;
      }
    }

    // calculate total valueOccupation
    valueOccupation += rowCount * 8; // note the timestamp column needn't the boolean is_empty
    valueOccupation += rowCount * dataTypes.size(); // for all is_empty

    ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation);
    for (ByteArrayOutputStream byteArrayOutputStream : byteArrayOutputStreams) {
      valueBuffer.put(byteArrayOutputStream.toByteArray());
    }
    valueBuffer.flip(); // PAY ATTENTION TO HERE
    tsQueryDataSet.setValues(valueBuffer);
    tsQueryDataSet.setRowCount(rowCount);
    return tsQueryDataSet;
  }


  public static long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    long[] times = new long[size];
    for (int i = 0; i < size; i++) {
      times[i] = buffer.getLong();
    }
    return times;
  }


  public static Object[] readValuesFromBuffer(ByteBuffer buffer, List<Integer> types,
      int columns, int size) {
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
  public static Object[] readValuesFromBuffer(ByteBuffer buffer, TSDataType[] types,
      int columns, int size) {
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
              String.format("data type %s is not supported when convert data at client",
                  types[i]));
      }
    }
    return values;
  }
}
