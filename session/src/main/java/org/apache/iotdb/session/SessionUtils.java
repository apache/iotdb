/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.record.RowBatch;

public class SessionUtils {

  public static ByteBuffer getTimeBuffer(RowBatch rowBatch) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(rowBatch.getTimeBytesSize());
    for (int i = 0; i < rowBatch.batchSize; i++) {
      timeBuffer.putLong(rowBatch.timestamps[i]);
    }
    timeBuffer.flip();
    return timeBuffer;
  }

  public static ByteBuffer getValueBuffer(RowBatch rowBatch) {
    ByteBuffer valueBuffer = ByteBuffer.allocate(rowBatch.getValueBytesSize());
    for (int i = 0; i < rowBatch.measurements.size(); i++) {
      TSDataType dataType = rowBatch.measurements.get(i).getType();
      switch (dataType) {
        case INT32:
          int[] intValues = (int[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.putInt(intValues[index]);
          }
          break;
        case INT64:
          long[] longValues = (long[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.putLong(longValues[index]);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.putFloat(floatValues[index]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.putDouble(doubleValues[index]);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.put(BytesUtils.boolToByte(boolValues[index]));
          }
          break;
        case TEXT:
          Binary[] binaryValues = (Binary[]) rowBatch.values[i];
          for (int index = 0; index < rowBatch.batchSize; index++) {
            valueBuffer.putInt(binaryValues[index].getLength());
            valueBuffer.put(binaryValues[index].getValues());
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
    valueBuffer.flip();
    return valueBuffer;
  }


  /**
   * convert row records.
   */
  static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet,
      List<String> columnTypeList) {
    int rowCount = tsQueryDataSet.getRowCount();
    ByteBuffer byteBuffer = tsQueryDataSet.bufferForValues();

    // process time buffer
    List<RowRecord> rowRecordList = processTimeAndCreateRowRecords(byteBuffer, rowCount);

    for (String type : columnTypeList) {
      for (int i = 0; i < rowCount; i++) {
        Field field = null;
        boolean is_empty = BytesUtils.byteToBool(byteBuffer.get());
        if (is_empty) {
          field = new Field(null);
          field.setNull();
        } else {
          TSDataType dataType = TSDataType.valueOf(type);
          field = new Field(dataType);
          switch (dataType) {
            case BOOLEAN:
              boolean booleanValue = BytesUtils.byteToBool(byteBuffer.get());
              field.setBoolV(booleanValue);
              break;
            case INT32:
              int intValue = byteBuffer.getInt();
              field.setIntV(intValue);
              break;
            case INT64:
              long longValue = byteBuffer.getLong();
              field.setLongV(longValue);
              break;
            case FLOAT:
              float floatValue = byteBuffer.getFloat();
              field.setFloatV(floatValue);
              break;
            case DOUBLE:
              double doubleValue = byteBuffer.getDouble();
              field.setDoubleV(doubleValue);
              break;
            case TEXT:
              int binarySize = byteBuffer.getInt();
              byte[] binaryValue = new byte[binarySize];
              byteBuffer.get(binaryValue);
              field.setBinaryV(new Binary(binaryValue));
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
        rowRecordList.get(i).getFields().add(field);
      }
    }
    return rowRecordList;
  }

  private static List<RowRecord> processTimeAndCreateRowRecords(ByteBuffer byteBuffer,
      int rowCount) {
    List<RowRecord> rowRecordList = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      long timestamp = byteBuffer.getLong(); // byteBuffer has been flipped by the server side
      RowRecord rowRecord = new RowRecord(timestamp);
      rowRecordList.add(rowRecord);
    }
    return rowRecordList;
  }
}
