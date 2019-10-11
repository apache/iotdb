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
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
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
   *
   * @param tsQueryDataSet -query data set
   * @return -list of row record
   */
  static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet) {
    List<RowRecord> records = new ArrayList<>();
    for (TSRowRecord ts : tsQueryDataSet.getRecords()) {
      RowRecord r = new RowRecord(ts.getTimestamp());
      int l = ts.getValuesSize();
      for (int i = 0; i < l; i++) {
        TSDataValue value = ts.getValues().get(i);
        if (value.is_empty) {
          Field field = new Field(null);
          field.setNull();
          r.getFields().add(field);
        } else {
          TSDataType dataType = TSDataType.valueOf(value.getType());
          Field field = new Field(dataType);
          addFieldAccordingToDataType(field, dataType, value);
          r.getFields().add(field);
        }
      }
      records.add(r);
    }
    return records;
  }

  /**
   *
   * @param field -the field need to add new data
   * @param dataType, -the data type of the new data
   * @param value, -the value of the new data
   */
  private static void addFieldAccordingToDataType(Field field, TSDataType dataType, TSDataValue value){
    switch (dataType) {
      case BOOLEAN:
        field.setBoolV(value.isBool_val());
        break;
      case INT32:
        field.setIntV(value.getInt_val());
        break;
      case INT64:
        field.setLongV(value.getLong_val());
        break;
      case FLOAT:
        field.setFloatV((float) value.getFloat_val());
        break;
      case DOUBLE:
        field.setDoubleV(value.getDouble_val());
        break;
      case TEXT:
        field.setBinaryV(new Binary(value.getBinary_val()));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("data type %s is not supported when convert data at client",
                dataType));
    }
  }
}
