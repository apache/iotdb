/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
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

  private QueryDataSetUtils(){}

  /**
   * convert query data set by fetch size.
   *
   * @param queryDataSet -query dataset
   * @param fetchSize -fetch size
   * @return -convert query dataset
   */
  public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet,
      int fetchSize)
      throws IOException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    tsQueryDataSet.setRecords(new ArrayList<>());
    for (int i = 0; i < fetchSize; i++) {
      if (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        tsQueryDataSet.getRecords().add(convertToTSRecord(rowRecord));
      } else {
        break;
      }
    }
    return tsQueryDataSet;
  }

  /**
   * convert to tsRecord.
   *
   * @param rowRecord -row record
   */
  private static TSRowRecord convertToTSRecord(RowRecord rowRecord) {
    TSRowRecord tsRowRecord = new TSRowRecord();
    tsRowRecord.setTimestamp(rowRecord.getTimestamp());
    tsRowRecord.setValues(new ArrayList<>());
    List<Field> fields = rowRecord.getFields();
    for (Field f : fields) {
      TSDataValue value = new TSDataValue(false);
      if (f.getDataType() == null) {
        value.setIs_empty(true);
      } else {
        switch (f.getDataType()) {
          case BOOLEAN:
            value.setBool_val(f.getBoolV());
            break;
          case INT32:
            value.setInt_val(f.getIntV());
            break;
          case INT64:
            value.setLong_val(f.getLongV());
            break;
          case FLOAT:
            value.setFloat_val(f.getFloatV());
            break;
          case DOUBLE:
            value.setDouble_val(f.getDoubleV());
            break;
          case TEXT:
            value.setBinary_val(ByteBuffer.wrap(f.getBinaryV().getValues()));
            break;
          default:
            throw new UnSupportedDataTypeException(String.format(
                "data type %s is not supported when convert data at server",
                f.getDataType().toString()));
        }
        value.setType(getIoTDBDataTypeByTSDataType(f.getDataType()));
      }
      tsRowRecord.getValues().add(value);
    }
    return tsRowRecord;
  }

  public static IoTDBDataType getIoTDBDataTypeByTSDataType(TSDataType type) {
    switch (type) {
      case BOOLEAN: return IoTDBDataType.BOOLEAN;
      case FLOAT: return IoTDBDataType.FLOAT;
      case DOUBLE: return IoTDBDataType.DOUBLE;
      case INT32: return IoTDBDataType.INT32;
      case INT64: return IoTDBDataType.INT64;
      case TEXT: return IoTDBDataType.TEXT;
      default: throw new RuntimeException("data type not supported: " + type);
    }
  }

  public static TSDataType getTSDataTypeByIoTDBDataType(IoTDBDataType type) {
    switch (type) {
      case BOOLEAN: return TSDataType.BOOLEAN;
      case FLOAT: return TSDataType.FLOAT;
      case DOUBLE: return TSDataType.DOUBLE;
      case INT32: return TSDataType.INT32;
      case INT64: return TSDataType.INT64;
      case TEXT: return TSDataType.TEXT;
      default: throw new RuntimeException("data type not supported: " + type);
    }
  }

  public static long[] readTimesFromBuffer(ByteBuffer buffer, int size) {
    long[] times = new long[size];
    for (int i = 0; i < size; i++) {
      times[i] = buffer.getLong();
    }
    return times;
  }

  /**
   * @param buffer data values
   * @param columns column number
   * @param size value count in each column
   */
  public static Object[] readValuesFromBuffer(ByteBuffer buffer, List<IoTDBDataType> types,
      int columns, int size) {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      switch (types.get(i)) {
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
                  types.get(i)));
      }
    }
    return values;
  }
}
