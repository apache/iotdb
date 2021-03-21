/*
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

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.record.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;

public class SessionUtils {

  public static ByteBuffer getTimeBuffer(Tablet tablet) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(tablet.getTimeBytesSize());
    for (int i = 0; i < tablet.rowSize; i++) {
      timeBuffer.putLong(tablet.timestamps[i]);
    }
    timeBuffer.flip();
    return timeBuffer;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ByteBuffer getValueBuffer(Tablet tablet) {
    ByteBuffer valueBuffer = ByteBuffer.allocate(tablet.getValueBytesSize());
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema instanceof MeasurementSchema) {
        getValueBufferOfDataType(schema.getType(), tablet, i, valueBuffer);
      } else {
        for (int j = 0; j < schema.getValueTSDataTypeList().size(); j++) {
          getValueBufferOfDataType(schema.getValueTSDataTypeList().get(j), tablet, j, valueBuffer);
        }
      }
    }
    valueBuffer.flip();
    return valueBuffer;
  }

  private static void getValueBufferOfDataType(
      TSDataType dataType, Tablet tablet, int i, ByteBuffer valueBuffer) {
    BitMap curBitMap = tablet.bitMaps[i];
    long[] curLongs = curBitMap.getLongArray();
    for (int j = 0; j < curLongs.length; j++) {
      valueBuffer.putLong(curLongs[j]);
    }

    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.putInt(intValues[index]);
          } else {
            valueBuffer.putInt(-1);
          }
        }
        break;
      case INT64:
        long[] longValues = (long[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.putLong(longValues[index]);
          } else {
            valueBuffer.putLong(-1);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.putFloat(floatValues[index]);
          } else {
            valueBuffer.putFloat(-1);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.putDouble(doubleValues[index]);
          } else {
            valueBuffer.putDouble(-1);
          }
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.put(BytesUtils.boolToByte(boolValues[index]));
          } else {
            valueBuffer.put(BytesUtils.boolToByte(false));
          }
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (curBitMap.get(index)) {
            valueBuffer.putInt(binaryValues[index].getLength());
            valueBuffer.put(binaryValues[index].getValues());
          } else {
            Binary emptyStr = new Binary(".");
            valueBuffer.putInt(emptyStr.getLength());
            valueBuffer.put(emptyStr.getValues());
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }
}
