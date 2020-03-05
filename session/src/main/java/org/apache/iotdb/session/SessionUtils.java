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
import org.apache.iotdb.tsfile.write.record.RowBatch;

import java.nio.ByteBuffer;

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
    for (int i = 0; i < rowBatch.timeseries.size(); i++) {
      TSDataType dataType = rowBatch.timeseries.get(i).getType();
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
}
