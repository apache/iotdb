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
package org.apache.iotdb.session.rpccompress.decoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;

import java.nio.ByteBuffer;

public class PlainColumnDecoder implements ColumnDecoder {
  private final Decoder decoder;
  private final TSDataType dataType;

  public PlainColumnDecoder(TSDataType dataType) {
    this.dataType = dataType;
    this.decoder = getDecoder(dataType, TSEncoding.PLAIN);
  }

  @Override
  public Object decode(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    switch (dataType) {
      case BOOLEAN:
        {
          boolean[] result = new boolean[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readBoolean(buffer);
          }
          return result;
        }
      case INT32:
      case DATE:
        {
          int[] result = new int[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readInt(buffer);
          }
          return result;
        }
      case INT64:
      case TIMESTAMP:
        {
          long[] result = new long[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readLong(buffer);
          }
          return result;
        }
      case FLOAT:
        {
          float[] result = new float[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readFloat(buffer);
          }
          return result;
        }
      case DOUBLE:
        {
          double[] result = new double[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readDouble(buffer);
          }
          return result;
        }
      case TEXT:
      case STRING:
      case BLOB:
        {
          Binary[] result = new Binary[rowCount];
          for (int i = 0; i < rowCount; i++) {
            result[i] = decoder.readBinary(buffer);
          }
          return result;
        }
      default:
        throw new UnsupportedOperationException("PLAIN doesn't support data type: " + dataType);
    }
  }

  @Override
  public boolean[] decodeBooleanColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    boolean[] result = new boolean[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readBoolean(buffer);
    }
    return result;
  }

  @Override
  public int[] decodeIntColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    int[] result = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readInt(buffer);
    }
    return result;
  }

  @Override
  public long[] decodeLongColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    long[] result = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readLong(buffer);
    }
    return result;
  }

  @Override
  public float[] decodeFloatColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    float[] result = new float[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readFloat(buffer);
    }
    return result;
  }

  @Override
  public double[] decodeDoubleColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    double[] result = new double[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readDouble(buffer);
    }
    return result;
  }

  @Override
  public Binary[] decodeBinaryColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    Binary[] result = new Binary[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readBinary(buffer);
    }
    return result;
  }
}
