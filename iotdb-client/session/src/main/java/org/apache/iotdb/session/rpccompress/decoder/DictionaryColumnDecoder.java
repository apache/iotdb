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
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class DictionaryColumnDecoder implements ColumnDecoder {
  private final Decoder decoder;
  private final TSDataType dataType;

  public DictionaryColumnDecoder(TSDataType dataType) {
    this.dataType = dataType;
    this.decoder = getDecoder(dataType, TSEncoding.DICTIONARY);
  }

  @Override
  public Object decode(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    return decodeBinaryColumn(buffer, columnEntry, rowCount);
  }

  @Override
  public boolean[] decodeBooleanColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Dictionary doesn't support data type: " + dataType);
  }

  @Override
  public int[] decodeIntColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Dictionary doesn't support data type: " + dataType);
  }

  @Override
  public long[] decodeLongColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Dictionary doesn't support data type: " + dataType);
  }

  @Override
  public float[] decodeFloatColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Dictionary doesn't support data type: " + dataType);
  }

  @Override
  public double[] decodeDoubleColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Dictionary doesn't support data type: " + dataType);
  }

  @Override
  public Binary[] decodeBinaryColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    Binary[] result = new Binary[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readBinary(buffer);
    }
    return result;
  }

  @Override
  public Decoder getDecoder(TSDataType type, TSEncoding encodingType) {
    return Decoder.getDecoderByType(encodingType, type);
  }
}
