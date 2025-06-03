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

public interface ColumnDecoder {

  Object decode(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  boolean[] decodeBooleanColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  int[] decodeIntColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  long[] decodeLongColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  float[] decodeFloatColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  double[] decodeDoubleColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  Binary[] decodeBinaryColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount);

  default Decoder getDecoder(TSDataType type, TSEncoding encodingType) {
    return Decoder.getDecoderByType(encodingType, type);
  }
}
