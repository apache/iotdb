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
package org.apache.iotdb.session.rpccompress.encoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;

import java.io.ByteArrayOutputStream;

/** Column encoder interface, which defines the encoding and decoding operations of column data */
public interface ColumnEncoder {

  void encode(boolean[] values, ByteArrayOutputStream out);

  void encode(short[] values, ByteArrayOutputStream out);

  void encode(int[] values, ByteArrayOutputStream out);

  void encode(long[] values, ByteArrayOutputStream out);

  void encode(float[] values, ByteArrayOutputStream out);

  void encode(double[] values, ByteArrayOutputStream out);

  void encode(Binary[] values, ByteArrayOutputStream out);

  TSDataType getDataType();

  TSEncoding getEncodingType();

  Encoder getEncoder(TSDataType type, TSEncoding encodingType);

  ColumnEntry getColumnEntry();
}
