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

import java.util.List;

public class Ts2DiffColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;

  public Ts2DiffColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = getEncoder(dataType, TSEncoding.RLE);
  }

  @Override
  public byte[] encode(List<?> data) {
    return new byte[0];
  }

  @Override
  public TSDataType getDataType() {
    return null;
  }

  @Override
  public TSEncoding getEncodingType() {
    return null;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return null;
  }

  @Override
  public ColumnEntry getColumnEntry() {
    return null;
  }
}
