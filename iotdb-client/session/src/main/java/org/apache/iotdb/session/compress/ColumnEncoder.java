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
package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

/** Column encoder interface, which defines the encoding and decoding operations of column data */
public interface ColumnEncoder {

  /**
   * Encode a column of data
   *
   * @param data The data column to be encoded
   * @return Encoded data
   */
  byte[] encode(List<?> data);

  TSDataType getDataType();

  TSEncoding getEncodingType();

  Encoder getEncoder(TSDataType type, TSEncoding encodingType);

  ColumnEntry getColumnEntry();
}

/** Encoding type not supported exception */
class EncodingTypeNotSupportedException extends RuntimeException {
  public EncodingTypeNotSupportedException(String message) {
    super("Encoding type " + message + " is not supported.");
  }
}
