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

package org.apache.iotdb.tsfile.read.common.block.column;

import java.util.HashMap;
import java.util.Map;

public class ColumnEncoderFactory {

  private static final Map<ColumnEncoding, ColumnEncoder> encodingToEncoder = new HashMap<>();

  static {
    encodingToEncoder.put(ColumnEncoding.INT32_ARRAY, new Int32ArrayColumnEncoder());
    encodingToEncoder.put(ColumnEncoding.INT64_ARRAY, new Int64ArrayColumnEncoder());
    encodingToEncoder.put(ColumnEncoding.BYTE_ARRAY, new ByteArrayColumnEncoder());
    encodingToEncoder.put(ColumnEncoding.BINARY_ARRAY, new BinaryArrayColumnEncoder());
    encodingToEncoder.put(ColumnEncoding.RLE, new RunLengthColumnEncoder());
  }

  public static ColumnEncoder get(ColumnEncoding columnEncoding) {
    if (!encodingToEncoder.containsKey(columnEncoding)) {
      throw new IllegalArgumentException("Unsupported column encoding: " + columnEncoding);
    }
    return encodingToEncoder.get(columnEncoding);
  }
}
