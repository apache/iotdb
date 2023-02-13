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

package org.apache.iotdb.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;

public class DoubleRAKEEncoder extends RAKEEncoder {

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    isReadFinish = false;
    String bit_value = Long.toBinaryString(Double.doubleToLongBits(value));
    encodeNumber(bit_value, 64, out);
  }

  @Override
  public int getOneItemMaxSize() {
    return 1 + (1 + 1) * Double.BYTES;
  }

  @Override
  public long getMaxByteSize() {
    return 1 + (long) (1 + groupNum) * Double.BYTES;
  }
}
