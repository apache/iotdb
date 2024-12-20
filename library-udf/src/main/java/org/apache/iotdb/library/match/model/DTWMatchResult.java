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

package org.apache.iotdb.library.match.model;

import java.nio.ByteBuffer;

public class DTWMatchResult {
  private final float dtwValue;
  private final long startTime;
  private final long endTime;
  public static final int BYTES = Float.BYTES + 2 * Long.BYTES;

  public DTWMatchResult(float dtwValue, long startTime, long endTime) {
    this.dtwValue = dtwValue;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"distance\":%f,\"startTime\":%d,\"endTime\":%d}", dtwValue, startTime, endTime);
  }

  public byte[] toByteArray() {
    ByteBuffer buffer = ByteBuffer.allocate(BYTES);
    buffer.putFloat(dtwValue);
    buffer.putLong(startTime);
    buffer.putLong(endTime);

    return buffer.array();
  }

  public static DTWMatchResult fromByteArray(byte[] byteArray) {
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    return new DTWMatchResult(buffer.getFloat(), buffer.getLong(), buffer.getLong());
  }
}
