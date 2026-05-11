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

package org.apache.iotdb.commons.queryengine.common;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum SqlDialect {
  TREE((byte) 0),
  TABLE((byte) 1);

  private final byte dialect;

  SqlDialect(byte dialect) {
    this.dialect = dialect;
  }

  public byte getDialect() {
    return dialect;
  }

  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(dialect, stream);
  }

  public void serialize(final ByteBuffer buffer) {
    ReadWriteIOUtils.write(dialect, buffer);
  }

  public static SqlDialect deserializeFrom(final ByteBuffer buffer) {
    byte b = ReadWriteIOUtils.readByte(buffer);
    switch (b) {
      case 0:
        return TREE;
      case 1:
        return TABLE;
      default:
        throw new IllegalArgumentException(String.format("Unknown sql dialect: %s", b));
    }
  }
}
