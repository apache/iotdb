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

package org.apache.iotdb.commons.udf;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum UDFType {
  ILLEGAL((byte) -1),
  TREE_EXTERNAL((byte) 0),
  TREE_BUILT_IN((byte) 1),
  TREE_UNAVAILABLE((byte) 2),
  TABLE_EXTERNAL((byte) 3),
  TABLE_UNAVAILABLE((byte) 4);

  private final byte type;

  UDFType(byte type) {
    this.type = type;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type, stream);
  }

  public static UDFType deserialize(ByteBuffer buffer) {
    byte type = ReadWriteIOUtils.readByte(buffer);
    for (UDFType udfType : UDFType.values()) {
      if (udfType.type == type) {
        return udfType;
      }
    }
    return ILLEGAL;
  }

  public boolean isTreeModel() {
    return this == TREE_EXTERNAL || this == TREE_BUILT_IN || this == TREE_UNAVAILABLE;
  }

  public boolean isTableModel() {
    return this == TABLE_EXTERNAL || this == TABLE_UNAVAILABLE;
  }

  public boolean isAvailable() {
    return this == TREE_EXTERNAL || this == TREE_BUILT_IN || this == TABLE_EXTERNAL;
  }
}
