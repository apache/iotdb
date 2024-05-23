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

package org.apache.iotdb.commons.schema.table;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum TsTableInternalRPCType {
  PRE_CREATE((byte) 0),
  ROLLBACK_CREATE((byte) 1),
  COMMIT_CREATE((byte) 2),

  INVALIDATE_CACHE((byte) 3),
  DELETE_DATA_IN_DATA_REGION((byte) 4),
  DELETE_SCHEMA_IN_SCHEMA_REGION((byte) 5),

  ADD_COLUMN((byte) 6),
  ROLLBACK_ADD_COLUMN((byte) 7);

  private final byte operationType;

  private TsTableInternalRPCType(byte operationType) {
    this.operationType = operationType;
  }

  public byte getOperationType() {
    return operationType;
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(operationType, stream);
  }

  public static TsTableInternalRPCType deserialize(ByteBuffer buffer) {
    byte type = ReadWriteIOUtils.readByte(buffer);
    return getType(type);
  }

  public static TsTableInternalRPCType getType(byte type) {
    switch (type) {
      case 0:
        return PRE_CREATE;
      case 1:
        return ROLLBACK_CREATE;
      case 2:
        return COMMIT_CREATE;
      case 3:
        return INVALIDATE_CACHE;
      case 4:
        return DELETE_DATA_IN_DATA_REGION;
      case 5:
        return DELETE_SCHEMA_IN_SCHEMA_REGION;
      case 6:
        return ADD_COLUMN;
      case 7:
        return ROLLBACK_ADD_COLUMN;
      default:
        throw new IllegalArgumentException("Unknown table update operation type" + type);
    }
  }
}
