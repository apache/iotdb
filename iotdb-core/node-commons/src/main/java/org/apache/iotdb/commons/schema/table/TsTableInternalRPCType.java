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
  PRE_CREATE_OR_ADD_COLUMN((byte) 0),
  ROLLBACK_CREATE_OR_ADD_COLUMN((byte) 1),
  COMMIT_CREATE_OR_ADD_COLUMN((byte) 2);

  private final byte operationType;

  private TsTableInternalRPCType(final byte operationType) {
    this.operationType = operationType;
  }

  public byte getOperationType() {
    return operationType;
  }

  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(operationType, stream);
  }

  public static TsTableInternalRPCType deserialize(final ByteBuffer buffer) {
    byte type = ReadWriteIOUtils.readByte(buffer);
    return getType(type);
  }

  public static TsTableInternalRPCType getType(final byte type) {
    switch (type) {
      case 0:
        return PRE_CREATE_OR_ADD_COLUMN;
      case 1:
        return ROLLBACK_CREATE_OR_ADD_COLUMN;
      case 2:
        return COMMIT_CREATE_OR_ADD_COLUMN;
      default:
        throw new IllegalArgumentException("Unknown table update operation type" + type);
    }
  }
}
