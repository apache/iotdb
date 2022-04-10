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
package org.apache.iotdb.db.wal.checkpoint;

/** Type of {@link Checkpoint} */
public enum CheckpointType {
  /** record all existing memtables' info */
  GLOBAL_MEMORY_TABLE_INFO((byte) 0),
  /** record create info of one memtable */
  CREATE_MEMORY_TABLE((byte) 1),
  /** record flush info of one memtable */
  FLUSH_MEMORY_TABLE((byte) 2),
  ;

  private final byte code;

  CheckpointType(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return code;
  }

  public static CheckpointType valueOf(byte code) {
    for (CheckpointType type : CheckpointType.values()) {
      if (type.code == code) {
        return type;
      }
    }
    return null;
  }
}
