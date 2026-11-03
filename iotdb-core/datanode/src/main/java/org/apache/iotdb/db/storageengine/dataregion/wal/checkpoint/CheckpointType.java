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

package org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

/** Type of {@link Checkpoint}. */
public enum CheckpointType {
  // record all existing memTables info
  GLOBAL_MEMORY_TABLE_INFO(
      (byte) 0, StorageEngineMessages.EXCEPTION_GLOBAL_MEMORY_TABLE_INFO_D31FC987),
  // record create info of one memTable
  CREATE_MEMORY_TABLE((byte) 1, StorageEngineMessages.EXCEPTION_CREATE_MEMORY_TABLE_C9A458F5),
  // record flush info of one memTable
  FLUSH_MEMORY_TABLE((byte) 2, StorageEngineMessages.EXCEPTION_FLUSH_MEMORY_TABLE_F5383277);

  private final byte code;
  private final String name;

  CheckpointType(byte code, String name) {
    this.code = code;
    this.name = name;
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

  @Override
  public String toString() {
    return name;
  }
}
