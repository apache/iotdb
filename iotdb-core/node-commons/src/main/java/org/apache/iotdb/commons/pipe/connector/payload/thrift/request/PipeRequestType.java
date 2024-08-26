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

package org.apache.iotdb.commons.pipe.connector.payload.thrift.request;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum PipeRequestType {

  // Handshake
  HANDSHAKE_CONFIGNODE_V1((short) 0),
  HANDSHAKE_DATANODE_V1((short) 1),
  HANDSHAKE_CONFIGNODE_V2((short) 50),
  HANDSHAKE_DATANODE_V2((short) 51),

  // Data region
  TRANSFER_TABLET_INSERT_NODE((short) 2),
  TRANSFER_TABLET_RAW((short) 3),
  TRANSFER_TS_FILE_PIECE((short) 4),
  TRANSFER_TS_FILE_SEAL((short) 5),
  TRANSFER_TABLET_BATCH((short) 6),
  TRANSFER_TABLET_BINARY((short) 7),
  TRANSFER_TS_FILE_PIECE_WITH_MOD((short) 8),
  TRANSFER_TS_FILE_SEAL_WITH_MOD((short) 9),

  // Schema region
  TRANSFER_SCHEMA_PLAN((short) 100),
  TRANSFER_SCHEMA_SNAPSHOT_PIECE((short) 101),
  TRANSFER_SCHEMA_SNAPSHOT_SEAL((short) 102),

  // Config region
  TRANSFER_CONFIG_PLAN((short) 200),
  TRANSFER_CONFIG_SNAPSHOT_PIECE((short) 201),
  TRANSFER_CONFIG_SNAPSHOT_SEAL((short) 202),

  // RPC Compression
  TRANSFER_COMPRESSED((short) 300),

  // Fallback Handling
  TRANSFER_SLICE((short) 400),
  ;

  private final short type;

  PipeRequestType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, PipeRequestType> TYPE_MAP =
      Arrays.stream(PipeRequestType.values())
          .collect(
              HashMap::new,
              (typeMap, pipeRequestType) -> typeMap.put(pipeRequestType.getType(), pipeRequestType),
              HashMap::putAll);

  public static boolean isValidatedRequestType(short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static PipeRequestType valueOf(short type) {
    return TYPE_MAP.get(type);
  }
}
