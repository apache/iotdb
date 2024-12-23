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

package org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum PipeConsensusRequestType {

  // Data region
  TRANSFER_TABLET_INSERT_NODE((short) 100),
  // PipeConsensus doesn't expect to handle rawTabletEvent.
  TRANSFER_TS_FILE_PIECE((short) 101),
  TRANSFER_TS_FILE_SEAL((short) 102),
  TRANSFER_TABLET_BATCH((short) 103),
  TRANSFER_TABLET_BINARY((short) 104),
  TRANSFER_TS_FILE_PIECE_WITH_MOD((short) 105),
  TRANSFER_TS_FILE_SEAL_WITH_MOD((short) 106),
  TRANSFER_DELETION((short) 107),

// Note: temporarily PipeConsensus only support data region. But we put this class in `node-common`
// to reserve the scalability
;

  private final short type;

  PipeConsensusRequestType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  private static final Map<Short, PipeConsensusRequestType> TYPE_MAP =
      Arrays.stream(PipeConsensusRequestType.values())
          .collect(
              HashMap::new,
              (typeMap, PipeConsensusRequestType) ->
                  typeMap.put(PipeConsensusRequestType.getType(), PipeConsensusRequestType),
              HashMap::putAll);

  public static boolean isValidatedRequestType(short type) {
    return TYPE_MAP.containsKey(type);
  }

  public static PipeConsensusRequestType valueOf(short type) {
    return TYPE_MAP.get(type);
  }
}
