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

package org.apache.iotdb.db.pipe.connector.v1;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum PipeRequestType {
  HANDSHAKE((short) 1),

  TRANSFER_INSERT_NODE((short) 2),
  TRANSFER_TABLET((short) 3),

  TRANSFER_FILE_PIECE((short) 4),
  TRANSFER_FILE_SEAL((short) 5),
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
