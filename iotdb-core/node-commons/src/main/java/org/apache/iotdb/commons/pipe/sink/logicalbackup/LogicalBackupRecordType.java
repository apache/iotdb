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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import java.util.HashMap;
import java.util.Map;

public enum LogicalBackupRecordType {
  EVENT_BEGIN((byte) 1),
  PIPE_REQUEST((byte) 2),
  EVENT_COMMIT((byte) 3),
  HEARTBEAT((byte) 4),
  SKIPPED_EVENT((byte) 5),
  STREAM_END((byte) 6);

  private static final Map<Byte, LogicalBackupRecordType> TYPE_MAP = new HashMap<>();

  static {
    for (final LogicalBackupRecordType type : values()) {
      TYPE_MAP.put(type.code, type);
    }
  }

  private final byte code;

  LogicalBackupRecordType(final byte code) {
    this.code = code;
  }

  public byte getCode() {
    return code;
  }

  public static LogicalBackupRecordType valueOf(final byte code) {
    return TYPE_MAP.get(code);
  }
}
