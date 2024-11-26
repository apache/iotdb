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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.pipe.event.SerializableEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public enum PipeSchemaSerializableEventType {
  SCHEMA_WRITE_PLAN((byte) 1),
  SCHEMA_SNAPSHOT((byte) 2),
  SCHEMA_SNAPSHOT_V2((byte) 3);

  private static final Map<Byte, PipeSchemaSerializableEventType> TYPE_MAP = new HashMap<>();

  static {
    for (final PipeSchemaSerializableEventType type : PipeSchemaSerializableEventType.values()) {
      TYPE_MAP.put(type.getType(), type);
    }
  }

  private final byte type;

  PipeSchemaSerializableEventType(final byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }

  public static PipeSchemaSerializableEventType deserialize(final byte type) {
    return TYPE_MAP.get(type);
  }

  public static SerializableEvent deserialize(final ByteBuffer buffer) throws IOException {
    final byte eventType = buffer.get();
    return deserialize(buffer, eventType);
  }

  public static SerializableEvent deserialize(final ByteBuffer buffer, final byte eventType)
      throws IOException {
    final SerializableEvent event;
    switch (eventType) {
      case 1:
        event = new PipeSchemaRegionWritePlanEvent();
        break;
      case 2:
        event = new PipeSchemaRegionSnapshotEvent(1);
        break;
      case 3:
        event = new PipeSchemaRegionSnapshotEvent(2);
        break;
      default:
        throw new IllegalArgumentException("Invalid event type: " + eventType);
    }
    event.deserializeFromByteBuffer(buffer);
    return event;
  }
}
