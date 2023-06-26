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
package org.apache.iotdb.commons.schema.view;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum ViewType {
  /** BASE */
  BASE((byte) 0),
  /** VIEW */
  VIEW((byte) 1);

  private final byte type;

  ViewType(byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }

  public static ViewType deserialize(byte type) {
    return getViewType(type);
  }

  public static ViewType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.get());
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(getType());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.write(getType());
  }

  public static ViewType getViewType(byte type) {
    switch (type) {
      case 0:
        return ViewType.BASE;
      case 1:
        return ViewType.VIEW;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }
}
