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
package org.apache.iotdb.commons.sync.pipe;

public enum PipeStatus {

  // a new pipe should be stop status
  RUNNING((byte) 0),
  STOP((byte) 1),
  DROP((byte) 2),
  // intermediate states that are not visible to the user
  PARTIAL_CREATE((byte) 3),
  PARTIAL_START((byte) 4),
  PARTIAL_STOP((byte) 5);

  private final byte type;

  PipeStatus(byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }

  public static PipeStatus getPipeStatus(byte type) {
    switch (type) {
      case 0:
        return PipeStatus.RUNNING;
      case 1:
        return PipeStatus.STOP;
      case 2:
        return PipeStatus.DROP;
      case 3:
        return PipeStatus.PARTIAL_CREATE;
      case 4:
        return PipeStatus.PARTIAL_START;
      case 5:
        return PipeStatus.PARTIAL_STOP;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }
}
