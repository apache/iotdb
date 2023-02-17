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

package org.apache.iotdb.trigger.api.enums;

public enum TriggerType {
  STATEFUL((byte) 0, "STATEFUL"),
  STATELESS((byte) 1, "STATELESS");

  private final byte id;
  private final String type;

  TriggerType(byte id, String type) {
    this.id = id;
    this.type = type;
  }

  public byte getId() {
    return id;
  }

  @Override
  public String toString() {
    return type;
  }

  public static TriggerType construct(byte id) {
    switch (id) {
      case 0:
        return STATEFUL;
      case 1:
        return STATELESS;
      default:
        throw new IllegalArgumentException(String.format("No such trigger type (id: %d)", id));
    }
  }
}
