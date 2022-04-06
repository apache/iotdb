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

package org.apache.iotdb.commons.consensus;

import java.nio.ByteBuffer;
import java.util.Objects;

// TODO Use a mature IDL framework such as Protobuf to manage this structure
public class ConsensusGroupId {

  private GroupType type;
  private int id;

  public ConsensusGroupId() {}

  public ConsensusGroupId(GroupType type, int id) {
    this.type = type;
    this.id = id;
  }

  public GroupType getType() {
    return type;
  }

  public int getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsensusGroupId that = (ConsensusGroupId) o;
    return id == that.id && Objects.equals(type, that.type);
  }

  public void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(type.ordinal());
    buffer.putInt(id);
  }

  public void deserializeImpl(ByteBuffer buffer) {
    int ordinal = buffer.getInt();
    // TODO: (xingtanzjr) should we add validation for the ordinal ?
    type = GroupType.values()[ordinal];
    id = buffer.getInt();
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public String toString() {
    return String.format("ConsensusGroupId[%s]-%s", type, id);
  }
}
