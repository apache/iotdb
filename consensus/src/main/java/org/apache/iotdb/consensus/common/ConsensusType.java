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
package org.apache.iotdb.consensus.common;

import java.util.Arrays;

public enum ConsensusType {
  STANDALONE("standalone"),
  RATIS("ratis");

  private final String typeName;

  ConsensusType(String typeName) {
    this.typeName = typeName;
  }

  public String getTypeName() {
    return typeName;
  }

  @Override
  public String toString() {
    return typeName;
  }

  public static ConsensusType getConsensusType(String typeName) {
    for (ConsensusType type : ConsensusType.values()) {
      if (type.getTypeName().equals(typeName)) {
        return type;
      }
    }

    throw new IllegalArgumentException(
        String.format(
            "Unknown consensus type, found: %s expected: %s",
            typeName, Arrays.toString(ConsensusType.values())));
  }
}
