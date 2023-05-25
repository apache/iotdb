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

package org.apache.iotdb.commons.consensus.index.impl;

import org.apache.iotdb.commons.consensus.index.ConsensusIndex;
import org.apache.iotdb.commons.consensus.index.ConsensusIndexType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MinimumConsensusIndex implements ConsensusIndex {
  public MinimumConsensusIndex() {}

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ConsensusIndexType.MINIMUM_CONSENSUS_INDEX.serialize(byteBuffer);
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    ConsensusIndexType.MINIMUM_CONSENSUS_INDEX.serialize(stream);
  }

  @Override
  public boolean isAfter(ConsensusIndex consensusIndex) {
    return false;
  }

  @Override
  public boolean equals(ConsensusIndex consensusIndex) {
    return consensusIndex instanceof MinimumConsensusIndex;
  }

  @Override
  public ConsensusIndex updateToMaximum(ConsensusIndex consensusIndex) {
    return consensusIndex == null ? this : consensusIndex;
  }

  public static MinimumConsensusIndex deserializeFrom(ByteBuffer byteBuffer) {
    return new MinimumConsensusIndex();
  }

  public static MinimumConsensusIndex deserializeFrom(InputStream stream) {
    return new MinimumConsensusIndex();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    return obj != null && getClass() == obj.getClass();
  }

  @Override
  public String toString() {
    return "MinimumConsensusIndex{}";
  }
}
