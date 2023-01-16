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

import java.io.IOException;

public enum ConsensusProtocolClass {
  SIMPLE_CONSENSUS("org.apache.iotdb.consensus.simple.SimpleConsensus"),
  RATIS_CONSENSUS("org.apache.iotdb.consensus.ratis.RatisConsensus"),
  IOT_CONSENSUS("org.apache.iotdb.consensus.iot.IoTConsensus");

  private final String protocol;

  ConsensusProtocolClass(String protocol) {
    this.protocol = protocol;
  }

  public String getProtocol() {
    return protocol;
  }

  public static ConsensusProtocolClass parse(String protocol) throws IOException {
    for (ConsensusProtocolClass consensusProtocolClass : ConsensusProtocolClass.values()) {
      if (consensusProtocolClass.protocol.equals(protocol)) {
        return consensusProtocolClass;
      }
    }
    throw new IOException(String.format("ConsensusProtocolClass %s doesn't exist.", protocol));
  }
}
