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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.Constants;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class HardStateTest {

  @Test
  public void testHardState() {
    // Not NULL
    HardState state = new HardState();
    state.setCurrentTerm(2);
    state.setVoteFor(new Node("127.0.0.1", 30000, 0, 40000, Constants.RPC_PORT, "127.0.0.1"));
    ByteBuffer buffer = state.serialize();
    HardState newState = HardState.deserialize(buffer);
    assertEquals(state, newState);

    // NULL
    state.setVoteFor(null);
    buffer = state.serialize();
    newState = HardState.deserialize(buffer);
    assertEquals(state, newState);
  }
}
