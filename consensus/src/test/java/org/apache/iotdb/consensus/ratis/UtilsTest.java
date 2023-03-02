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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;

import org.apache.ratis.protocol.RaftGroupId;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {
  @Test
  public void testEncryption() {
    ConsensusGroupId raw = new ConfigRegionId(100);
    RaftGroupId id = Utils.fromConsensusGroupIdToRaftGroupId(raw);
    ConsensusGroupId cgid = Utils.fromRaftGroupIdToConsensusGroupId(id);
    Assert.assertEquals(raw.getId(), cgid.getId());
    Assert.assertEquals(raw.getType(), cgid.getType());
  }
}
