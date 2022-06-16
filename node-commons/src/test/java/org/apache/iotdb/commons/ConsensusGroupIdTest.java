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

package org.apache.iotdb.commons;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ConsensusGroupIdTest {
  @Test
  public void TestCreate() throws IOException {
    ConsensusGroupId dataRegionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    Assert.assertTrue(dataRegionId instanceof DataRegionId);
    Assert.assertEquals(1, dataRegionId.getId());
    Assert.assertEquals(TConsensusGroupType.DataRegion, dataRegionId.getType());

    ConsensusGroupId schemaRegionId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2));
    Assert.assertTrue(schemaRegionId instanceof SchemaRegionId);
    Assert.assertEquals(2, schemaRegionId.getId());
    Assert.assertEquals(TConsensusGroupType.SchemaRegion, schemaRegionId.getType());
  }
}
