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

package org.apache.iotdb.db.it.iotconsensusv2.stream;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.iotconsensusv2.IoTDBIoTConsensusV23C3DBasicITBase;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/** IoTConsensusV2 3C3D integration test with stream mode. */
@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBIoTConsensusV2Stream3C3DBasicIT extends IoTDBIoTConsensusV23C3DBasicITBase {

  @Override
  protected String getIoTConsensusV2Mode() {
    return ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE;
  }

  @Override
  @Test
  public void testReplicaConsistencyAfterLeaderStop() throws Exception {
    super.testReplicaConsistencyAfterLeaderStop();
  }

  @Override
  @Test
  public void test3C3DWriteFlushAndQuery() throws Exception {
    super.test3C3DWriteFlushAndQuery();
  }
}
