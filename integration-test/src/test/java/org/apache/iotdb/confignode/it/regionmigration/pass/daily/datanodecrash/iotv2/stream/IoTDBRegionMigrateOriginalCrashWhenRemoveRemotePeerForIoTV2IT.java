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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.datanodecrash.iotv2.stream;

import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusInactivatePeerKillPoints;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateDataNodeCrashITFrameworkForIoTV2;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateOriginalCrashWhenRemoveRemotePeerForIoTV2IT
    extends IoTDBRegionMigrateDataNodeCrashITFrameworkForIoTV2 {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setIoTConsensusV2Mode(ConsensusFactory.IOT_CONSENSUS_V2_STREAM_MODE);
  }

  @Test
  public void crashBeforeInactivate() throws Exception {
    success(IoTConsensusInactivatePeerKillPoints.BEFORE_INACTIVATE);
  }

  @Test
  public void crashAfterInactivate() throws Exception {
    success(IoTConsensusInactivatePeerKillPoints.AFTER_INACTIVATE);
  }
}
