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

package org.apache.iotdb.confignode.it.regionmigration.pass.datanodecrash;

import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusDeleteLocalPeerKillPoints;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.platform.commons.function.Try.success;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
<<<<<<<< HEAD:integration-test/src/test/java/org/apache/iotdb/confignode/it/regionmigration/pass/datanodecrash/IoTDBRegionMigrateOriginalCrashWhenDeleteLocalPeerIT.java
public class IoTDBRegionMigrateOriginalCrashWhenDeleteLocalPeerIT
    extends IoTDBRegionOperationReliabilityITFramework {
========
public class IoTDBRegionMigrateOriginalCrashWhenDeleteLocalPeerForIoTV2BatchIT
    extends IoTDBRegionMigrateDataNodeCrashITFrameworkForIoTV2 {
>>>>>>>> 083ae4858f (Fix IT names & Fix region operation related IT (#14905)):integration-test/src/test/java/org/apache/iotdb/confignode/it/regionmigration/pass/datanodecrash/IoTDBRegionMigrateOriginalCrashWhenDeleteLocalPeerForIoTV2BatchIT.java
  @Test
  public void crashBeforeDelete() throws Exception {
    success(IoTConsensusDeleteLocalPeerKillPoints.BEFORE_DELETE);
  }

  @Test
  public void crashAfterDelete() throws Exception {
    success(IoTConsensusDeleteLocalPeerKillPoints.AFTER_DELETE);
  }
}
