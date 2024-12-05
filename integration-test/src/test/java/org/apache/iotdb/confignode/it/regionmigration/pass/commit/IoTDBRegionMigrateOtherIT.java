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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit;

import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.commons.utils.KillPoint.NeverTriggeredKillPoint;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBRegionMigrateOtherIT extends IoTDBRegionMigrateReliabilityITFramework {
  @Test
  public void badKillPoint() throws Exception {
    try {
      successTest(
          1,
          1,
          1,
          2,
          buildSet(NeverTriggeredKillPoint.NEVER_TRIGGERED_KILL_POINT),
          noKillPoints(),
          KillNode.ALL_NODES);
    } catch (AssertionError e) {
      return;
    }
    Assert.fail("kill point not triggered but test pass");
  }
}
