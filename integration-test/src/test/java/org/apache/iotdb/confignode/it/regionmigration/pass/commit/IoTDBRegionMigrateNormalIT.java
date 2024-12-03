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
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionMigrateReliabilityITFramework;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateNormalIT extends IoTDBRegionMigrateReliabilityITFramework {
  @Test
  public void normal1C2DTest() throws Exception {
    successTest(1, 1, 1, 2, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }

  @Test
  public void normal3C3DTest() throws Exception {
    successTest(2, 3, 3, 3, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }
}
