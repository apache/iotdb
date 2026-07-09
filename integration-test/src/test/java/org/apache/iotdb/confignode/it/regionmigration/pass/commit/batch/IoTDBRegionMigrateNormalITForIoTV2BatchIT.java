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

package org.apache.iotdb.confignode.it.regionmigration.pass.commit.batch;

import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.util.MagicUtils.makeItCloseQuietly;

@Category({ClusterIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateNormalITForIoTV2BatchIT
    extends IoTDBRegionOperationReliabilityITFramework {
  @Test
  public void normal1C2DTest() throws Exception {
    successTest(1, 1, 1, 2, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }

  @Test
  public void normal3C3DTest() throws Exception {
    successTest(2, 3, 3, 3, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);
  }

  @Test
  public void migrateRegionWithDegradedTimeIndexTest() throws Exception {
    // set TsFileResource memory to 0 to trigger degrading
    EnvFactory.getEnv().getConfig().getCommonConfig().setQueryMemoryProportion("1:1:1:1:1:1:0");

    successTest(1, 1, 1, 2, noKillPoints(), noKillPoints(), KillNode.ALL_NODES);

    try (final Connection connection = makeItCloseQuietly(EnvFactory.getEnv().getConnection());
        final Statement statement = makeItCloseQuietly(connection.createStatement())) {
      assertCounts(statement, 1, 1);
      statement.execute("INSERT INTO root.sg.d1(timestamp,speed,temperature) values(101, 3, 4)");
      assertCounts(statement, 2, 2);
    }
  }

  private static void assertCounts(
      final Statement statement, final long expectedSpeedCount, final long expectedTemperatureCount)
      throws Exception {
    try (final ResultSet resultSet =
        statement.executeQuery("select count(speed), count(temperature) from root.sg.d1")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedSpeedCount, resultSet.getLong(1));
      Assert.assertEquals(expectedTemperatureCount, resultSet.getLong(2));
    }
  }
}
