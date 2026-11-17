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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTsFileDecompositionWithModsIT extends AbstractPipeTableModelDualManualIT {

  /**
   * Flush five rows in sg1 and 55,000 rows in sg2 (devices t10-t14, timestamps [0, 11000)), then
   * apply overlapping time deletions and tag-specific deletions. A mods-enabled tablet pipe must
   * retain only time=1 in sg1 and respectively 1000, 0, 5900, 1000 and 10000 rows for t10-t14 in
   * sg2, preserving both inclusive and exclusive deletion boundaries.
   */
  @Test
  public void testTsFileDecompositionWithMods() throws Exception {
    TableModelUtils.createDataBaseAndTable(senderEnv, "table1", "sg1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "table1", "sg1");

    TableModelUtils.insertData("sg1", "table1", 1, 6, senderEnv);

    TableModelUtils.createDataBaseAndTable(senderEnv, "table1", "sg2");
    try (final ITableSession session = senderEnv.getTableSessionConnection()) {
      // The former 110 calls did not flush between batches. Preserve every row and both flush
      // boundaries while reusing one Session and increasing SQL batches from 10 to 100 rows.
      TableModelUtils.insertData("sg2", "table1", 10, 15, 0, 11000, session, 100);
      session.executeNonQueryStatement("FLUSH");

      session.executeNonQueryStatement("USE sg1");
      session.executeNonQueryStatement("DELETE FROM table1 WHERE time >= 2 AND time <= 4");
      session.executeNonQueryStatement("DELETE FROM table1 WHERE time >= 3 AND time <= 5");

      session.executeNonQueryStatement("USE sg2");
      for (final String deletion :
          Arrays.asList(
              "DELETE FROM table1 WHERE time >= 0 AND time < 10000 AND s0 ='t10' AND s1='t10' AND s2='t10' AND s3='t10'",
              "DELETE FROM table1 WHERE time >= 0 AND time <= 11000 AND s0 ='t11' AND s1='t11' AND s2='t11' AND s3='t11'",
              "DELETE FROM table1 WHERE time >= 5000 AND time < 10100 AND s0 ='t12' AND s1='t12' AND s2='t12' AND s3='t12'",
              "DELETE FROM table1 WHERE time >= 0 AND time < 10000 AND s0 ='t13' AND s1='t13' AND s2='t13' AND s3='t13'",
              "DELETE FROM table1 WHERE time >= 10000 AND time <= 11000 AND s0 ='t14' AND s1='t14' AND s2='t14' AND s3='t14'")) {
        session.executeNonQueryStatement(deletion);
      }
      session.executeNonQueryStatement("FLUSH");
    }

    final Set<String> expectedResults = new HashSet<>();
    expectedResults.add(
        "t1,t1,t1,t1,1,1.0,1,1970-01-01T00:00:00.001Z,1,1.0,1970-01-01,1,1970-01-01T00:00:00.001Z,");

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        TableModelUtils.getQuerySql("table1"),
        TableModelUtils.generateHeaderResults(),
        expectedResults,
        "sg1");

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        "SELECT s4 FROM table1 WHERE time >= 2 AND time <= 4",
        "s4,",
        Collections.emptySet(),
        "sg1");

    final String deviceCountsSql =
        "SELECT s0, s1, s2, s3, COUNT(*) AS count FROM table1 GROUP BY s0, s1, s2, s3";
    final String deviceCountsHeader = "s0,s1,s2,s3,count,";
    final Set<String> expectedDeviceCounts =
        new HashSet<>(
            Arrays.asList(
                "t10,t10,t10,t10,1000,",
                "t12,t12,t12,t12,5900,",
                "t13,t13,t13,t13,1000,",
                "t14,t14,t14,t14,10000,"));

    // Check every sender endpoint before extraction to avoid stale mods. Grouping by all four
    // tags retains the exact device predicates, including t11's absence, in one query per endpoint.
    for (final DataNodeWrapper dataNode : senderEnv.getDataNodeWrapperList()) {
      TestUtils.assertDataEventuallyOnEnv(
          senderEnv, dataNode, deviceCountsSql, deviceCountsHeader, expectedDeviceCounts, "sg2");
    }

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe WITH SOURCE ('mods.enable'='true', 'capture.table'='true', 'inclusion'='data.insert,data.delete') WITH CONNECTOR('ip'='%s', 'port'='%s', 'username'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort(),
            SessionConfig.DEFAULT_USER),
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        null,
        BaseEnv.TABLE_SQL_DIALECT);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("table1"),
        TableModelUtils.generateHeaderResults(),
        expectedResults,
        "sg1");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT s4 FROM table1 WHERE time >= 2 AND time <= 4",
        "s4,",
        Collections.emptySet(),
        "sg1");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, deviceCountsSql, deviceCountsHeader, expectedDeviceCounts, "sg2");
  }
}
