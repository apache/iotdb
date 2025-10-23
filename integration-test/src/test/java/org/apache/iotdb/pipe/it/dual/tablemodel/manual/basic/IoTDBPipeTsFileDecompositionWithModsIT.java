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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTsFileDecompositionWithModsIT extends AbstractPipeTableModelDualManualIT {

  /**
   * Test IoTDB pipe handling TsFile decomposition with Mods (modification operations) in table
   * model
   *
   * <p>Test scenario: 1. Create two storage groups sg1 and sg2, each containing table1 2. Insert
   * small amount of data in sg1 (1-6 rows), insert large amount of data in sg2 (110 batches, 100
   * rows per batch) 3. Execute FLUSH operation to persist data to TsFile 4. Execute multiple DELETE
   * operations on sg1, deleting data in time ranges 2-4 and 3-5 5. Execute multiple DELETE
   * operations on sg2, deleting data matching specific conditions (s0-s3 field values) 6. Execute
   * FLUSH operation again 7. Create pipe with mods enabled, synchronize data to receiver 8. Verify
   * correctness of receiver data: - sg1 only retains time=1 data, time=2-4 data is correctly
   * deleted - sg2 DELETE operation results meet expectations (t10 retains 1000 rows, t11 all
   * deleted, t12 retains 5900 rows, etc.)
   *
   * <p>Test purpose: Verify that IoTDB pipe can correctly handle Mods (modification operations) in
   * TsFile, ensuring DELETE operations can be correctly synchronized to the receiver and data
   * consistency is guaranteed.
   */
  @Test
  public void testTsFileDecompositionWithMods() {
    TableModelUtils.createDataBaseAndTable(senderEnv, "table1", "sg1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "table1", "sg1");

    TableModelUtils.insertData("sg1", "table1", 1, 6, senderEnv);

    TableModelUtils.createDataBaseAndTable(senderEnv, "table1", "sg2");
    for (int i = 1; i <= 110; i++) {
      TableModelUtils.insertData("sg2", "table1", 10, 15, (i - 1) * 100, i * 100, senderEnv);
    }

    executeNonQueryWithRetry(senderEnv, "FLUSH");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 2 AND time <= 4",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg1",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 3 AND time <= 5",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg1",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 0 AND time < 10000 AND s0 ='t10' AND s1='t10' AND s2='t10' AND s3='t10'",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg2",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 0 AND time <= 11000 AND s0 ='t11' AND s1='t11' AND s2='t11' AND s3='t11'",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg2",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 5000 AND time < 10100 AND s0 ='t12' AND s1='t12' AND s2='t12' AND s3='t12'",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg2",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 0 AND time < 10000 AND s0 ='t13' AND s1='t13' AND s2='t13' AND s3='t13'",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg2",
        "table");

    executeNonQueryWithRetry(
        senderEnv,
        "DELETE FROM table1 WHERE time >= 10000 AND time <= 11000 AND s0 ='t14' AND s1='t14' AND s2='t14' AND s3='t14'",
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        "sg2",
        "table");

    executeNonQueryWithRetry(senderEnv, "FLUSH");

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe WITH SOURCE ('mods.enable'='true', 'capture.table'='true') WITH CONNECTOR('ip'='%s', 'port'='%s', 'username'='root', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    HashSet<String> expectedResults = new HashSet<>();
    expectedResults.add(
        "t1,t1,t1,t1,1,1.0,1,1970-01-01T00:00:00.001Z,1,1.0,1970-01-01,1,1970-01-01T00:00:00.001Z,");

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
        receiverEnv,
        "SELECT COUNT(*) as count FROM table1 WHERE s0 ='t10' AND s1='t10' AND s2='t10' AND s3='t10'",
        "count,",
        Collections.singleton("1000,"),
        "sg2");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(*) as count FROM table1 WHERE s0 ='t11' AND s1='t11' AND s2='t11' AND s3='t11'",
        "count,",
        Collections.singleton("0,"),
        "sg2");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(*) as count FROM table1 WHERE s0 ='t12' AND s1='t12' AND s2='t12' AND s3='t12'",
        "count,",
        Collections.singleton("5900,"),
        "sg2");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(*) as count FROM table1 WHERE s0 ='t13' AND s1='t13' AND s2='t13' AND s3='t13'",
        "count,",
        Collections.singleton("1000,"),
        "sg2");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(*) as count FROM table1 WHERE s0 ='t14' AND s1='t14' AND s2='t14' AND s3='t14'",
        "count,",
        Collections.singleton("10000,"),
        "sg2");
  }
}
