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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeTsFileDecompositionWithModsIT extends AbstractPipeDualTreeModelManualIT {

  @Override
  protected void setupConfig() {
    super.setupConfig();
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
  }

  /**
   * Test IoTDB pipe handling TsFile decomposition with Mods (modification operations) in tree model
   *
   * <p>Test scenario: 1. Create database root.sg1 with 4 devices: d1 (aligned timeseries), d2
   * (non-aligned timeseries), d3 (aligned timeseries), d4 (aligned timeseries) 2. Insert initial
   * data into d1, d2, d3 3. Execute FLUSH operation to persist data to TsFile 4. Execute DELETE
   * operation on d1.s1, deleting data in time range 2-4 5. Insert large amount of data into d4
   * (11000 records, inserted in batches) 6. Execute multiple DELETE operations on d4: - Delete s1
   * field data where time<=10000 - Delete s2 field data where time>1000 - Delete s3 field data
   * where time<=8000 7. Delete all data from d2 and d3 8. Execute FLUSH operation again 9. Create
   * pipe with mods enabled, synchronize data to receiver 10. Verify correctness of receiver data: -
   * d1 s1 field is null in time range 2-4, other data is normal - d2 and d3 data is completely
   * deleted - d4 DELETE operation results meet expectations for each field
   *
   * <p>Test purpose: Verify that IoTDB pipe can correctly handle Mods (modification operations) in
   * TsFile under tree model, ensuring various DELETE operations can be correctly synchronized to
   * the receiver and data consistency is guaranteed.
   */
  @Test
  public void testTsFileDecompositionWithMods() throws Exception {
    TestUtils.executeNonQueryWithRetry(senderEnv, "CREATE DATABASE root.sg1");
    TestUtils.executeNonQueryWithRetry(receiverEnv, "CREATE DATABASE root.sg1");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d1(s1 FLOAT, s2 FLOAT, s3 FLOAT)");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE TIMESERIES root.sg1.d2.s1 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE TIMESERIES root.sg1.d2.s2 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d3(s1 FLOAT, s2 FLOAT, s3 FLOAT)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d1(time, s1, s2, s3) ALIGNED VALUES (1, 1.0, 2.0, 3.0), (2, 1.1, 2.1, 3.1), (3, 1.2, 2.2, 3.2), (4, 1.3, 2.3, 3.3), (5, 1.4, 2.4, 3.4)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d2(time, s1, s2) VALUES (1, 10.0, 20.0), (2, 10.1, 20.1), (3, 10.2, 20.2), (4, 10.3, 20.3), (5, 10.4, 20.4)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d3(time, s1, s2, s3) ALIGNED VALUES (1, 100.0, 200.0, 300.0), (2, 100.1, 200.1, 300.1), (3, 100.2, 200.2, 300.2)");

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "DELETE FROM root.sg1.d1.s1 WHERE time >= 2 AND time <= 4");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d3(time, s1, s2, s3) ALIGNED VALUES (1, 100.0, 200.0, 300.0), (2, 100.1, 200.1, 300.1), (3, 100.2, 200.2, 300.2)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d4(s1 FLOAT, s2 FLOAT, s3 FLOAT)");
    String s = "INSERT INTO root.sg1.d4(time, s1, s2, s3) ALIGNED VALUES ";
    StringBuilder insertBuilder = new StringBuilder(s);
    for (int i = 1; i <= 11000; i++) {
      insertBuilder
          .append("(")
          .append(i)
          .append(",")
          .append(1.0f)
          .append(",")
          .append(2.0f)
          .append(",")
          .append(3.0f)
          .append(")");
      if (i % 100 != 0) {
        insertBuilder.append(",");
      } else {
        TestUtils.executeNonQueryWithRetry(senderEnv, insertBuilder.toString());
        insertBuilder = new StringBuilder(s);
      }
    }

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s1 WHERE time <= 10000");
    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s2 WHERE time > 1000");
    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s3 WHERE time <= 8000");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d2.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d3.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe WITH SOURCE ('mods.enable'='true') WITH CONNECTOR('ip'='%s', 'port'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    HashSet<String> results = new HashSet<>();
    results.add("1,3.0,1.0,2.0,");
    results.add("2,3.1,null,2.1,");
    results.add("3,3.2,null,2.2,");
    results.add("4,3.3,null,2.3,");
    results.add("5,3.4,1.4,2.4,");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT * FROM root.sg1.d1 ORDER BY time",
        "Time,root.sg1.d1.s3,root.sg1.d1.s1,root.sg1.d1.s2,",
        results);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT * FROM root.sg1.d2 ORDER BY time", "Time,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT * FROM root.sg1.d3 ORDER BY time", "Time,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT s1 FROM root.sg1.d1 WHERE time >= 2 AND time <= 4",
        "Time,root.sg1.d1.s1,",
        Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(**) FROM root.sg1.d4",
        "COUNT(root.sg1.d4.s3),COUNT(root.sg1.d4.s1),COUNT(root.sg1.d4.s2),",
        Collections.singleton("3000,1000,1000,"));
  }

  /**
   * Test IoTDB pipe handling TsFile decomposition with Mods (modification operations) in tree model
   * - Multi-pipe scenario
   *
   * <p>Test scenario: 1. Create database root.sg1 with 4 devices: d1 (aligned timeseries), d2
   * (non-aligned timeseries), d3 (aligned timeseries), d4 (aligned timeseries) 2. Insert initial
   * data into d1, d2, d3 3. Execute FLUSH operation to persist data to TsFile 4. Execute DELETE
   * operation on d1.s1, deleting data in time range 2-4 5. Insert large amount of data into d4
   * (11000 records, inserted in batches) 6. Execute multiple DELETE operations on d4: - Delete s1
   * field data where time<=10000 - Delete s2 field data where time>1000 - Delete s3 field data
   * where time<=8000 7. Delete all data from d2 and d3 8. Execute FLUSH operation again 9. Create 4
   * independent pipes, each targeting different device paths: - test_pipe1: handles data for
   * root.sg1.d1.** path - test_pipe2: handles data for root.sg1.d2.** path - test_pipe3: handles
   * data for root.sg1.d3.** path - test_pipe4: handles data for root.sg1.d4.** path 10. Verify
   * correctness of receiver data: - d1 s1 field is null in time range 2-4, other data is normal -
   * d2 and d3 data is completely deleted - d4 DELETE operation results meet expectations for each
   * field
   *
   * <p>Test purpose: Verify that IoTDB pipe can correctly handle Mods (modification operations) in
   * TsFile under tree model through multiple independent pipes, ensuring DELETE operations for
   * different paths can be correctly synchronized to the receiver and data consistency is
   * guaranteed. The main difference from the first test method is using multiple pipes to handle
   * data for different devices separately.
   */
  @Test
  public void testTsFileDecompositionWithMods2() throws Exception {
    TestUtils.executeNonQueryWithRetry(senderEnv, "CREATE DATABASE root.sg1");
    TestUtils.executeNonQueryWithRetry(receiverEnv, "CREATE DATABASE root.sg1");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d1(s1 FLOAT, s2 FLOAT, s3 FLOAT)");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE TIMESERIES root.sg1.d2.s1 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE TIMESERIES root.sg1.d2.s2 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d3(s1 FLOAT, s2 FLOAT, s3 FLOAT)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d1(time, s1, s2, s3) ALIGNED VALUES (1, 1.0, 2.0, 3.0), (2, 1.1, 2.1, 3.1), (3, 1.2, 2.2, 3.2), (4, 1.3, 2.3, 3.3), (5, 1.4, 2.4, 3.4)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d2(time, s1, s2) VALUES (1, 10.0, 20.0), (2, 10.1, 20.1), (3, 10.2, 20.2), (4, 10.3, 20.3), (5, 10.4, 20.4)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d3(time, s1, s2, s3) ALIGNED VALUES (1, 100.0, 200.0, 300.0), (2, 100.1, 200.1, 300.1), (3, 100.2, 200.2, 300.2)");

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "DELETE FROM root.sg1.d1.s1 WHERE time >= 2 AND time <= 4");

    TestUtils.executeNonQueryWithRetry(
        senderEnv,
        "INSERT INTO root.sg1.d3(time, s1, s2, s3) ALIGNED VALUES (1, 100.0, 200.0, 300.0), (2, 100.1, 200.1, 300.1), (3, 100.2, 200.2, 300.2)");

    TestUtils.executeNonQueryWithRetry(
        senderEnv, "CREATE ALIGNED TIMESERIES root.sg1.d4(s1 FLOAT, s2 FLOAT, s3 FLOAT)");
    String s = "INSERT INTO root.sg1.d4(time, s1, s2, s3) ALIGNED VALUES ";
    StringBuilder insertBuilder = new StringBuilder(s);
    for (int i = 1; i <= 11000; i++) {
      insertBuilder
          .append("(")
          .append(i)
          .append(",")
          .append(1.0f)
          .append(",")
          .append(2.0f)
          .append(",")
          .append(3.0f)
          .append(")");
      if (i % 100 != 0) {
        insertBuilder.append(",");
      } else {
        TestUtils.executeNonQueryWithRetry(senderEnv, insertBuilder.toString());
        insertBuilder = new StringBuilder(s);
      }
    }

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s1 WHERE time <= 10000");
    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s2 WHERE time > 1000");
    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d4.s3 WHERE time <= 8000");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d2.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d3.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe1 WITH SOURCE ('mods.enable'='true','path'='root.sg1.d1.**') WITH CONNECTOR('ip'='%s', 'port'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe2 WITH SOURCE ('mods.enable'='true','path'='root.sg1.d2.**') WITH CONNECTOR('ip'='%s', 'port'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe3 WITH SOURCE ('mods.enable'='true','path'='root.sg1.d3.**') WITH CONNECTOR('ip'='%s', 'port'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    executeNonQueryWithRetry(
        senderEnv,
        String.format(
            "CREATE PIPE test_pipe4 WITH SOURCE ('mods.enable'='true','path'='root.sg1.d4.**') WITH CONNECTOR('ip'='%s', 'port'='%s', 'format'='tablet')",
            receiverEnv.getDataNodeWrapperList().get(0).getIp(),
            receiverEnv.getDataNodeWrapperList().get(0).getPort()));

    HashSet<String> results = new HashSet<>();
    results.add("1,3.0,1.0,2.0,");
    results.add("2,3.1,null,2.1,");
    results.add("3,3.2,null,2.2,");
    results.add("4,3.3,null,2.3,");
    results.add("5,3.4,1.4,2.4,");

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT * FROM root.sg1.d1 ORDER BY time",
        "Time,root.sg1.d1.s3,root.sg1.d1.s1,root.sg1.d1.s2,",
        results);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT * FROM root.sg1.d2 ORDER BY time", "Time,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT * FROM root.sg1.d3 ORDER BY time", "Time,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT s1 FROM root.sg1.d1 WHERE time >= 2 AND time <= 4",
        "Time,root.sg1.d1.s1,",
        Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "SELECT COUNT(**) FROM root.sg1.d4",
        "COUNT(root.sg1.d4.s3),COUNT(root.sg1.d4.s1),COUNT(root.sg1.d4.s2),",
        Collections.singleton("3000,1000,1000,"));
  }
}
