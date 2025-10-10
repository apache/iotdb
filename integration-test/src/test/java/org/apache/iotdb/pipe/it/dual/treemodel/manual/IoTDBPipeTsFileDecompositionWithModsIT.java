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
        receiverEnv, "CREATE ALIGNED TIMESERIES root.sg1.d1(s1 FLOAT, s2 FLOAT, s3 FLOAT)");
    TestUtils.executeNonQueryWithRetry(
        receiverEnv, "CREATE TIMESERIES root.sg1.d2.s1 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        receiverEnv, "CREATE TIMESERIES root.sg1.d2.s2 WITH DATATYPE=FLOAT");
    TestUtils.executeNonQueryWithRetry(
        receiverEnv, "CREATE ALIGNED TIMESERIES root.sg1.d3(s1 FLOAT, s2 FLOAT, s3 FLOAT)");

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

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d2.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "DELETE FROM root.sg1.d3.*");

    TestUtils.executeNonQueryWithRetry(senderEnv, "FLUSH");

    executeNonQueryWithRetry(
            senderEnv,
            String.format(
                    "CREATE PIPE test_pipe WITH SOURCE ('mods.enable'='true') WITH CONNECTOR('ip'='%s', 'port'='%s', 'username'='root', 'format'='tablet')",
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
        receiverEnv, "SELECT * FROM root.sg1.d2 ORDER BY time", "Time,root.sg1.d2.s1,root.sg1.d2.s2,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT * FROM root.sg1.d3 ORDER BY time", "Time,root.sg1.d3.s3,root.sg1.d3.s1,root.sg1.d3.s2,", Collections.emptySet());

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "SELECT s1 FROM root.sg1.d1 WHERE time >= 2 AND time <= 4", "Time,root.sg1.d1.s1,", Collections.emptySet());
  }
}
