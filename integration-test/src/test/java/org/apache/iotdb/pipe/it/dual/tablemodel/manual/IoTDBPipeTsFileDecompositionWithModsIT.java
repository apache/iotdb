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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashSet;

import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQueryWithRetry;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeTsFileDecompositionWithModsIT extends AbstractPipeTableModelDualManualIT {

  @Test
  public void testTsFileDecompositionWithMods() {
    TableModelUtils.createDataBaseAndTable(senderEnv, "table1", "sg1");
    TableModelUtils.createDataBaseAndTable(receiverEnv, "table1", "sg1");

    TableModelUtils.insertData("sg1", "table1", 1, 6, senderEnv);

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
  }
}
