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
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeAlterIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testAlterPipeSourceAndSink() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
    // Create pipe
    final String sql =
        String.format(
            "create pipe a2b with source ('source'='iotdb-source', 'database-name'='test', 'table-name'='test', 'mode.streaming'='true') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
    TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);

    // Check data on receiver
    TableModelUtils.assertData("test", "test", 0, 100, receiverEnv, handleFailure);

    // Alter pipe (modify 'source.path', 'source.inclusion' and
    // 'processor.tumbling-time.interval-seconds')
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify source('source' = 'iotdb-source','database-name'='test1', 'table-name'='test1', 'mode.streaming'='true', 'source.inclusion'='data.insert') modify sink ('batch.enable'='true')");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
    TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

    TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 100),
        "test",
        handleFailure);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test1"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 200),
        "test1",
        handleFailure);
  }
}
