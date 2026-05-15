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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.basic;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoBasic;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoBasic.class})
public class IoTDBPipeReceiverAutoCreateDisabledIT extends AbstractPipeDualTreeModelAutoIT {

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
  }

  @Override
  protected void setupConfig() {
    super.setupConfig();
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1);
  }

  @Test
  public void testReceiverAutoCreateSchemaDisabledWithSpecialTimeSeries() throws Exception {
    Assert.assertEquals(1, senderEnv.getConfigNodeWrapperList().size());
    Assert.assertEquals(1, senderEnv.getDataNodeWrapperList().size());
    Assert.assertEquals(1, receiverEnv.getConfigNodeWrapperList().size());
    Assert.assertEquals(1, receiverEnv.getDataNodeWrapperList().size());

    final String createPipeSql =
        String.format(
            "create pipe test with source ('inclusion'='all') "
                + "with sink ('sink'='iotdb-thrift-sink', 'sink.node-urls'='%s');",
            receiverEnv.getDataNodeWrapper(0).getIpAndPortString());
    final String createDatabaseSql = "create database root.test.sg;";
    final String createSecondDatabaseSql =
        "create database root.test.ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz1;";
    final String createFirstTimeSeriesSql =
        "create timeseries root.test.sg.`1~!@#$%^&*()_+=:'\"/|[]{}`.`~!@#$%^&*()_+=:'\"/|[]{}` float;";
    final String insertFirstSql =
        "insert into root.test.sg.`1~!@#$%^&*()_+=:'\"/|[]{}`(time, `~!@#$%^&*()_+=:'\"/|[]{}`) "
            + "values (1706659200,3.5),(1706660000, 15.5);";
    final String firstSelectSql =
        "select `~!@#$%^&*()_+=:'\"/|[]{}` from root.test.sg.`1~!@#$%^&*()_+=:'\"/|[]{}`;";
    final String createSecondTimeSeriesSql =
        "create timeseries root.test.ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz1.`~!@#$%^&*().,<>?_-+=:'\"/|[]{}`.`~!@#$%^&*().,<>?_-+=:'\"/|[]{}` int32;";
    final String insertSecondSql =
        "insert into root.test.ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz1.`~!@#$%^&*().,<>?_-+=:'\"/|[]{}`"
            + "(time, `~!@#$%^&*().,<>?_-+=:'\"/|[]{}`) values (1706666400,23456),(1706667400,23456),(1706686400,23456);";
    final String secondSelectSql =
        "select * from root.test.ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz1.`~!@#$%^&*().,<>?_-+=:'\"/|[]{}`;";

    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(createPipeSql);
      statement.execute(createDatabaseSql);
      statement.execute(createFirstTimeSeriesSql);
      statement.execute(insertFirstSql);
      final QueryResult firstQueryResult = queryForResult(statement, firstSelectSql);
      statement.execute(createSecondDatabaseSql);
      statement.execute(createSecondTimeSeriesSql);
      statement.execute(insertSecondSql);
      final QueryResult secondQueryResult = queryForResult(statement, secondSelectSql);

      awaitUntilFlush(senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, firstSelectSql, firstQueryResult.header, firstQueryResult.rows);
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, secondSelectSql, secondQueryResult.header, secondQueryResult.rows);
    }
  }

  private QueryResult queryForResult(final Statement statement, final String sql)
      throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery(sql)) {
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      final StringBuilder headerBuilder = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        headerBuilder.append(resultSetMetaData.getColumnName(i)).append(",");
      }

      final Set<String> rows = new HashSet<>();
      while (resultSet.next()) {
        final StringBuilder rowBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          rowBuilder.append(resultSet.getString(i)).append(",");
        }
        rows.add(rowBuilder.toString());
      }
      return new QueryResult(headerBuilder.toString(), rows);
    }
  }

  private static class QueryResult {
    private final String header;
    private final Set<String> rows;

    private QueryResult(final String header, final Set<String> rows) {
      this.header = header;
      this.rows = rows;
    }
  }
}
