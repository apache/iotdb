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
import java.util.Arrays;
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
        .setSchemaReplicationFactor(1)
        .setPipeAutoSplitFullEnabled(true);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setDataReplicationFactor(1)
        .setSchemaReplicationFactor(1)
        .setPipeAutoSplitFullEnabled(true);
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

  @Test
  public void testAutoSplitHistoryTsFileWithDeletionWhenReceiverAutoCreateSchemaDisabled()
      throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.sg",
            "create timeseries root.sg.non_aligned.s1 with datatype=INT32",
            "create timeseries root.sg.non_aligned.s2 with datatype=DOUBLE",
            "create aligned timeseries root.sg.aligned(s1 INT32, s2 DOUBLE)",
            "create timeseries root.sg.deleted_measurement.s1 with datatype=INT32",
            "create timeseries root.sg.deleted_measurement.s2 with datatype=DOUBLE",
            "insert into root.sg.non_aligned(time, s1, s2) values(1, 1, 1.0), (2, 2, 2.0), (3, 3, 3.0)",
            "insert into root.sg.aligned(time, s1, s2) values(1, 10, 10.0), (2, 20, 20.0), (3, 30, 30.0)",
            "insert into root.sg.deleted_measurement(time, s1, s2) values(1, 100, 100.0), (2, 200, 200.0)",
            "flush",
            "delete from root.sg.non_aligned.s1 where time > 2",
            "delete from root.sg.aligned.* where time > 2",
            "delete timeseries root.sg.deleted_measurement.s1",
            "flush"));

    awaitUntilFlush(senderEnv);

    TestUtils.executeNonQuery(
        senderEnv,
        String.format(
            "create pipe test with source ('inclusion'='all', 'source.history.enable'='true', 'source.realtime.mode'='batch') "
                + "with sink ('sink'='iotdb-thrift-sink', 'sink.node-urls'='%s')",
            receiverEnv.getDataNodeWrapper(0).getIpAndPortString()));

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.sg.non_aligned",
        "Time,root.sg.non_aligned.s1,root.sg.non_aligned.s2,",
        new HashSet<>(Arrays.asList("1,1,1.0,", "2,2,2.0,", "3,null,3.0,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.sg.aligned",
        "Time,root.sg.aligned.s1,root.sg.aligned.s2,",
        new HashSet<>(Arrays.asList("1,10,10.0,", "2,20,20.0,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.sg.deleted_measurement",
        "Time,root.sg.deleted_measurement.s2,",
        new HashSet<>(Arrays.asList("1,100.0,", "2,200.0,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries root.sg.deleted_measurement.*",
        "count(timeseries),",
        new HashSet<>(Arrays.asList("1,")));
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show devices root.sg.aligned",
        "Device,IsAligned,Template,TTL(ms),",
        new HashSet<>(Arrays.asList("root.sg.aligned,true,null,INF,")));
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
