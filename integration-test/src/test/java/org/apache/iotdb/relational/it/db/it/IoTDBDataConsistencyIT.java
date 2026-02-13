/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.db.it;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.utils.Pair;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("UnnecessaryLocalVariable")
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDataConsistencyIT {

  private static final int numDNs = 3;
  private static final int numDataReplications = 2;
  //                                     device   measurement   values
  private final Map<DataNodeWrapper, Map<String, Map<String, List<Object>>>> dataNodeData = new HashMap<>();
  //                device      measurement  value  #occurrences
  private final Map<String, Map<String, Map<Object, Integer>>> dataOccurrences = new HashMap<>();
  private final boolean verbose = true;

  @BeforeClass
  public static void setUpClass() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataReplicationFactor(numDataReplications)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(1, numDNs);
  }

  @Before
  public void setUp() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS test");
      statement.execute("CREATE TABLE test.t1 (tag1 string tag, s1 int32, s2 int32)");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS test");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void printCollectedResult() {
    System.out.println("====================Collected Result=====================");
    dataNodeData.forEach((dn, data) -> System.out.println(dn + ": " + data));
    dataOccurrences.forEach((deviceId, measurementMap) -> System.out.println(deviceId + ": " + measurementMap));
  }

  private void collectDataAndOccurrences(ResultSet resultSet,
      Map<DataNodeWrapper, Map<String, Map<String, List<Object>>>> dataNodeData,
      Map<String, Map<String, Map<Object, Integer>>> dataOccurrences,
      DataNodeWrapper dataNodeWrapper) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<String> tagColumnNames = new ArrayList<>();
    List<String> fieldColumnNames = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      if (columnName.startsWith("tag")) {
        tagColumnNames.add(columnName);
      } else if (columnName.startsWith("s")) {
        fieldColumnNames.add(columnName);
      }
    }

    while (resultSet.next()) {
      long time = resultSet.getLong("time");
      StringBuilder deviceId = new StringBuilder();
      for (String tagColumnName : tagColumnNames) {
        String tag = resultSet.getString(tagColumnName);
        deviceId.append(tag).append(",");
      }

      for (String fieldColumnName : fieldColumnNames) {
        Object val = resultSet.getObject(fieldColumnName);
        Pair<Long, Object> timeValuePair = new Pair<>(time, val);
        dataOccurrences.computeIfAbsent(deviceId.toString(), k -> new HashMap<>()).computeIfAbsent(
            fieldColumnName, k -> new HashMap<>()).merge(timeValuePair, 1, Integer::sum);
        dataNodeData.computeIfAbsent(dataNodeWrapper, dn -> new HashMap<>()).computeIfAbsent(
            deviceId.toString(), k -> new HashMap<>()).computeIfAbsent(fieldColumnName, k -> new ArrayList<>()).add(timeValuePair);
      }
    }
  }

  private void queryAndCollect(Map<DataNodeWrapper, Map<String, Map<String, List<Object>>>> dataNodeData,
      Map<String, Map<String, Map<Object, Integer>>> dataOccurrences,
      BaseEnv env) throws SQLException {
    dataNodeData.clear();
    dataOccurrences.clear();
    List<DataNodeWrapper> dataNodeWrapperList = env.getDataNodeWrapperList();
    for (DataNodeWrapper dataNodeWrapper : dataNodeWrapperList) {
      try (Connection localConnection =
          env.getConnection(dataNodeWrapper, BaseEnv.TABLE_SQL_DIALECT);
          Statement localStatement = localConnection.createStatement()) {
        ResultSet resultSet =
            localStatement.executeQuery("SELECT LOCALLY * FROM test.t1");

        collectDataAndOccurrences(resultSet, dataNodeData, dataOccurrences, dataNodeWrapper);
      }
    }
    if (verbose) {
      printCollectedResult();
    }
  }

  private void checkConsistency(Map<String, Map<String, Map<Object, Integer>>> dataOccurrences, boolean expectEmpty) {
    if (!expectEmpty) {
      assertFalse(dataOccurrences.isEmpty());
    } else {
      assertTrue(dataOccurrences.isEmpty());
    }
    dataOccurrences.values().forEach(measurementMap ->
        measurementMap.values().forEach(valueMap -> valueMap.values().forEach(
            count -> assertEquals(numDataReplications, count.intValue())
        )));
  }

  private void prepareData(Statement statement, long numTimestamp, int numDevices)
      throws SQLException {
    for (int d = 0; d < numDevices; d++) {
      for (long t = 0; t < numTimestamp; t++) {
        statement.execute(String.format("INSERT INTO test.t1 (time, tag1, s1, s2) VALUES(%s, 'a%s', %s, %s)", t, d, t, t + 100));
      }
    }
  }

  @Test
  public void testBasicConsistency() {
    BaseEnv env = EnvFactory.getEnv();

    try (Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      long numTimestamp = 3;
      int numDevices = 3;
      prepareData(statement, numTimestamp, numDevices);

      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });

      statement.execute("FLUSH");
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });
    } catch (Exception e) {
      printCollectedResult();
      fail(e.getMessage());
    }
  }

  @Test
  public void testConsistencyAfterDelete() {
    BaseEnv env = EnvFactory.getEnv();

    try (Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      long numTimestamp = 3;
      int numDevices = 3;
      prepareData(statement, numTimestamp, numDevices);

      statement.execute("DELETE FROM test.t1 WHERE time < 1");
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });

      statement.execute("DELETE FROM test.t1 WHERE tag1='a1'");
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });

      statement.execute("DELETE FROM test.t1");
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, true);
              });
    } catch (Exception e) {
      printCollectedResult();
      fail(e.getMessage());
    }
  }

  @Test
  public void testConsistencyAfterRestart() throws SQLException {
    BaseEnv env = EnvFactory.getEnv();

    try (Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      long numTimestamp = 3;
      int numDevices = 3;
      prepareData(statement, numTimestamp, numDevices);
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });
      statement.execute("FLUSH");
    }

    TestUtils.restartCluster(env);
    try {
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                queryAndCollect(dataNodeData, dataOccurrences, env);
                checkConsistency(dataOccurrences, false);
              });
    } catch (Exception e) {
      printCollectedResult();
      throw e;
    }
  }
}
