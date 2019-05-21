/**
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
package org.apache.iotdb.cluster.query.manager;

import static org.apache.iotdb.cluster.utils.Utils.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.query.manager.querynode.ClusterLocalQueryManager;
import org.apache.iotdb.cluster.query.manager.querynode.ClusterLocalSingleQueryManager;
import org.apache.iotdb.cluster.query.reader.querynode.AbstractClusterSelectSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReader;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReaderByTimestamp;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterFilterSeriesBatchReaderEntity;
import org.apache.iotdb.cluster.query.reader.querynode.ClusterSelectSeriesBatchReaderEntity;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClusterLocalManagerTest {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static ClusterLocalQueryManager manager = ClusterLocalQueryManager.getInstance();
  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());
  private static final String URL = "127.0.0.1:6667/";

  private String[] createSQLs = {
      "set storage group to root.vehicle",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN"
  };
  private String[] insertSQLs = {
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s3) values(19,'103')",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(22,1031,'3102')",
      "insert into root.vehicle.d0(timestamp,s1) values(192,'1033')"
  };
  private String queryStatementsWithoutFilter = "select * from root.vehicle";
  private String queryStatementsWithFilter = "select * from root.vehicle where d0.s0 > 10 and d0.s0 < 101 or d0.s0 = 3";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    CLUSTER_CONFIG.createAllPath();
    server = Server.getInstance();
    server.start();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testClusterLocalQueryManagerWithoutFilter() throws Exception {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      // first query
      boolean hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      ConcurrentHashMap<String, Long> map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(1, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }

      // second query
      hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(2, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }

      // third query
      hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(3, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }
      statement.close();
    }
  }

  @Test
  public void testClusterLocalQueryManagerWithFilter() throws Exception {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      // first query
      boolean hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      assertEquals(10, resultSet.getLong(1));
      assertEquals(100, resultSet.getInt(2));
      assertNull(resultSet.getString(3));
      assertNull(resultSet.getString(4));
      ConcurrentHashMap<String, Long> map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(1, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }
      assertFalse(resultSet.next());

      // second query
      hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      assertEquals(10, resultSet.getLong(1));
      assertEquals(100, resultSet.getInt(2));
      assertNull(resultSet.getString(3));
      assertNull(resultSet.getString(4));
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(2, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }
      assertFalse(resultSet.next());

      // third query
      hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      assertEquals(10, resultSet.getLong(1));
      assertEquals(100, resultSet.getInt(2));
      assertNull(resultSet.getString(3));
      assertNull(resultSet.getString(4));
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(3, map.size());
      for (String taskId : map.keySet()) {
        assertNotNull(manager.getSingleQuery(taskId));
      }
      assertFalse(resultSet.next());

      statement.close();
    }
  }

  @Test
  public void testClusterLocalSingleQueryWithoutFilterManager() throws Exception {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      // first query
      boolean hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      ConcurrentHashMap<String, Long> map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(1, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(0, singleQueryManager.getQueryRound());
        assertNull(singleQueryManager.getFilterReaderEntity());
        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        assertEquals(3, selectSeriesBatchReaderEntity.getAllReaders().size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReader) clusterBatchReader).getReader());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReader) clusterBatchReader).getDataType());
        }
      }

      // second query
      hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(2, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(0, singleQueryManager.getQueryRound());
        assertNull(singleQueryManager.getFilterReaderEntity());
        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        assertEquals(3, selectSeriesBatchReaderEntity.getAllReaders().size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReader) clusterBatchReader).getReader());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReader) clusterBatchReader).getDataType());
        }
      }

      // third query
      hasResultSet = statement.execute(queryStatementsWithoutFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(3, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(0, singleQueryManager.getQueryRound());
        assertNull(singleQueryManager.getFilterReaderEntity());
        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        assertEquals(3, selectSeriesBatchReaderEntity.getAllReaders().size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReader) clusterBatchReader).getReader());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReader) clusterBatchReader).getDataType());
        }
      }
      statement.close();
    }
  }

  @Test
  public void testClusterLocalSingleQueryWithFilterManager() throws Exception {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + URL, "root", "root")) {
      insertData(connection, createSQLs, insertSQLs);
      Statement statement = connection.createStatement();

      // first query
      boolean hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      ConcurrentHashMap<String, Long> map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(1, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(3, singleQueryManager.getQueryRound());
        ClusterFilterSeriesBatchReaderEntity filterReader = (ClusterFilterSeriesBatchReaderEntity) singleQueryManager.getFilterReaderEntity();
        assertNotNull(filterReader);
        List<Path> allFilterPaths = new ArrayList<>();
        allFilterPaths.add(new Path("root.vehicle.d0.s0"));
        assertTrue(allFilterPaths.containsAll(filterReader.getAllFilterPath()));
        assertNotNull(filterReader.getQueryDataSet());

        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        assertNotNull(selectSeriesBatchReaderEntity.getAllReaders());
        assertEquals(3, selectSeriesBatchReaderEntity.getAllReaders().size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getReaderByTimeStamp());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getDataType());
        }
      }

      // second query
      hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(2, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(3, singleQueryManager.getQueryRound());
        ClusterFilterSeriesBatchReaderEntity filterReader = (ClusterFilterSeriesBatchReaderEntity) singleQueryManager.getFilterReaderEntity();
        assertNotNull(filterReader);
        List<Path> allFilterPaths = new ArrayList<>();
        allFilterPaths.add(new Path("root.vehicle.d0.s0"));
        assertTrue(allFilterPaths.containsAll(filterReader.getAllFilterPath()));
        assertNotNull(filterReader.getQueryDataSet());

        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        assertNotNull(readers);
        assertEquals(3, readers.size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getReaderByTimeStamp());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getDataType());
        }
      }

      // third query
      hasResultSet = statement.execute(queryStatementsWithFilter);
      assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      assertTrue(resultSet.next());
      map = ClusterLocalQueryManager.getTaskIdMapJobId();
      assertEquals(3, map.size());
      for (String taskId : map.keySet()) {
        ClusterLocalSingleQueryManager singleQueryManager = manager.getSingleQuery(taskId);
        assertNotNull(singleQueryManager);
        assertEquals((long) map.get(taskId), singleQueryManager.getJobId());
        assertEquals(3, singleQueryManager.getQueryRound());
        ClusterFilterSeriesBatchReaderEntity filterReader = (ClusterFilterSeriesBatchReaderEntity) singleQueryManager.getFilterReaderEntity();
        assertNotNull(filterReader);
        List<Path> allFilterPaths = new ArrayList<>();
        allFilterPaths.add(new Path("root.vehicle.d0.s0"));
        assertTrue(allFilterPaths.containsAll(filterReader.getAllFilterPath()));
        assertNotNull(filterReader.getQueryDataSet());

        ClusterSelectSeriesBatchReaderEntity selectSeriesBatchReaderEntity = singleQueryManager.getSelectReaderEntity();
        List<AbstractClusterSelectSeriesBatchReader> readers = selectSeriesBatchReaderEntity.getAllReaders();
        assertNotNull(readers);
        assertEquals(3, readers.size());
        Map<String, TSDataType> typeMap = singleQueryManager.getDataTypeMap();
        List<String> paths = selectSeriesBatchReaderEntity.getAllPaths();
        for (int i =0 ; i < readers.size(); i++) {
          TSDataType dataType = typeMap.get(paths.get(i));
          AbstractClusterSelectSeriesBatchReader clusterBatchReader = readers.get(i);
          assertNotNull(((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getReaderByTimeStamp());
          assertEquals(dataType,
              ((ClusterSelectSeriesBatchReaderByTimestamp) clusterBatchReader).getDataType());
        }
      }
      statement.close();
    }
  }

}
