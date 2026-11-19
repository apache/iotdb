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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.QUERY_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATEMENT;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowQueriesIT {

  private static final String DATABASE = "root.show_queries";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String TIMESERIES = DEVICE + ".s1";
  private static final String QUERY_USER = "show_queries_query_user";
  private static final String SHOW_USER = "show_queries_show_user";
  private static final String KILL_USER = "show_queries_kill_user";
  private static final String PASSWORD = "password123456";
  private static final String PENDING_QUERY = "SELECT s1 FROM " + DEVICE;
  private static final String SHOW_QUERIES = "SHOW QUERIES";
  private static final String TEMPLATE = "show_queries_template";
  // Leave one default 1,000-row TsBlock after the initial Session fetch.
  private static final int STREAMING_QUERY_ROW_COUNT = SessionConfig.DEFAULT_FETCH_SIZE + 1_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("CREATE TIMESERIES " + TIMESERIES + " INT32");
      Tablet tablet =
          new Tablet(
              DEVICE,
              List.of(new MeasurementSchema("s1", TSDataType.INT32)),
              STREAMING_QUERY_ROW_COUNT);
      for (int i = 0; i < STREAMING_QUERY_ROW_COUNT; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, i);
        tablet.addValue("s1", rowIndex, i);
      }
      session.insertTablet(tablet);
    }
    createUser(QUERY_USER, PASSWORD);
    createUser(SHOW_USER, PASSWORD);
    createUser(KILL_USER, PASSWORD);
    grantUserSeriesPrivilege(QUERY_USER, PrivilegeType.READ_DATA, DATABASE + ".**");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showQueriesAuthTest() throws Exception {
    try (ISession querySession = EnvFactory.getEnv().getSessionConnection(QUERY_USER, PASSWORD);
        ISession showQueriesSession =
            EnvFactory.getEnv().getSessionConnection(SHOW_USER, PASSWORD);
        SessionDataSet pendingDataSet = querySession.executeQueryStatement(PENDING_QUERY)) {
      // Keep QUERY_USER's query active by not consuming pendingDataSet.
      Assert.assertFalse(containsQuery(showQueriesSession, PENDING_QUERY));

      grantUserSystemPrivileges(SHOW_USER, PrivilegeType.SYSTEM);
      Assert.assertTrue(containsQuery(showQueriesSession, PENDING_QUERY));
    }
  }

  @Test
  public void killQueryAuthTest() throws Exception {
    try (ISession querySession = EnvFactory.getEnv().getSessionConnection(QUERY_USER, PASSWORD);
        ISession killSession = EnvFactory.getEnv().getSessionConnection(KILL_USER, PASSWORD);
        ISession adminSession = EnvFactory.getEnv().getSessionConnection();
        SessionDataSet pendingDataSet = querySession.executeQueryStatement(PENDING_QUERY)) {
      // Keep QUERY_USER's query active by not consuming pendingDataSet.
      String queryId = getQueryId(adminSession, PENDING_QUERY);

      StatementExecutionException exception =
          Assert.assertThrows(
              StatementExecutionException.class,
              () -> killSession.executeNonQueryStatement("KILL QUERY '" + queryId + "'"));
      Assert.assertEquals(NO_PERMISSION.getStatusCode(), exception.getStatusCode());
      Assert.assertEquals(queryId, getQueryId(adminSession, PENDING_QUERY));

      grantUserSystemPrivileges(KILL_USER, PrivilegeType.SYSTEM);
      killSession.executeNonQueryStatement("KILL QUERY '" + queryId + "'");
    }
  }

  @Test
  public void queryInBatchReleaseTest() throws Exception {
    try (ISession adminSession = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Set<String> queryIdsBefore = getOtherQueryIds(adminSession);
      statement.addBatch(PENDING_QUERY);
      statement.executeBatch();
      // A batch only returns the status of its statements, so nobody can fetch or close the query.
      assertNoQueryIsLeft(adminSession, queryIdsBefore);
    }
  }

  @Test
  public void schemaTemplateQueryReleaseTest() throws Exception {
    try (ISession adminSession = EnvFactory.getEnv().getSessionConnection()) {
      adminSession.executeNonQueryStatement("CREATE DEVICE TEMPLATE " + TEMPLATE + " (s1 INT32)");
      try {
        Set<String> queryIdsBefore = getOtherQueryIds(adminSession);
        // The measurements come back in the response, so nothing is left to fetch or close.
        Assert.assertEquals(
            Collections.singletonList("s1"), adminSession.showMeasurementsInTemplate(TEMPLATE));
        assertNoQueryIsLeft(adminSession, queryIdsBefore);
      } finally {
        adminSession.executeNonQueryStatement("DROP DEVICE TEMPLATE " + TEMPLATE);
      }
    }
  }

  private static boolean containsQuery(ISession session, String query) throws Exception {
    try (SessionDataSet dataSet = session.executeQueryStatement("SHOW QUERIES")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        if (query.equals(iterator.getString(STATEMENT))) {
          return true;
        }
      }
      return false;
    }
  }

  private static String getQueryId(ISession session, String query) throws Exception {
    try (SessionDataSet dataSet = session.executeQueryStatement("SHOW QUERIES")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        if (query.equals(iterator.getString(STATEMENT))) {
          return iterator.getString(QUERY_ID);
        }
      }
    }
    Assert.fail("Query not found: " + query);
    return null;
  }

  /** Ids of the registered queries, except the SHOW QUERIES statement that lists them. */
  private static Set<String> getOtherQueryIds(ISession session) throws Exception {
    Set<String> queryIds = new HashSet<>();
    try (SessionDataSet dataSet = session.executeQueryStatement(SHOW_QUERIES)) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        if (!SHOW_QUERIES.equals(iterator.getString(STATEMENT))) {
          queryIds.add(iterator.getString(QUERY_ID));
        }
      }
    }
    return queryIds;
  }

  private static void assertNoQueryIsLeft(ISession session, Set<String> queryIdsBefore)
      throws Exception {
    Set<String> leftQueryIds = getOtherQueryIds(session);
    leftQueryIds.removeAll(queryIdsBefore);
    Assert.assertEquals(Collections.emptySet(), leftQueryIds);
  }
}
