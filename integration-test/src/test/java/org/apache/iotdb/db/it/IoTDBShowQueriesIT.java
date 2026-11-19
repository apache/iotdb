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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.QUERY_ID;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.STATEMENT;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowQueriesIT {
  private static final String DATABASE = "root.show_queries";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String TIMESERIES = DEVICE + ".s1";
  private static final String BATCH_QUERY = "SELECT s1 FROM " + DEVICE;
  private static final String SHOW_QUERIES = "SHOW QUERIES";
  private static final String TEMPLATE = "show_queries_template";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("CREATE TIMESERIES " + TIMESERIES + " INT32");
      session.executeNonQueryStatement("INSERT INTO " + DEVICE + "(time, s1) VALUES (1, 1)");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void queryInBatchReleaseTest() throws Exception {
    try (ISession adminSession = EnvFactory.getEnv().getSessionConnection();
        Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Set<String> queryIdsBefore = getOtherQueryIds(adminSession);
      statement.addBatch(BATCH_QUERY);
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
