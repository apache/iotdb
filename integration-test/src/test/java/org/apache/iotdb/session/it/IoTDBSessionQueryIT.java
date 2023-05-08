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

package org.apache.iotdb.session.it;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSessionQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertDataWithSession();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ------------------------------ Raw Data Query ----------------------------------
  @Test
  public void rawDataQueryWithTimeRangeTest1() throws IoTDBConnectionException {
    String[] retArray =
        new String[] {
          "16,16.0,null,null",
          "17,17.0,null,null",
          "18,18.0,null,null",
          "19,19.0,null,null",
          "20,20.0,null,null",
          "21,null,true,null",
          "22,null,true,null",
          "23,230000.0,false,null",
          "24,null,true,null",
          "25,null,true,null",
          "26,null,false,null",
          "27,null,false,null",
          "28,null,false,null",
          "29,null,false,null",
          "30,null,false,null",
          "31,null,null,aligned_test31",
          "32,null,null,aligned_test32",
          "33,null,null,aligned_test33",
          "34,null,null,aligned_test34",
        };

    List<String> columnNames = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s4", "root.sg1.d1.s5");

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeRawDataQuery(columnNames, 16, 35)) {
        assertResultSetEqual(resultSet, columnNames, retArray, false);
      }
    } catch (StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void rawDataQueryWithTimeRangeTest2() throws IoTDBConnectionException {
    String[] retArray =
        new String[] {
          "16,null,null,16.0,null,null,16.0",
          "17,null,null,17.0,null,null,17.0",
          "18,null,null,18.0,null,null,18.0",
          "19,null,null,19.0,null,null,19.0",
          "20,null,null,20.0,null,null,20.0",
          "21,null,true,null,null,true,null",
          "22,null,true,null,null,true,null",
          "23,null,false,null,null,true,230000.0",
          "24,null,true,null,null,true,null",
          "25,null,true,null,null,true,null",
          "26,null,false,null,null,false,null",
          "27,null,false,null,null,false,null",
          "28,null,false,null,null,false,null",
          "29,null,false,null,null,false,null",
          "30,null,false,null,null,false,null",
          "31,non_aligned_test31,null,null,aligned_test31,null,null",
          "32,non_aligned_test32,null,null,aligned_test32,null,null",
          "33,non_aligned_test33,null,null,aligned_test33,null,null",
          "34,non_aligned_test34,null,null,aligned_test34,null,null",
        };

    List<String> columnNames =
        Arrays.asList(
            "root.sg1.d2.s5",
            "root.sg1.d1.s4",
            "root.sg1.d2.s1",
            "root.sg1.d1.s5",
            "root.sg1.d2.s4",
            "root.sg1.d1.s1");

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeRawDataQuery(columnNames, 16, 35)) {
        assertResultSetEqual(resultSet, columnNames, retArray, false);
      }
    } catch (StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // --------------------------------- Last Query ------------------------------------
  private final List<String> lastQueryColumnNames =
      Arrays.asList("Time", "Timeseries", "Value", "DataType");

  @Test
  public void lastQueryTest() throws IoTDBConnectionException {
    String[] retArray = new String[] {"23,root.sg1.d1.s1,230000.0,FLOAT"};

    List<String> selectedPaths = Collections.singletonList("root.sg1.d1.s1");

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeLastDataQuery(selectedPaths)) {
        assertResultSetEqual(resultSet, lastQueryColumnNames, retArray, true);
      }
    } catch (StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastQueryWithLastTimeTest() throws IoTDBConnectionException {
    String[] retArray = new String[] {};

    List<String> selectedPaths = Collections.singletonList("root.sg1.d1.s1");

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeLastDataQuery(selectedPaths, 30)) {
        assertResultSetEqual(resultSet, lastQueryColumnNames, retArray, true);
      }
    } catch (StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ------------------------------ Aggregation Query ------------------------------
  @Test
  public void aggregationQueryTest() {
    String[] retArray = new String[] {"0,20,29,28,19,20"};
    List<String> paths =
        Arrays.asList(
            "root.sg1.d1.s1",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d1.s4",
            "root.sg1.d1.s5");
    List<TAggregationType> aggregations = Collections.nCopies(paths.size(), TAggregationType.COUNT);

    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      columnNames.add(String.format("%s(%s)", aggregations.get(i), paths.get(i)));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeAggregationQuery(paths, aggregations)) {
        assertResultSetEqual(resultSet, columnNames, retArray, true);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregationQueryWithTimeRangeTest() {
    String[] retArray = new String[] {"0,12,15,22,13,6"};
    List<String> paths =
        Arrays.asList(
            "root.sg1.d1.s1",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d1.s4",
            "root.sg1.d1.s5");
    List<TAggregationType> aggregations = Collections.nCopies(paths.size(), TAggregationType.COUNT);

    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      columnNames.add(String.format("%s(%s)", aggregations.get(i), paths.get(i)));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet = session.executeAggregationQuery(paths, aggregations, 9, 34)) {
        assertResultSetEqual(resultSet, columnNames, retArray, true);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ---------------------------- Group By Aggregation Query -------------------------
  @Test
  public void groupByQueryTest1() {
    String[] retArray =
        new String[] {"11,10,130142.0,13014.2", "21,1,null,230000.0", "31,0,355.0,null"};
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s1");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.COUNT, TAggregationType.SUM, TAggregationType.AVG);

    List<String> columnNames = new ArrayList<>();
    columnNames.add("Time");
    for (int i = 0; i < paths.size(); i++) {
      columnNames.add(String.format("%s(%s)", aggregations.get(i), paths.get(i)));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet =
          session.executeAggregationQuery(paths, aggregations, 11, 41, 10)) {
        assertResultSetEqual(resultSet, columnNames, retArray, true);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByQueryTest2() {
    String[] retArray =
        new String[] {
          "7,3,34.0,8.0",
          "13,4,130045.0,32511.25",
          "19,2,39.0,19.5",
          "25,0,null,null",
          "31,0,130.0,null",
          "37,0,154.0,null"
        };
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s1");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.COUNT, TAggregationType.SUM, TAggregationType.AVG);

    List<String> columnNames = new ArrayList<>();
    columnNames.add("Time");
    for (int i = 0; i < paths.size(); i++) {
      columnNames.add(String.format("%s(%s)", aggregations.get(i), paths.get(i)));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet =
          session.executeAggregationQuery(paths, aggregations, 7, 41, 4, 6)) {
        assertResultSetEqual(resultSet, columnNames, retArray, true);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByQueryTest3() {
    String[] retArray =
        new String[] {
          "6,9,130092.0,14453.555555555555",
          "11,10,130142.0,13014.2",
          "16,6,90.0,38348.333333333336",
          "21,1,null,230000.0",
          "26,0,165.0,null",
          "31,0,355.0,null",
          "36,0,190.0,null"
        };
    List<String> paths = Arrays.asList("root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s1");
    List<TAggregationType> aggregations =
        Arrays.asList(TAggregationType.COUNT, TAggregationType.SUM, TAggregationType.AVG);

    List<String> columnNames = new ArrayList<>();
    columnNames.add("Time");
    for (int i = 0; i < paths.size(); i++) {
      columnNames.add(String.format("%s(%s)", aggregations.get(i), paths.get(i)));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      try (SessionDataSet resultSet =
          session.executeAggregationQuery(paths, aggregations, 6, 41, 10, 5)) {
        assertResultSetEqual(resultSet, columnNames, retArray, true);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
