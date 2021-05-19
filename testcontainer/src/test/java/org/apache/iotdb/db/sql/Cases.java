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
package org.apache.iotdb.db.sql;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public abstract class Cases {

  protected Statement writeStatement;
  protected Connection writeConnection;
  protected Statement[] readStatements;
  protected Connection[] readConnections;

  /** initialize the writeStatement,writeConnection, readStatements and the readConnections. */
  public abstract void init() throws Exception;

  public void clean() throws Exception {
    writeStatement.close();
    writeConnection.close();
    for (Statement statement : readStatements) {
      statement.close();
    }
    for (Connection connection : readConnections) {
      connection.close();
    }
  }

  // if we seperate the test into multiply test() methods, then the docker container have to be
  // built
  // several times. So, if the test cases are not conflict, we can put them into one method.
  // but if you want to avoid other cases' impact, use a seperate test() method.
  @Test
  public void multiCasesTest() throws SQLException {

    String[] timeSeriesArray = {"root.sg1.aa.bb", "root.sg1.aa.bb.cc", "root.sg1.aa"};

    for (String timeSeries : timeSeriesArray) {
      writeStatement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              timeSeries));
    }
    ResultSet resultSet = null;
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("show timeseries");
      Set<String> result = new HashSet<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      Assert.assertEquals(3, result.size());
      for (String timeseries : timeSeriesArray) {
        Assert.assertTrue(result.contains(timeseries));
      }
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1331
    writeStatement.execute(
        "create timeseries root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    String[] initDataArray = {
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(200,20.71)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(220,50.71)"
    };
    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select avg(temperature) from root.ln.wf01.wt01");
      if (resultSet.next()) {
        Assert.assertEquals(35.71, resultSet.getDouble(1), 0.01);
      } else {
        Assert.fail("expect 1 result, but get an empty resultSet.");
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1348
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(250, 10.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(300, 20.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(350, 25.0)"
        };

    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select last * from root.ln.wf01.wt01;");
      Assert.assertTrue(resultSet.next());
      double last = Double.parseDouble(resultSet.getString(3));
      Assert.assertEquals(25.0, last, 0.1);
      resultSet.close();
    }
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1266
  @Test
  public void showTimeseriesRowsTest() throws SQLException {

    int n = 3000;
    String timeSeriesPrefix = "root.ln.wf01.wt";
    String timeSeriesSuffix = ".temperature WITH DATATYPE=DOUBLE, ENCODING=RLE";
    String timeSeries;
    for (int i = 0; i < n; i++) {
      timeSeries = timeSeriesPrefix + i + timeSeriesSuffix;
      writeStatement.execute(String.format("create timeseries %s ", timeSeries));
    }

    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(n, cnt);
      resultSet.close();
    }
  }

  @Test
  public void testInsertAlignedValues() throws SQLException {
    writeStatement.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (4000, (true, 17.1))");
    writeStatement.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (5000, (false, 20.1))");
    writeStatement.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (6000, (true, 22))");
    // auto-create-schema test
    // same sg, but different device
    writeStatement.execute(
        "insert into root.t1.wf01.wt02(time, (status, temperature)) values (6000, (false, 22))");
    writeStatement.close();

    for (Statement readStatement : readStatements) {
      ResultSet rs1 = readStatement.executeQuery("select status from root.t1.wf01.wt01");
      rs1.next();
      Assert.assertTrue(rs1.getBoolean(2));
      rs1.close();

      ResultSet rs2 = readStatement.executeQuery("select status from root.t1.wf01.wt02");
      rs2.next();
      Assert.assertFalse(rs2.getBoolean(2));
      rs2.close();

      ResultSet rs3 = readStatement.executeQuery("select * from root.t1.wf01.wt01");
      rs3.next();
      Assert.assertEquals(4000, rs3.getLong(1));
      Assert.assertTrue(rs3.getBoolean(2));
      Assert.assertEquals(17.1, rs3.getFloat(3), 0.1);

      rs3.next();
      Assert.assertEquals(5000, rs3.getLong(1));
      Assert.assertFalse(rs3.getBoolean(2));
      Assert.assertEquals(20.1, rs3.getFloat(3), 0.1);

      rs3.next();
      Assert.assertEquals(6000, rs3.getLong(1));
      Assert.assertTrue(rs3.getBoolean(2));
      Assert.assertEquals(22, rs3.getFloat(3), 0.1);
      rs3.close();

      ResultSet rs4 = readStatement.executeQuery("select * from root.t1.wf01.wt02");
      rs4.next();
      Assert.assertEquals(6000, rs4.getLong(1));
      Assert.assertFalse(rs4.getBoolean(2));
      Assert.assertEquals(22, rs4.getFloat(3), 0.1);
      rs4.close();
    }
  }
}
