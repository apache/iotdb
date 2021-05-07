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
  public abstract void setUp() throws Exception;

  public void tearDown() throws Exception {
    writeStatement.close();
    writeConnection.close();
    for (Statement statement : readStatements) {
      statement.close();
    }
    for (Connection connection : readConnections) {
      connection.close();
    }
  }

  @Test
  public void testSimplePutAndGet() throws SQLException {

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
    writeStatement.execute("insert into root.ln.wf01.wt01(time, temperature) values(10, 1.0)");
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select avg(temperature) from root.ln.wf01.wt01");
      if (resultSet.next()) {
        Assert.assertEquals(1.0, resultSet.getDouble(1), 0.01);
      } else {
        Assert.fail("expect 1 result, but get an empty resultSet.");
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }
  }
}
