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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;
import org.apache.iotdb.jdbc.IoTDBConnection;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBDatetimeFormatIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void testDatetimeInputFormat() {
    String[] datetimeStrings = {
      "2022-01-01 01:02:03", // yyyy-MM-dd HH:mm:ss
      "2022/01/02 01:02:03", // yyyy/MM/dd HH:mm:ss
      "2022.01.03 01:02:03", // yyyy.MM.dd HH:mm:ss
      "2022-01-04 01:02:03+01:00", // yyyy-MM-dd HH:mm:ssZZ
      "2022/01/05 01:02:03+01:00", // yyyy/MM/dd HH:mm:ssZZ
      "2022.01.06 01:02:03+01:00", // yyyy.MM.dd HH:mm:ssZZ
      "2022-01-07 01:02:03.400", // yyyy-MM-dd HH:mm:ss.SSS
      "2022/01/08 01:02:03.400", // yyyy/MM/dd HH:mm:ss.SSS
      "2022.01.09 01:02:03.400", // yyyy.MM.dd HH:mm:ss.SSS
      "2022-01-10 01:02:03.400+01:00", // yyyy-MM-dd HH:mm:ss.SSSZZ
      "2022-01-11 01:02:03.400+01:00", // yyyy/MM/dd HH:mm:ss.SSSZZ
      "2022-01-12 01:02:03.400+01:00", // yyyy.MM.dd HH:mm:ss.SSSZZ
      "2022-01-13T01:02:03.400+01:00" // ISO8601 standard time format
    };
    long[] timestamps = {
      1640970123000L,
      1641056523000L,
      1641142923000L,
      1641254523000L,
      1641340923000L,
      1641427323000L,
      1641488523400L,
      1641574923400L,
      1641661323400L,
      1641772923400L,
      1641859323400L,
      1641945723400L,
      1642032123400L
    };
    try (IoTDBConnection connection = (IoTDBConnection) EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      connection.setTimeZone("+08:00");

      for (int i = 0; i < datetimeStrings.length; i++) {
        String insertSql =
            String.format(
                "INSERT INTO root.sg1.d1(time, s1) values (%s, %d)", datetimeStrings[i], i);
        statement.execute(insertSql);
      }

      boolean hasResult = statement.execute("SELECT s1 FROM root.sg1.d1");
      Assert.assertTrue(hasResult);

      int cnt = 0;
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertEquals(timestamps[cnt], resultSet.getLong(1));
        cnt++;
      }
      Assert.assertEquals(timestamps.length, cnt);
    } catch (SQLException | TException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testBigDateTime() {
    try (IoTDBConnection connection = (IoTDBConnection) EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg");

      statement.execute("CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=DOUBLE, ENCODING=PLAIN;");

      statement.execute("insert into root.sg.d1(time,s2) values (1618283005586000, 8.76);");
      statement.execute("select * from root.sg.d1;");
      statement.execute("select * from root.sg.d1 where time=53251-05-07T17:06:26.000+08:00");
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
