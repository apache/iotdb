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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class IoTDBCompressTypeIT {
  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testGZIPCompression() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.name WITH DATATYPE=TEXT");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt01.age WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSOR = GZIP");

      statement.execute(
          "insert into root.ln.wf01.wt01(timestamp,name,age) values(1000,'zhang',10)");
      statement.execute("flush");

      ResultSet r1 = statement.executeQuery("select * from root.ln.wf01.wt01");
      r1.next();

      Assert.assertEquals("zhang", r1.getString(2));
      Assert.assertEquals(10, r1.getInt(3));

      statement.execute("insert into root.ln.wf01.wt01(timestamp,name,age) values(2000,'wang',20)");
      statement.execute("flush");
      statement.execute("insert into root.ln.wf01.wt01(timestamp,name,age) values(3000,'li',30)");

      ResultSet r2 = statement.executeQuery("select * from root.ln.wf01.wt01 where name = 'wang'");
      r2.next();
      Assert.assertEquals(20, r2.getInt(3));

      ResultSet r3 = statement.executeQuery("select * from root.ln.wf01.wt01 where name = 'li'");
      r3.next();
      Assert.assertEquals(30, r3.getInt(3));

      ResultSet r4 = statement.executeQuery("select sum(age) from root.ln.wf01.wt01");
      r4.next();
      double d = r4.getDouble(1);

      Assert.assertTrue(60.0 == d);

      // now we try to insert more values
      for (int i = 1; i <= 1000; i++) {
        String time = String.valueOf(i * 100);
        String values = String.valueOf(i * 10);
        statement.execute(
            String.format(
                "insert into root.ln.wf01.wt01(timestamp,name,age) values(%s,'wang', %s)",
                time, values));
      }

      statement.execute("flush");
      ResultSet r5 =
          statement.executeQuery("select * from root.ln.wf01.wt01 where timestamp = 100000");
      r5.next();
      Assert.assertEquals(10000, r5.getInt(3));
    }
  }
}
