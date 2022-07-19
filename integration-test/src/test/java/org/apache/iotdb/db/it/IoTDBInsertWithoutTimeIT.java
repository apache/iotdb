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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertWithoutTimeIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;
  private static Statement statement;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
    connection = EnvFactory.getEnv().getConnection();
    statement = connection.createStatement();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvFactory.getEnv().cleanAfterTest();
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Statement statement = connection.createStatement();

    sqls.add("SET STORAGE GROUP TO root.t1");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.s1 WITH DATATYPE=FLOAT, ENCODING=RLE");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.s2 WITH DATATYPE=TEXT, ENCODING=PLAIN");

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Test
  public void testInsertWithoutTime() throws SQLException, InterruptedException {
    statement.execute("insert into root.t1.wf01.wt01(time, s1, temperature) values (1, 1.1, 11)");
    statement.execute(
        "insert into root.t1.wf01.wt01(time, s1, temperature) values (2, 2.2, 22),(3, 3.3, 33)");
    statement.execute("insert into root.t1.wf01.wt01(s1, temperature) values (2, 10)");
    Thread.sleep(3);
    statement.execute("insert into root.t1.wf01.wt01(s1) values (6.6)");

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(s1) from root.t1.wf01.wt01");
    rs1.next();
    long countStatus = rs1.getLong(1);
    Assert.assertEquals(countStatus, 5L);

    st1.close();
  }

  @Test
  public void testInsertWithoutTimestamp() throws SQLException {

    statement.execute("insert into root.sg.d1(s1,s2) values(25,'test')");

    ResultSet resultSet = statement.executeQuery("select * from root.**");

    Float values = 25.0F;
    String values2 = "test";

    while (resultSet.next()) {
      if (values == resultSet.getFloat("root.sg.d1.s1")) {
        assertEquals(values2, resultSet.getString("root.sg.d1.s2"));
      }
      ;
    }
  }
}
