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
package org.apache.iotdb.db.it.quotas;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBThrottleQuotaIT {
  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void setThrottleQuotaTest0() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set throttle quota request='10req/sec' on user1;");
      ResultSet resultSet1 = adminStmt.executeQuery("show throttle quota user1;");
      String ans1 = "user1,request,10req\\sec," + ",\n";
      validateResultSet(resultSet1, ans1);

      adminStmt.execute("set throttle quota request='10req/min',type='read' on user1;");
      ResultSet resultSet2 = adminStmt.executeQuery("show throttle quota;");
      String ans2 = "user1,request,10req\\sec," + ",\n" + "user1,request,10req\\min,read" + ",\n";
      validateResultSet(resultSet2, ans2);

      adminStmt.execute("set throttle quota request='10req/hour',type='write' on user1;");
      ResultSet resultSet3 = adminStmt.executeQuery("show throttle quota;");
      String ans3 =
          "user1,request,10req\\sec,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n";
      validateResultSet(resultSet3, ans3);

      adminStmt.execute("set throttle quota request='10req/day' on user1;");
      ResultSet resultSet4 = adminStmt.executeQuery("show throttle quota;");
      String ans4 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n";
      validateResultSet(resultSet4, ans4);

      adminStmt.execute("set throttle quota size='1B/min' on user1;");
      ResultSet resultSet5 = adminStmt.executeQuery("show throttle quota;");
      String ans5 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1B\\min,"
              + ",\n";
      validateResultSet(resultSet5, ans5);

      adminStmt.execute("set throttle quota size='1K/sec',type='read' on user1;");
      ResultSet resultSet6 = adminStmt.executeQuery("show throttle quota;");
      String ans6 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1B\\min,"
              + ",\n"
              + "user1,size,1K\\sec,read"
              + ",\n";
      validateResultSet(resultSet6, ans6);

      adminStmt.execute("set throttle quota size='1M/hour' on user1;");
      ResultSet resultSet7 = adminStmt.executeQuery("show throttle quota;");
      String ans7 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1M\\hour,"
              + ",\n"
              + "user1,size,1K\\sec,read"
              + ",\n";
      validateResultSet(resultSet7, ans7);

      adminStmt.execute("set throttle quota size='1G/min' on user1;");
      ResultSet resultSet8 = adminStmt.executeQuery("show throttle quota;");
      String ans8 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1024M\\min,"
              + ",\n"
              + "user1,size,1K\\sec,read"
              + ",\n";
      validateResultSet(resultSet8, ans8);

      adminStmt.execute("set throttle quota size='1T/hour' on user1;");
      ResultSet resultSet9 = adminStmt.executeQuery("show throttle quota;");
      String ans9 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1048576M\\hour,"
              + ",\n"
              + "user1,size,1K\\sec,read"
              + ",\n";
      validateResultSet(resultSet9, ans9);

      adminStmt.execute("set throttle quota size='1P/min' on user1;");
      ResultSet resultSet10 = adminStmt.executeQuery("show throttle quota;");
      String ans10 =
          "user1,request,10req\\day,"
              + ",\n"
              + "user1,request,10req\\min,read"
              + ",\n"
              + "user1,request,10req\\hour,write"
              + ",\n"
              + "user1,size,1073741824M\\min,"
              + ",\n"
              + "user1,size,1K\\sec,read"
              + ",\n";
      validateResultSet(resultSet10, ans10);
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  @Test
  public void setThrottleQuotaTest1() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set throttle quota cpu=2 on user1;");
      ResultSet resultSet1 = adminStmt.executeQuery("show throttle quota user1;");
      String ans1 = "user1,cpu,2,read" + ",\n";
      validateResultSet(resultSet1, ans1);

      adminStmt.execute("set throttle quota cpu=3 on user1;");
      resultSet1 = adminStmt.executeQuery("show throttle quota user1;");
      ans1 = "user1,cpu,3,read" + ",\n";
      validateResultSet(resultSet1, ans1);

      adminStmt.execute("set throttle quota mem=1G on user1;");
      ResultSet resultSet2 = adminStmt.executeQuery("show throttle quota;");
      String ans2 = "user1,cpu,3,read" + ",\n" + "user1,mem,1024M,read" + ",\n";
      validateResultSet(resultSet2, ans2);

      adminStmt.execute("set throttle quota mem=2G on user1;");
      resultSet2 = adminStmt.executeQuery("show throttle quota;");
      ans2 = "user1,cpu,3,read" + ",\n" + "user1,mem,2048M,read" + ",\n";
      validateResultSet(resultSet2, ans2);
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  @Test
  public void setThrottleQuotaTest2() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER user1 'user1'");
      adminStmt.execute("grant user user1 privileges all");
      adminStmt.execute("set throttle quota request='3req/hour' on user1;");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "user1");
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(1,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(2,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(3,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(4,true)");
      }
    } catch (SQLException throwables) {
      if (!throwables.getMessage().contains("1701: number of requests exceeded - wait")) {
        Assert.fail(throwables.getMessage());
      }
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER user2 'user2'");
      adminStmt.execute("grant user user2 privileges all");
      adminStmt.execute("set throttle quota request='3req/hour',type='read' on user2;");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user2", "user2");
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(1,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(2,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(3,true)");
        userStmt.executeQuery("select status from root.ln.wf02.wt02");
        userStmt.executeQuery("select status from root.ln.wf02.wt02");
        userStmt.executeQuery("select status from root.ln.wf02.wt02");
        userStmt.executeQuery("select status from root.ln.wf02.wt02");
      }
    } catch (SQLException throwables) {
      if (!throwables.getMessage().contains("1703: number of read requests exceeded - wait")) {
        Assert.fail(throwables.getMessage());
      }
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER user3 'user3'");
      adminStmt.execute("grant user user3 privileges all");
      adminStmt.execute("set throttle quota request='3req/hour',type='write' on user3;");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user3", "user3");
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(1,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(2,true)");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(3,true)");
        userStmt.executeQuery("select status from root.ln.wf02.wt02");
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(4,true)");
      }
    } catch (SQLException throwables) {
      if (!throwables.getMessage().contains("1704: number of write requests exceeded - wait")) {
        Assert.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void setThrottleQuotaTest3() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER user1 'user1'");
      adminStmt.execute("grant user user1 privileges all");
      adminStmt.execute("set throttle quota request='1req/min' on user1;");
      try (Connection userCon = EnvFactory.getEnv().getConnection("user1", "user1");
          Statement userStmt = userCon.createStatement()) {
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(1,true)");
        Thread.sleep(60000);
        userStmt.execute("insert into root.ln.wf02.wt02(timestamp,status) values(2,true)");
      }
    } catch (SQLException | InterruptedException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  private void validateResultSet(ResultSet set, String ans) throws SQLException {
    try {
      StringBuilder builder = new StringBuilder();
      ResultSetMetaData metaData = set.getMetaData();
      int colNum = metaData.getColumnCount();
      while (set.next()) {
        for (int i = 1; i <= colNum; i++) {
          builder.append(set.getString(i)).append(",");
        }
        builder.append("\n");
      }
      String result = builder.toString();
      assertEquals(ans.length(), result.length());
      List<String> ansLines = Arrays.asList(ans.split("\n"));
      List<String> resultLines = Arrays.asList(result.split("\n"));
      assertEquals(ansLines.size(), resultLines.size());
      for (String resultLine : resultLines) {
        assertTrue(ansLines.contains(resultLine));
      }
    } finally {
      set.close();
    }
  }
}
