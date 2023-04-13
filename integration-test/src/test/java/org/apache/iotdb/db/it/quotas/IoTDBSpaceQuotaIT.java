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
public class IoTDBSpaceQuotaIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(true);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeCommonConfig().setQuotaEnable(false);
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void timeseriesNumExceedTest() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=3 on root.sg0");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status2 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status3 with datatype=BOOLEAN,encoding=PLAIN;");
    } catch (SQLException | InterruptedException throwables) {
      Assert.assertEquals(
          "301: The used quota exceeds the preset quota. Please set a larger value.",
          throwables.getMessage());
    }
  }

  @Test
  public void devicesNumExceedTest() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3 on root.sg0");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf02.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf03.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute(
          "create timeseries root.sg0.wf04.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
    } catch (SQLException | InterruptedException throwables) {
      Assert.assertEquals(
          "301: The used quota exceeds the preset quota. Please set a larger value.",
          throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest0() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=3 on root.sg0");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status2 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute("set space quota timeseries=4 on root.sg0");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status3 with datatype=BOOLEAN,encoding=PLAIN;");
    } catch (SQLException | InterruptedException throwables) {
      Assert.fail(throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3 on root.sg1");
      adminStmt.execute(
          "create timeseries root.sg1.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg1.wf02.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg1.wf03.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute("set space quota devices=4 on root.sg1");
      adminStmt.execute(
          "create timeseries root.sg1.wf04.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
    } catch (SQLException | InterruptedException throwables) {
      Assert.fail(throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota diskSize=100M on root.sg2");
      adminStmt.execute("set space quota diskSize=200M on root.sg2");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest1() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=3 on root.sg0");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status2 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute("set space quota timeseries=2 on root.sg0");
    } catch (SQLException | InterruptedException throwables) {
      Assert.assertEquals(
          "301: The used quota exceeds the preset quota. Please set a larger value.",
          throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3 on root.sg1");
      adminStmt.execute(
          "create timeseries root.sg1.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg1.wf02.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg1.wf03.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(1000);
      adminStmt.execute("set space quota devices=2 on root.sg1");
    } catch (SQLException | InterruptedException throwables) {
      Assert.assertEquals(
          "301: The used quota exceeds the preset quota. Please set a larger value.",
          throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest2() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=0 on root.sg0");
    } catch (SQLException throwables) {
      Assert.assertEquals(
          "701: Please set the number of timeseries greater than 0", throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=0 on root.sg1");
    } catch (SQLException throwables) {
      Assert.assertEquals(
          "701: Please set the number of devices greater than 0", throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk=0M on root.sg2");
    } catch (SQLException throwables) {
      Assert.assertEquals("701: Please set the disk size greater than 0", throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=0,devices=0,disk=0M on root.sg3");
    } catch (SQLException throwables) {
      Assert.assertEquals(
          "701: Please set the number of devices greater than 0", throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest3() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=2 on root.sg0");
      adminStmt.execute("set space quota timeseries='unlimited' on root.sg0");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=2 on root.sg1");
      adminStmt.execute("set space quota devices='unlimited' on root.sg1");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk=10M on root.sg2");
      adminStmt.execute("set space quota disk='unlimited' on root.sg2");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }

    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota timeseries=2,devices=2,disk=10M on root.sg3");
      adminStmt.execute(
          "set space quota timeseries='unlimited',devices='unlimited',disk='unlimited' on root.sg3");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest4() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3,timeseries=5 on root.sg0;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt02.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt03.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt04.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status2 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status3 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute("set space quota devices=4,timeseries=6 on root.sg0;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt05.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status5 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute("set space quota devices='unlimited',timeseries='unlimited' on root.sg0;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt06.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status6 with datatype=BOOLEAN,encoding=PLAIN;");
    } catch (SQLException throwables) {
      Assert.fail(throwables.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest5() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3,timeseries=5,disk='100M' on root.sg0;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt02.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt02.status1 with datatype=BOOLEAN,encoding=PLAIN;");
      Thread.sleep(2000);
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg0;");
      String ans1 =
          "root.sg0,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg0,deviceNum,3,2"
              + ",\n"
              + "root.sg0,timeSeriesNum,5,3"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    } catch (InterruptedException | SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest6() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk='100K' on root.sg0;");
    } catch (SQLException e) {
      Assert.assertEquals(
          "701: When setting the disk size, the unit is incorrect. Please use 'M', 'G', 'P', 'T' as the unit",
          e.getMessage());
    }
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk='100M' on root.sg0;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg0;");
      String ans1 =
          "root.sg0,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg0,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg0,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk='100G' on root.sg1;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg1;");
      String ans1 =
          "root.sg1,diskSize,100.0G,0.0G"
              + ",\n"
              + "root.sg1,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg1,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk='100T' on root.sg2;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg2;");
      String ans1 =
          "root.sg2,diskSize,102400.0G,0.0G"
              + ",\n"
              + "root.sg2,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg2,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota disk='100P' on root.sg3;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg3;");
      String ans1 =
          "root.sg3,diskSize,1.048576E8G,0.0G"
              + ",\n"
              + "root.sg3,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg3,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void setSpaceQuotaTest7() {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute(
          "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf02.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf03.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf04.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute(
          "create timeseries root.sg0.wf05.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;");
      adminStmt.execute("set space quota devices=5 on root.sg0;");
      adminStmt.execute("set space quota disk='5g' on root.sg0;");
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void showSpaceQuotaTest0() throws SQLException {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3,timeseries=5,disk='100M' on root.sg1,root.sg2;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota;");
      String ans1 =
          "root.sg1,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg1,deviceNum,3,0"
              + ",\n"
              + "root.sg1,timeSeriesNum,5,0"
              + ",\n"
              + "root.sg2,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg2,deviceNum,3,0"
              + ",\n"
              + "root.sg2,timeSeriesNum,5,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
      adminStmt.execute(
          "set space quota devices='unlimited',timeseries='unlimited',disk='unlimited' on root.sg1;");
      ResultSet resultSet2 = adminStmt.executeQuery("show space quota root.sg1;");
      String ans2 =
          "root.sg1,diskSize,unlimited,0.0G"
              + ",\n"
              + "root.sg1,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg1,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet2, ans2);
      IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
    }
  }

  @Test
  public void showSpaceQuotaTest1() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("set space quota devices=3,timeseries=5,disk='100M' on root.sg4,root.sg5;");
      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg4,root.sg5;");
      String ans1 =
          "root.sg4,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg4,deviceNum,3,0"
              + ",\n"
              + "root.sg4,timeSeriesNum,5,0"
              + ",\n"
              + "root.sg5,diskSize,0.09765625G,0.0G"
              + ",\n"
              + "root.sg5,deviceNum,3,0"
              + ",\n"
              + "root.sg5,timeSeriesNum,5,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);
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
