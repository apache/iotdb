package org.apache.iotdb.db.it.quotas;

import org.apache.iotdb.commons.conf.CommonDescriptor;
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
public class IoTDBQuotaIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    CommonDescriptor.getInstance().getConfig().setQuotaEnable(true);
  }

  @After
  public void tearDown() throws Exception {
    CommonDescriptor.getInstance().getConfig().setQuotaEnable(false);
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void setSpaceQuotaTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertTrue(
          adminStmt.execute("set space quota devices=3,timeseries=5,disk='100M' on root.sg0;"));

      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt02.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt03.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertFalse(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt04.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status1 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status2 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertFalse(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status3 with datatype=BOOLEAN,encoding=PLAIN;"));

      Assert.assertFalse(adminStmt.execute("set space quota devices=0 on root.sg0;"));
      Assert.assertFalse(adminStmt.execute("set space quota timeseries=0 on root.sg0;"));
      Assert.assertFalse(adminStmt.execute("set space quota disk='0M' on root.sg0;"));

      Assert.assertTrue(
          adminStmt.execute(
              "set space quota devices='unlimited',timeseries='unlimited',disk='unlimited' on root.sg0;"));

      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg0.wf01.wt01.status3 with datatype=BOOLEAN,encoding=PLAIN;"));
    }
  }

  @Test
  public void showSpaceQuotaTest() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertTrue(
          adminStmt.execute(
              "set space quota devices=3,timeseries=5,disk='100M' on root.sg1,root.sg2;"));

      ResultSet resultSet1 = adminStmt.executeQuery("show space quota;");

      String ans1 =
          "root.sg1,diskSize,100M,0M"
              + ",\n"
              + "root.sg1,deviceNum,3,0"
              + ",\n"
              + "root.sg1,timeSeriesNum,5,0"
              + ",\n"
              + "root.sg2,diskSize,100M,0M"
              + ",\n"
              + "root.sg2,deviceNum,3,0"
              + ",\n"
              + "root.sg2,timeSeriesNum,5,0"
              + ",\n";
      validateResultSet(resultSet1, ans1);

      Assert.assertTrue(
          adminStmt.execute(
              "set space quota devices='unlimited',timeseries='unlimited',disk='unlimited' on root.sg1;"));

      ResultSet resultSet2 = adminStmt.executeQuery("show space quota root.sg1;");
      String ans2 =
          "root.sg1,diskSize,unlimited,0M"
              + ",\n"
              + "root.sg1,deviceNum,unlimited,0"
              + ",\n"
              + "root.sg1,timeSeriesNum,unlimited,0"
              + ",\n";
      validateResultSet(resultSet2, ans2);
    }
  }

  @Test
  public void setSpaceQuotaTest2() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertTrue(
          adminStmt.execute("set space quota devices=3,timeseries=5,disk='100M' on root.sg3;"));

      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg3.wf01.wt01.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg3.wf01.wt02.status0 with datatype=BOOLEAN,encoding=PLAIN;"));
      Assert.assertTrue(
          adminStmt.execute(
              "create timeseries root.sg3.wf01.wt02.status1 with datatype=BOOLEAN,encoding=PLAIN;"));

      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg3;");

      String ans1 =
          "root.sg3,diskSize,100M,0M"
              + ",\n"
              + "root.sg3,deviceNum,3,2"
              + ",\n"
              + "root.sg3,timeSeriesNum,5,3"
              + ",\n";
      validateResultSet(resultSet1, ans1);
    }
  }

  @Test
  public void showSpaceQuotaTest2() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      Assert.assertTrue(
          adminStmt.execute(
              "set space quota devices=3,timeseries=5,disk='100M' on root.sg4,root.sg5;"));

      ResultSet resultSet1 = adminStmt.executeQuery("show space quota root.sg4,root.sg5;");

      String ans1 =
          "root.sg4,diskSize,100M,0M"
              + ",\n"
              + "root.sg4,deviceNum,3,0"
              + ",\n"
              + "root.sg4,timeSeriesNum,5,0"
              + ",\n"
              + "root.sg5,diskSize,100M,0M"
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
