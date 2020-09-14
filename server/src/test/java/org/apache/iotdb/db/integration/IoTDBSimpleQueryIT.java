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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class IoTDBSimpleQueryIT {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreatTimeseries() throws SQLException, ClassNotFoundException, MetadataException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    MeasurementMNode mNode = (MeasurementMNode) MManager.getInstance().getNodeByPath(new PartialPath(
        "root.sg1.d0.s1"));
    assertNull(mNode.getSchema().getProps());
  }

  @Test
  public void testEmptyDataSet() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()){

      ResultSet resultSet = statement.executeQuery("select * from root");
      // has an empty time column
      Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
      try {
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 group by ([0, 100), 20ms, 20ms)");
        // has an empty time column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root");
        // has no column
        Assert.assertEquals(0, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select * from root align by device");
        // has time and device columns
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root align by device");
        // has device column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 "
                        + "group by ([0, 100), 20ms, 20ms) align by device");
        // has time and device columns
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }
      } finally {
        resultSet.close();
      }

      resultSet.close();
    }
  }

  @Test
  public void testShowTimeseriesDataSet1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");


      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("show timeseries");

      int count = 0;
      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesDataSet2() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.setFetchSize(10);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");


      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("show timeseries");

      int count = 0;
      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesDataSet3() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.setFetchSize(15);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");


      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("show timeseries");

      int count = 0;
      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testShowTimeseriesDataSet4() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");


      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("show timeseries limit 8");

      int count = 0;
      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(8, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesWithLimitOffset() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()){

      String[] exps = new String[]{"root.sg1.d0.s2", "root.sg1.d0.s3"};

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s4) VALUES (5, 5)");

      ResultSet resultSet = statement.executeQuery("show timeseries limit 2 offset 1");

      int count = 0;
      try {
        while (resultSet.next()) {
          Assert.assertEquals(exps[count++], resultSet.getString(1));
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(exps.length, count);
      resultSet.close();
    }
  }

  @Test
  public void testFirstOverlappedPageFiltered() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq chunk : [1,10]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (10, 10)");

      statement.execute("flush");

      // seq chunk : [13,20]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (13, 13)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (20, 20)");

      statement.execute("flush");

      // unseq chunk : [5,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 15)");

      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0 where s0 > 18");

      long count = 0;
      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(1, count);
    }
  }


  @Test
  public void testPartialInsertion() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      try {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s0, s1) VALUES (1, 1, 2.2)");
        fail();
      } catch (IoTDBSQLException e) {
        assertTrue(e.getMessage().contains("s1"));
      }

      ResultSet resultSet = statement.executeQuery("select s0, s1 from root.sg1.d0");

      try {
        while (resultSet.next()) {
          assertEquals(1, resultSet.getInt("root.sg1.d0.s0"));
          assertEquals(null, resultSet.getString("root.sg1.d0.s1"));
        }
      } finally {
        resultSet.close();
      }
    }
  }


  @Test
  public void testPartialInsertionAllFailed() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    boolean autoCreateSchemaEnabled = IoTDBDescriptor.getInstance().getConfig()
        .isAutoCreateSchemaEnabled();
    boolean enablePartialInsert = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();

    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);

      statement.execute("SET STORAGE GROUP TO root.sg1");

      try {
        statement.execute("INSERT INTO root.sg1(timestamp, s0) VALUES (1, 1)");
        fail();
      } catch (IoTDBSQLException e) {
        assertTrue(e.getMessage().contains("s0"));
      }
    }

    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(enablePartialInsert);
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(autoCreateSchemaEnabled);

  }

  @Test
  public void testOverlappedPagesMerge() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq chunk : start-end [1000, 1000]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1000, 0)");

      statement.execute("flush");

      // unseq chunk : [1,10]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (10, 10)");

      statement.execute("flush");

      // usneq chunk : [5,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 15)");

      statement.execute("flush");

      // unseq chunk : [15,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 150)");

      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0 where s0 < 100");

      long count = 0;

      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      Assert.assertEquals(4, count);
    }
  }

  @Test
  public void testUnseqUnsealedDeleteQuery() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try(Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()){
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq data
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1000, 1)");
      statement.execute("flush");

      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("flush");

      // unseq data
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("delete from root.sg1.d0.s0 where time <= 15");

      ResultSet resultSet = statement.executeQuery("select * from root");

      long count = 0;

      try {
        while (resultSet.next()) {
          count++;
        }
      } finally {
        resultSet.close();
      }

      System.out.println(count);

    }
  }

  @Test
  public void testTimeseriesMetadataCache() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      for (int i = 0; i < 10000; i++) {
        statement.execute("CREATE TIMESERIES root.sg1.d0.s"+ i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
      }
      for (int i = 1; i < 10000; i++) {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s" + i + ") VALUES (1000, 1)");
      }
      statement.execute("flush");
      statement.executeQuery("select s0 from root.sg1.d0");
    } catch (SQLException e) {
      fail();
    }
  }


  @Test
  public void testInvalidSchema() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/",
            "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s1 with datatype=BOOLEAN, encoding=TS_2DIFF");
      } catch (Exception e) {
        Assert.assertEquals("303: org.apache.iotdb.db.exception.metadata.MetadataException: encoding BOOLEAN does not support TS_2DIFF", e.getMessage());
      }

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s2 with datatype=INT32, encoding=GORILLA");
      } catch (Exception e) {
        Assert.assertEquals("303: org.apache.iotdb.db.exception.metadata.MetadataException: encoding INT32 does not support GORILLA", e.getMessage());
      }

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s3 with datatype=DOUBLE, encoding=REGULAR");
      } catch (Exception e) {
        Assert.assertEquals("303: org.apache.iotdb.db.exception.metadata.MetadataException: encoding DOUBLE does not support REGULAR", e.getMessage());
      }

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s4 with datatype=TEXT, encoding=TS_2DIFF");
      } catch (Exception e) {
        Assert.assertEquals("303: org.apache.iotdb.db.exception.metadata.MetadataException: encoding TEXT does not support TS_2DIFF", e.getMessage());
      }


    } catch (SQLException e) {
      fail();
    }
  }
}
