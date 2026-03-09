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
package org.apache.iotdb.relational.it.query.view.old;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.TSDataType;
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
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSimpleQueryTableViewIT {
  private static final String DATABASE_NAME = "test";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateTimeseries1() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

    } catch (SQLException e) {
      e.printStackTrace();
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE VIEW table1(device STRING TAG, s1 INT32 FIELD) as root.sg1.**");

      try (ResultSet resultSet = statement.executeQuery("describe table1")) {
        if (resultSet.next()
            && resultSet.getString(ColumnHeaderConstant.COLUMN_NAME).equals("s1")) {
          assertEquals("INT32", resultSet.getString(ColumnHeaderConstant.DATATYPE).toUpperCase());
          assertEquals(
              "FIELD", resultSet.getString(ColumnHeaderConstant.COLUMN_CATEGORY).toUpperCase());
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testOrderByTimeDesc() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (2, 2)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (3, 3)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (4, 4)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (3, 3)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (1, 1)");
      statement.execute("flush");
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s0 INT32 FIELD, s1 INT32 FIELD) as root.sg1.**");
      statement.execute("flush");

      String[] expectedHeader = new String[] {"time", "device", "s0", "s1"};
      String[] ret =
          new String[] {
            defaultFormatDataTime(4) + ",d0,4,null,",
            defaultFormatDataTime(3) + ",d0,3,3,",
            defaultFormatDataTime(2) + ",d0,2,null,",
            defaultFormatDataTime(1) + ",d0,1,1,",
          };

      tableResultSetEqualTest(
          "select * from table1 order by time desc", expectedHeader, ret, DATABASE_NAME);
    }
  }

  @Test
  public void testShowTimeseriesDataSet1() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 INT32 FIELD, s4 INT32 FIELD, s5 INT32 FIELD, s6 INT32 FIELD, s7 INT32 FIELD, s8 INT32 FIELD, s9 INT32 FIELD, s10 INT32 FIELD) as root.sg1.**");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("describe table1")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(12, count);

    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testShowTimeseriesDataSet2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(10);
      statement.execute("CREATE DATABASE root.sg1");
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
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(10);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 INT32 FIELD, s4 INT32 FIELD, s5 INT32 FIELD, s6 INT32 FIELD, s7 INT32 FIELD, s8 INT32 FIELD, s9 INT32 FIELD, s10 INT32 FIELD) as root.sg1.**");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("describe table1")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(12, count);

    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testShowTimeseriesDataSet3() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(15);
      statement.execute("CREATE DATABASE root.sg1");
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
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(15);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s1 INT32 FIELD, s2 INT32 FIELD, s3 INT32 FIELD, s4 INT32 FIELD, s5 INT32 FIELD, s6 INT32 FIELD, s7 INT32 FIELD, s8 INT32 FIELD, s9 INT32 FIELD, s10 INT32 FIELD) as root.sg1.**");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("describe table1")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(12, count);

    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testFirstOverlappedPageFiltered() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE VIEW table1(device STRING TAG, s0 INT32 FIELD) as root.sg1.**");

      long count = 0;
      try (ResultSet resultSet = statement.executeQuery("select s0 from table1 where s0 > 18")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(1, count);
    }
  }

  @Test
  public void testPartialInsertion() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      try {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s0, s1) VALUES (1, 1, 2.2)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("s1"));
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s0 INT32 FIELD, s1 INT32 FIELD) as root.sg1.**");

      try (ResultSet resultSet = statement.executeQuery("select s0, s1 from table1")) {
        while (resultSet.next()) {
          assertEquals(1, resultSet.getInt("s0"));
          assertEquals(null, resultSet.getString("s1"));
        }
      }
    }
  }

  @Test
  public void testOverlappedPagesMerge() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE VIEW table1(device STRING TAG, s0 INT32 FIELD) as root.sg1.**");

      long count = 0;

      try (ResultSet resultSet = statement.executeQuery("select s0 from table1 where s0 < 100")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(4, count);
    }
  }

  @Test
  public void testTimeseriesMetadataCache() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      for (int i = 0; i < 10000; i++) {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d0.s" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
      }
      for (int i = 1; i < 10000; i++) {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s" + i + ") VALUES (1000, 1)");
      }
      statement.execute("flush");
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      StringBuilder createTableBuilder = new StringBuilder();
      createTableBuilder.append("CREATE VIEW table1(device STRING TAG,");
      for (int i = 0; i < 1000; i++) {
        String columnName = "s" + i;
        createTableBuilder.append(columnName).append(" INT32 FIELD,");
      }
      createTableBuilder
          .deleteCharAt(createTableBuilder.lastIndexOf(","))
          .append(") as root.sg1.**");
      statement.execute(createTableBuilder.toString());
      statement.executeQuery("select s0 from table1");
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testUseSameStatement() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s0 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");

      statement.execute("insert into root.sg1.d0(timestamp,s0,s1) values(1,1,1)");
      statement.execute("insert into root.sg1.d1(timestamp,s0,s1) values(1000,1000,1000)");
      statement.execute("insert into root.sg1.d0(timestamp,s0,s1) values(10,10,10)");
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s0 INT64 FIELD, s1 INT64 FIELD) as root.sg1.**");

      List<ResultSet> resultSetList = new ArrayList<>();

      ResultSet r1 = statement.executeQuery("select * from table1 where device='d0' and time <= 1");
      resultSetList.add(r1);

      ResultSet r2 = statement.executeQuery("select * from table1 where device='d1' and s0 = 1000");
      resultSetList.add(r2);

      ResultSet r3 = statement.executeQuery("select * from table1 where device='d0' and s1 = 10");
      resultSetList.add(r3);

      r1.next();
      Assert.assertEquals(r1.getLong(1), 1L);
      Assert.assertEquals(r1.getLong(3), 1L);
      Assert.assertEquals(r1.getLong(4), 1L);

      r2.next();
      Assert.assertEquals(r2.getLong(1), 1000L);
      Assert.assertEquals(r2.getLong(3), 1000L);
      Assert.assertEquals(r2.getLong(4), 1000L);

      r3.next();
      Assert.assertEquals(r3.getLong(1), 10L);
      Assert.assertEquals(r3.getLong(3), 10L);
      Assert.assertEquals(r3.getLong(4), 10L);
    }
  }

  @Test
  public void testEnableAlign() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=BOOLEAN");
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s1 INT32 FIELD, s2 BOOLEAN FIELD) as root.sg1.**");
      ResultSet resultSet = statement.executeQuery("select time, s1, s2 from table1");
      ResultSetMetaData metaData = resultSet.getMetaData();
      int[] types = {Types.TIMESTAMP, Types.INTEGER, Types.BOOLEAN};
      int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        Assert.assertEquals(types[i], metaData.getColumnType(i + 1));
      }
    }
  }

  @Test
  public void testNewDataType() throws SQLException {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s4 WITH DATATYPE=DATE, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s5 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s6 WITH DATATYPE=BLOB, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s7 WITH DATATYPE=STRING, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "insert into root.sg1.d1(timestamp, s4, s5, s6, s7) values(%d, \"%s\", %d, %s, \"%s\")",
                i, LocalDate.of(2024, 5, i % 31 + 1), i, "X'cafebabe'", i));
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE VIEW table1(device STRING TAG, s4 DATE FIELD, s5 TIMESTAMP FIELD, s6 BLOB FIELD, s7 STRING FIELD) as root.sg1.**");

      try (ResultSet resultSet = statement.executeQuery("select * from table1")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        assertEquals(6, columnCount);
        HashMap<Integer, TSDataType> columnType = new HashMap<>();
        for (int i = 3; i <= columnCount; i++) {
          if (metaData.getColumnLabel(i).equals("s4")) {
            columnType.put(i, TSDataType.DATE);
          } else if (metaData.getColumnLabel(i).equals("s5")) {
            columnType.put(i, TSDataType.TIMESTAMP);
          } else if (metaData.getColumnLabel(i).equals("s6")) {
            columnType.put(i, TSDataType.BLOB);
          } else if (metaData.getColumnLabel(i).equals("s7")) {
            columnType.put(i, TSDataType.TEXT);
          }
        }
        byte[] byteArray = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        while (resultSet.next()) {
          long time = resultSet.getLong(1);
          Date date = resultSet.getDate(3);
          long timestamp = resultSet.getLong(4);
          byte[] blob = resultSet.getBytes(5);
          String text = resultSet.getString(6);
          assertEquals(2024 - 1900, date.getYear());
          assertEquals(5 - 1, date.getMonth());
          assertEquals(time % 31 + 1, date.getDate());
          assertEquals(time, timestamp);
          assertArrayEquals(byteArray, blob);
          assertEquals(String.valueOf(time), text);
        }
      }

    } catch (SQLException e) {
      fail();
    }
  }
}
