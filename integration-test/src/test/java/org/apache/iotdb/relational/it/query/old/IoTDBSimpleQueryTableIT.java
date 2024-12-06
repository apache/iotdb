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
package org.apache.iotdb.relational.it.query.old;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSimpleQueryTableIT {
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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT)");

      try (ResultSet resultSet = statement.executeQuery("describe table1")) {
        if (resultSet.next()
            && resultSet.getString(ColumnHeaderConstant.COLUMN_NAME).equals("s1")) {
          assertEquals("INT32", resultSet.getString(ColumnHeaderConstant.DATATYPE).toUpperCase());
          assertEquals(
              "MEASUREMENT",
              resultSet.getString(ColumnHeaderConstant.COLUMN_CATEGORY).toUpperCase());
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Ignore // TODO After last query supported
  @Test
  public void testLastQueryNonCached() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine.d1.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d1.s2 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d2.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute("insert into root.turbine.d1(timestamp,s1,s2) values(1,1,2)");

      List<String> expected = Arrays.asList("root.turbine.d1.s1", "root.turbine.d1.s2");
      List<String> actual = new ArrayList<>();

      try (ResultSet resultSet = statement.executeQuery("select last ** from root")) {
        while (resultSet.next()) {
          actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
      }

      assertEquals(expected, actual);

      actual.clear();
      try (ResultSet resultSet = statement.executeQuery("select last * from root")) {
        while (resultSet.next()) {
          actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
      }

      assertEquals(Collections.emptyList(), actual);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Ignore // TODO After Aggregation supported
  @Test
  public void testEmptyDataSet() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("select * from root.**");
      // has an empty time column
      Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
      try {
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 group by ([0, 100), 20ms, 20ms)");
        // has an empty time column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root");
        // has no column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select * from root.** align by device");
        // has time and device columns
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root align by device");
        // has device column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 "
                    + "group by ([0, 100), 20ms, 20ms) align by device");
        // has time and device columns
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
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
  public void testOrderByTimeDesc() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT, s1 INT32 MEASUREMENT)");
      statement.execute("INSERT INTO table1(device, time, s0) VALUES ('d0', 1, 1)");
      statement.execute("INSERT INTO table1(device, time, s0) VALUES ('d0',2, 2)");
      statement.execute("INSERT INTO table1(device, time, s0) VALUES ('d0',3, 3)");
      statement.execute("INSERT INTO table1(device, time, s0) VALUES ('d0',4, 4)");
      statement.execute("INSERT INTO table1(device, time, s1) VALUES ('d0',3, 3)");
      statement.execute("INSERT INTO table1(device, time, s1) VALUES ('d0',1, 1)");
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
  public void testShowTimeseriesDataSet1() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT32 MEASUREMENT, s4 INT32 MEASUREMENT, s5 INT32 MEASUREMENT, s6 INT32 MEASUREMENT, s7 INT32 MEASUREMENT, s8 INT32 MEASUREMENT, s9 INT32 MEASUREMENT, s10 INT32 MEASUREMENT)");

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
    }
  }

  @Test
  public void testShowTimeseriesDataSet2() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(10);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT32 MEASUREMENT, s4 INT32 MEASUREMENT, s5 INT32 MEASUREMENT, s6 INT32 MEASUREMENT, s7 INT32 MEASUREMENT, s8 INT32 MEASUREMENT, s9 INT32 MEASUREMENT, s10 INT32 MEASUREMENT)");

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
    }
  }

  @Test
  public void testShowTimeseriesDataSet3() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(15);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT32 MEASUREMENT, s4 INT32 MEASUREMENT, s5 INT32 MEASUREMENT, s6 INT32 MEASUREMENT, s7 INT32 MEASUREMENT, s8 INT32 MEASUREMENT, s9 INT32 MEASUREMENT, s10 INT32 MEASUREMENT)");

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
    }
  }

  @Test
  public void testShowTimeseriesDataSet4() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT32 MEASUREMENT, s4 INT32 MEASUREMENT, s5 INT32 MEASUREMENT, s6 INT32 MEASUREMENT, s7 INT32 MEASUREMENT, s8 INT32 MEASUREMENT, s9 INT32 MEASUREMENT, s10 INT32 MEASUREMENT)");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("describe table1 limit 8")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(8, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Ignore // TODO After describe limit offset supported
  @Test
  public void testDescribeWithLimitOffset() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT32 MEASUREMENT, s4 INT32 MEASUREMENT)");

      Set<String> exps = ImmutableSet.of("device", "s1");
      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("describe table1 offset 1 limit 2")) {
        while (resultSet.next()) {
          Assert.assertTrue(exps.contains(resultSet.getString(1)));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Ignore // TODO After DISTINCT supported
  @Test
  public void testShowDevicesWithLimitOffset() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      List<String> exps = Arrays.asList("root.sg1.d1,false", "root.sg1.d2,false");
      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2 offset 1")) {
        while (resultSet.next()) {
          Assert.assertEquals(
              exps.get(count), resultSet.getString(1) + "," + resultSet.getString(2));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Ignore // TODO After DISTINCT supported
  @Test
  public void testShowDevicesWithLimit() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      List<String> exps = Arrays.asList("root.sg1.d0,false", "root.sg1.d1,false");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2")) {
        while (resultSet.next()) {
          Assert.assertEquals(
              exps.get(count), resultSet.getString(1) + "," + resultSet.getString(2));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testFirstOverlappedPageFiltered() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT)");

      // seq chunk : [1,10]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (1,'d0',1)");
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (10,'d0',10)");

      statement.execute("flush");

      // seq chunk : [13,20]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (13,'d0',13)");
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (20,'d0',20)");

      statement.execute("flush");

      // unseq chunk : [5,15]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (5,'d0',5)");
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (15,'d0',15)");

      statement.execute("flush");

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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT, s1 INT32 MEASUREMENT)");

      try {
        statement.execute("INSERT INTO table1(time, device, s0, s1) VALUES (1, 'd0', 1, 2.2)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("s1"));
      }

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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT)");

      // seq chunk : start-end [1000, 1000]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (1000, 'd0', 0)");

      statement.execute("flush");

      // unseq chunk : [1,10]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (1, 'd0', 1)");
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (10, 'd0', 10)");

      statement.execute("flush");

      // usneq chunk : [5,15]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (5, 'd0', 5)");
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (15, 'd0', 15)");

      statement.execute("flush");

      // unseq chunk : [15,15]
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (15, 'd0', 150)");

      statement.execute("flush");

      long count = 0;

      try (ResultSet resultSet = statement.executeQuery("select s0 from table1 where s0 < 100")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(4, count);
    }
  }

  @Ignore // TODO After delete data introduced
  @Test
  public void testUnseqUnsealedDeleteQuery() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT)");

      // seq data
      statement.execute("INSERT INTO table1(time, device, s0) VALUES (1000, 'd0', 1)");
      statement.execute("flush");

      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format("INSERT INTO table1(time, device, s0) VALUES (%d, 'd0', %d)", i, i));
      }

      statement.execute("flush");

      // unseq data
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("INSERT INTO table1(time, device, s0) VALUES (%d, 'd0', %d)", i, i));
      }

      statement.execute("delete from table1 where time <= 15");

      long count = 0;

      try (ResultSet resultSet = statement.executeQuery("select * from table1")) {
        while (resultSet.next()) {
          count++;
        }
      }

      System.out.println(count);
    }
  }

  @Test
  public void testTimeseriesMetadataCache() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      StringBuilder createTableBuilder = new StringBuilder();
      createTableBuilder.append("CREATE TABLE table1(device STRING ID,");
      for (int i = 0; i < 1000; i++) {
        String columnName = "s" + i;
        createTableBuilder.append(columnName).append(" INT32 MEASUREMENT,");
      }
      createTableBuilder.deleteCharAt(createTableBuilder.lastIndexOf(",")).append(")");
      statement.execute(createTableBuilder.toString());
      for (int i = 1; i < 1000; i++) {
        statement.execute("INSERT INTO table1(time, device, s" + i + ") VALUES (1000, 'd0', 1)");
      }
      statement.execute("flush");
      statement.executeQuery("select s0 from table1");
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testUseSameStatement() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s0 INT32 MEASUREMENT, s1 INT32 MEASUREMENT)");
      statement.execute("insert into table1(time,device,s0,s1) values(1,'d0',1,1)");
      statement.execute("insert into table1(time,device,s0,s1) values(1000,'d1',1000,1000)");
      statement.execute("insert into table1(time,device,s0,s1) values(10,'d0',10,10)");

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
  public void testStorageGroupWithHyphenInName() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE group_with_hyphen");
    } catch (SQLException e) {
      fail();
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getString(1));
          Assert.assertEquals(builder.toString(), "group_with_hyphen");
        }
      }
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testEnableAlign() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s1 INT32 MEASUREMENT, s2 BOOLEAN MEASUREMENT)");
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
  public void testNewDataType() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING ID, s4 DATE MEASUREMENT, s5 TIMESTAMP MEASUREMENT, s6 BLOB MEASUREMENT, s7 STRING MEASUREMENT)");

      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "insert into table1(time, device, s4, s5, s6, s7) values(%d, 'd1', '%s', %d, %s, '%s')",
                i, LocalDate.of(2024, 5, i % 31 + 1), i, "X'cafebabe'", i));
      }

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
