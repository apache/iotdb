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

package org.apache.iotdb.relational.it.query.object;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBObjectQuery2IT {

  private static final String DATABASE_NAME = "test";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDataReplicationFactor(1);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE table1(device STRING TAG, s4 DATE FIELD, s5 TIMESTAMP FIELD, s6 BLOB FIELD, s7 STRING FIELD, s8 OBJECT FIELD, s9 OBJECT FIELD)");
      for (int i = 1; i <= 10; i++) {
        for (int j = 0; j < 10; j++) {
          statement.execute(
              String.format(
                  "insert into table1(time, device, s4, s5, s6, s7, s8) "
                      + "values(%d, '%s', '%s', %d, %s, '%s', %s)",
                  j,
                  "d" + i,
                  LocalDate.of(2024, 5, i % 31 + 1),
                  j,
                  "X'cafebabe'",
                  j,
                  "to_object(true, 0, X'cafebabe')"));
        }
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testObjectLength() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select length(s8) from table1 limit 1");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        long length = iterator.getLong(1);
        Assert.assertEquals(4, length);
      }
    }
  }

  @Test
  public void testReadObject() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("select read_object(s8) from table1 where device = 'd2'");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      byte[] expected = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
      while (iterator.next()) {
        Binary blob = iterator.getBlob(1);
        Assert.assertArrayEquals(expected, blob.getValues());
      }

      sessionDataSet =
          session.executeQueryStatement(
              "select read_object(s8, 1) from table1 where device = 'd3'");
      iterator = sessionDataSet.iterator();
      expected = new byte[] {(byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
      while (iterator.next()) {
        Binary blob = iterator.getBlob(1);
        Assert.assertArrayEquals(expected, blob.getValues());
      }
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select read_object(s8, 1, 2) from table1 where device = 'd1'");
      iterator = sessionDataSet.iterator();
      expected = new byte[] {(byte) 0xFE, (byte) 0xBA};
      while (iterator.next()) {
        Binary blob = iterator.getBlob(1);
        Assert.assertArrayEquals(expected, blob.getValues());
      }
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select read_object(s8, 1, 1000) from table1 where device = 'd1'");
      iterator = sessionDataSet.iterator();
      expected = new byte[] {(byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
      while (iterator.next()) {
        Binary blob = iterator.getBlob(1);
        Assert.assertArrayEquals(expected, blob.getValues());
      }
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select count(*) from table1 where device = 'd1' and s6 = read_object(s8)");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        long count = iterator.getLong(1);
        Assert.assertEquals(10, count);
      }
      sessionDataSet.close();

      // read_object are not pushed down. Read remote files
      sessionDataSet =
          session.executeQueryStatement(
              "select read_object(t1_s8) from (select t1.s8 as t1_s8, t2.s8 as t2_s8 from table1 as t1 inner join table1 as t2 using(time))");
      iterator = sessionDataSet.iterator();
      expected = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
      while (iterator.next()) {
        Binary blob = iterator.getBlob(1);
        Assert.assertArrayEquals(expected, blob.getValues());
      }
      sessionDataSet.close();
    }
  }

  @Test
  public void testFunctionAndClauses()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select length(s8) from table1 where device = 'd2' and s8 is not null limit 1");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals(4, iterator.getLong(1));
      }
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select count(s8), first(s8), last(s8), first_by(s8, time), last_by(s8, time) from table1 where device = 'd1' and cast(s8 as string) = '(Object) 4 B' and try_cast(s8 as string) = '(Object) 4 B'");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals(10, iterator.getLong(1));
        Assert.assertEquals("(Object) 4 B", iterator.getString(2));
        Assert.assertEquals("(Object) 4 B", iterator.getString(3));
        Assert.assertEquals("(Object) 4 B", iterator.getString(4));
        Assert.assertEquals("(Object) 4 B", iterator.getString(5));
      }
      sessionDataSet.close();

      sessionDataSet = session.executeQueryStatement("select coalesce(s9, s8) from table1");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals("(Object) 4 B", iterator.getString(1));
      }
      sessionDataSet.close();

      // MATCH_RECOGNIZE
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  "select m.cnt from table1 match_recognize (order by s8 measures RPR_LAST(time) as cnt one row per match pattern (B+) define B as B.s6 = prev(B.s6)) as m"));
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  "select m.cnt from table1 match_recognize (partition by s8 measures RPR_LAST(time) as cnt one row per match pattern (B+) define B as B.s6 = prev(B.s6)) as m"));

      sessionDataSet =
          session.executeQueryStatement(
              "select m.value from table1 match_recognize(partition by s6 measures prev(s8) as value one row per match pattern (B+) define B as B.s6=prev(B.s6)) as m");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals("(Object) 4 B", iterator.getString(1));
      }
      sessionDataSet.close();

      // WHERE
      session.executeQueryStatement(
          "select time, s8 from table1 where device = 'd10' and s8 is not null");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals("(Object) 4 B", iterator.getString(2));
      }
      sessionDataSet.close();

      // GROUP BY
      Assert.assertThrows(
          StatementExecutionException.class,
          () -> session.executeQueryStatement("select count(*) from table1 group by s8"));

      // ORDER BY
      Assert.assertThrows(
          StatementExecutionException.class,
          () -> session.executeQueryStatement("select count(*) from table1 order by s8"));

      // FILL
      sessionDataSet =
          session.executeQueryStatement(
              "select time, s8 from table1 where device = 'd10' fill method linear");
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select time, s8 from table1 where device = 'd10' fill method previous");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        Assert.assertEquals("(Object) 4 B", iterator.getString(2));
      }
      sessionDataSet.close();

      // HAVING
      sessionDataSet =
          session.executeQueryStatement(
              "select device, count(s8) from table1 group by device having count(s8) > 0");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        long count = iterator.getLong(2);
        Assert.assertEquals(10, count);
      }
      sessionDataSet.close();

      // WINDOW
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeQueryStatement(
                  "select *, nth_value(s8,2) over(partition by s8) from table1"));
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeQueryStatement(
                  "select *, nth_value(s8,2) over(order by s8) from table1"));
      sessionDataSet =
          session.executeQueryStatement(
              "select *, nth_value(s8,2) over(partition by device) from table1");
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select *, lead(s8) over(partition by device order by time) from table1");
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select *, first_value(s8) over(partition by device) from table1");
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select *, last_value(s8) over(partition by device) from table1");
      sessionDataSet.close();

      sessionDataSet =
          session.executeQueryStatement(
              "select *, lag(s8) over(partition by device order by time) from table1");
      sessionDataSet.close();

      // Table-value function
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeQueryStatement(
                  "select * from session(data => table1 partition by s8, timecol => 'time', gap => 1ms)"));
      Assert.assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeQueryStatement(
                  "select * from session(data => table1 order by s8, timecol => 'time', gap => 1ms)"));
      sessionDataSet =
          session.executeQueryStatement(
              "select * from hop(data => table1, timecol => 'time', slide => 1ms, size => 2ms)");
      iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String str = iterator.getString("s8");
        Assert.assertEquals("(Object) 4 B", str);
      }
      sessionDataSet.close();
    }
  }
}
