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
package org.apache.iotdb.relational.it.query.old.aligned;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class generates data for test cases in aligned time series scenarios.
 *
 * <p>You can comprehensively view the generated data in the following online doc:
 *
 * <p>https://docs.google.com/spreadsheets/d/1kfrSR1_paSd9B1Z0jnPBD3WQIMDslDuNm4R0mpWx9Ms/edit?usp=sharing
 */
public class TableUtils {

  public static final String USE_DB = "use db";

  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE db",
        "USE db",
        "CREATE TABLE table0 (device string id, s1 FLOAT measurement, s2 INT32 measurement, s3 INT64 measurement, s4 BOOLEAN measurement, s5 TEXT measurement)",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d1', 1, 1.0, 1, 1, TRUE, 'aligned_test1')",
        "insert into table0(device, time, s1, s2, s3, s5) values('d1', 2, 2.0, 2, 2, 'aligned_test2')",
        "insert into table0(device, time, s1, s3, s4, s5) values('d1', 3, 3.0, 3, FALSE, 'aligned_test3')",
        "insert into table0(device, time, s1, s2, s4, s5) values('d1', 4, 4.0, 4, TRUE, 'aligned_test4')",
        "insert into table0(device, time, s1, s2, s4, s5) values('d1', 5, 5.0, 5, TRUE, 'aligned_test5')",
        "insert into table0(device, time, s1, s2, s3, s4) values('d1', 6, 6.0, 6, 6, TRUE)",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d1',7, 7.0, 7, 7, FALSE, 'aligned_test7')",
        "insert into table0(device, time, s1, s2, s3, s5) values('d1',8, 8.0, 8, 8, 'aligned_test8')",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d1',9, 9.0, 9, 9, FALSE, 'aligned_test9')",
        "insert into table0(device, time, s2, s3, s4, s5) values('d1',10, 10, 10, TRUE, 'aligned_test10')",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d2',1, 1.0, 1, 1, TRUE, 'non_aligned_test1')",
        "insert into table0(device, time, s1, s2, s3, s5) values('d2',2, 2.0, 2, 2, 'non_aligned_test2')",
        "insert into table0(device, time, s1, s3, s4, s5) values('d2',3, 3.0, 3, FALSE, 'non_aligned_test3')",
        "insert into table0(device, time, s1, s2, s4, s5) values('d2',4, 4.0, 4, TRUE, 'non_aligned_test4')",
        "insert into table0(device, time, s1, s2, s4, s5) values('d2',5, 5.0, 5, TRUE, 'non_aligned_test5')",
        "insert into table0(device, time, s1, s2, s3, s4) values('d2',6, 6.0, 6, 6, TRUE)",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d2',7, 7.0, 7, 7, FALSE, 'non_aligned_test7')",
        "insert into table0(device, time, s1, s2, s3, s5) values('d2',8, 8.0, 8, 8, 'non_aligned_test8')",
        "insert into table0(device, time, s1, s2, s3, s4, s5) values('d2',9, 9.0, 9, 9, FALSE, 'non_aligned_test9')",
        "insert into table0(device, time, s2, s3, s4, s5) values('d2',10, 10, 10, TRUE, 'non_aligned_test10')",
        "flush",
        "insert into table0(device,time, s1, s3, s4, s5) values('d1',3, 30000.0, 30000, TRUE, 'aligned_unseq_test3')",
        "insert into table0(device,time, s1, s2, s3) values('d1',11, 11.0, 11, 11)",
        "insert into table0(device,time, s1, s2, s3) values('d1',12, 12.0, 12, 12)",
        "insert into table0(device,time, s1, s2, s3) values('d1',13, 13.0, 13, 13)",
        "insert into table0(device,time, s1, s2, s3) values('d1',14, 14.0, 14, 14)",
        "insert into table0(device,time, s1, s2, s3) values('d1',15, 15.0, 15, 15)",
        "insert into table0(device,time, s1, s2, s3) values('d1',16, 16.0, 16, 16)",
        "insert into table0(device,time, s1, s2, s3) values('d1',17, 17.0, 17, 17)",
        "insert into table0(device,time, s1, s2, s3) values('d1',18, 18.0, 18, 18)",
        "insert into table0(device,time, s1, s2, s3) values('d1',19, 19.0, 19, 19)",
        "insert into table0(device,time, s1, s2, s3) values('d1',20, 20.0, 20, 20)",
        "insert into table0(device,time, s1, s2, s3) values('d2',11, 11.0, 11, 11)",
        "insert into table0(device,time, s1, s2, s3) values('d2',12, 12.0, 12, 12)",
        "insert into table0(device,time, s1, s2, s3) values('d2',13, 13.0, 13, 13)",
        "insert into table0(device,time, s1, s2, s3) values('d2',14, 14.0, 14, 14)",
        "insert into table0(device,time, s1, s2, s3) values('d2',15, 15.0, 15, 15)",
        "insert into table0(device,time, s1, s2, s3) values('d2',16, 16.0, 16, 16)",
        "insert into table0(device,time, s1, s2, s3) values('d2',17, 17.0, 17, 17)",
        "insert into table0(device,time, s1, s2, s3) values('d2',18, 18.0, 18, 18)",
        "insert into table0(device,time, s1, s2, s3) values('d2',19, 19.0, 19, 19)",
        "insert into table0(device,time, s1, s2, s3) values('d2',20, 20.0, 20, 20)",
        "flush",
        "insert into table0(device,time, s1, s2, s3, s4, s5) values('d1',13, 130000.0, 130000, 130000, TRUE, 'aligned_unseq_test13')",
        "insert into table0(device,time, s3, s4) values('d1',21, 21, TRUE)",
        "insert into table0(device,time, s3, s4) values('d1',22, 22, TRUE)",
        "insert into table0(device,time, s3, s4) values('d1',23, 23, TRUE)",
        "insert into table0(device,time, s3, s4) values('d1',24, 24, TRUE)",
        "insert into table0(device,time, s3, s4) values('d1',25, 25, TRUE)",
        "insert into table0(device,time, s3, s4) values('d1',26, 26, FALSE)",
        "insert into table0(device,time, s3, s4) values('d1',27, 27, FALSE)",
        "insert into table0(device,time, s3, s4) values('d1',28, 28, FALSE)",
        "insert into table0(device,time, s3, s4) values('d1',29, 29, FALSE)",
        "insert into table0(device,time, s3, s4) values('d1',30, 30, FALSE)",
        "insert into table0(device,time, s3, s4) values('d2',21, 21, TRUE)",
        "insert into table0(device,time, s3, s4) values('d2',22, 22, TRUE)",
        "insert into table0(device,time, s3, s4) values('d2',23, 23, TRUE)",
        "insert into table0(device,time, s3, s4) values('d2',24, 24, TRUE)",
        "insert into table0(device,time, s3, s4) values('d2',25, 25, TRUE)",
        "insert into table0(device,time, s3, s4) values('d2',26, 26, FALSE)",
        "insert into table0(device,time, s3, s4) values('d2',27, 27, FALSE)",
        "insert into table0(device,time, s3, s4) values('d2',28, 28, FALSE)",
        "insert into table0(device,time, s3, s4) values('d2',29, 29, FALSE)",
        "insert into table0(device,time, s3, s4) values('d2',30, 30, FALSE)",
        "flush",
        "insert into table0(device,time, s1, s3, s4) values('d1',23, 230000.0, 230000, FALSE)",
        "insert into table0(device,time, s2, s5) values('d1',31, 31, 'aligned_test31')",
        "insert into table0(device,time, s2, s5) values('d1',32, 32, 'aligned_test32')",
        "insert into table0(device,time, s2, s5) values('d1',33, 33, 'aligned_test33')",
        "insert into table0(device,time, s2, s5) values('d1',34, 34, 'aligned_test34')",
        "insert into table0(device,time, s2, s5) values('d1',35, 35, 'aligned_test35')",
        "insert into table0(device,time, s2, s5) values('d1',36, 36, 'aligned_test36')",
        "insert into table0(device,time, s2, s5) values('d1',37, 37, 'aligned_test37')",
        "insert into table0(device,time, s2, s5) values('d1',38, 38, 'aligned_test38')",
        "insert into table0(device,time, s2, s5) values('d1',39, 39, 'aligned_test39')",
        "insert into table0(device,time, s2, s5) values('d1',40, 40, 'aligned_test40')",
        "insert into table0(device,time, s2, s5) values('d2',31, 31, 'non_aligned_test31')",
        "insert into table0(device,time, s2, s5) values('d2',32, 32, 'non_aligned_test32')",
        "insert into table0(device,time, s2, s5) values('d2',33, 33, 'non_aligned_test33')",
        "insert into table0(device,time, s2, s5) values('d2',34, 34, 'non_aligned_test34')",
        "insert into table0(device,time, s2, s5) values('d2',35, 35, 'non_aligned_test35')",
        "insert into table0(device,time, s2, s5) values('d2',36, 36, 'non_aligned_test36')",
        "insert into table0(device,time, s2, s5) values('d2',37, 37, 'non_aligned_test37')",
        "insert into table0(device,time, s2, s5) values('d2',38, 38, 'non_aligned_test38')",
        "insert into table0(device,time, s2, s5) values('d2',39, 39, 'non_aligned_test39')",
        "insert into table0(device,time, s2, s5) values('d2',40, 40, 'non_aligned_test40')",
      };

  public static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void insertDataWithSession() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      for (String sql : sqls) {
        session.executeNonQueryStatement(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void tableResultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray, String database) {
    tableResultSetEqualTest(
        sql,
        expectedHeader,
        expectedRetArray,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        database);
  }

  public static void tableResultSetEqualTest(
      String sql,
      String[] expectedHeader,
      String[] expectedRetArray,
      String userName,
      String password,
      String database) {
    try (Connection connection =
        EnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();

            for (int i = 1; i <= expectedHeader.length; i++) {
              if (i == 1) {
                builder.append(resultSet.getLong(i)).append(",");
              } else {
                builder.append(resultSet.getString(i)).append(",");
              }
            }
            assertEquals(expectedRetArray[cnt], builder.toString());
            cnt++;
          }
          assertEquals(expectedRetArray.length, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
