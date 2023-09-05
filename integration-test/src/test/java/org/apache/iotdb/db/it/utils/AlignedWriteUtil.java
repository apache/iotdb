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
package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * This class generates data for test cases in aligned time series scenarios.
 *
 * <p>You can comprehensively view the generated data in the following online doc:
 *
 * <p>https://docs.google.com/spreadsheets/d/1kfrSR1_paSd9B1Z0jnPBD3WQIMDslDuNm4R0mpWx9Ms/edit?usp=sharing
 */
public class AlignedWriteUtil {

  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE root.sg1",
        "create aligned timeseries root.sg1.d1(s1 FLOAT encoding=RLE, s2 INT32 encoding=Gorilla compression=SNAPPY, s3 INT64, s4 BOOLEAN, s5 TEXT)",
        "create timeseries root.sg1.d2.s1 WITH DATATYPE=FLOAT, encoding=RLE",
        "create timeseries root.sg1.d2.s2 WITH DATATYPE=INT32, encoding=Gorilla",
        "create timeseries root.sg1.d2.s3 WITH DATATYPE=INT64",
        "create timeseries root.sg1.d2.s4 WITH DATATYPE=BOOLEAN",
        "create timeseries root.sg1.d2.s5 WITH DATATYPE=TEXT",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(1, 1.0, 1, 1, TRUE, 'aligned_test1')",
        "insert into root.sg1.d1(time, s1, s2, s3, s5) aligned values(2, 2.0, 2, 2, 'aligned_test2')",
        "insert into root.sg1.d1(time, s1, s3, s4, s5) aligned values(3, 3.0, 3, FALSE, 'aligned_test3')",
        "insert into root.sg1.d1(time, s1, s2, s4, s5) aligned values(4, 4.0, 4, TRUE, 'aligned_test4')",
        "insert into root.sg1.d1(time, s1, s2, s4, s5) aligned values(5, 5.0, 5, TRUE, 'aligned_test5')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4) aligned values(6, 6.0, 6, 6, TRUE)",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(7, 7.0, 7, 7, FALSE, 'aligned_test7')",
        "insert into root.sg1.d1(time, s1, s2, s3, s5) aligned values(8, 8.0, 8, 8, 'aligned_test8')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(9, 9.0, 9, 9, FALSE, 'aligned_test9')",
        "insert into root.sg1.d1(time, s2, s3, s4, s5) aligned values(10, 10, 10, TRUE, 'aligned_test10')",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5) values(1, 1.0, 1, 1, TRUE, 'non_aligned_test1')",
        "insert into root.sg1.d2(time, s1, s2, s3, s5) values(2, 2.0, 2, 2, 'non_aligned_test2')",
        "insert into root.sg1.d2(time, s1, s3, s4, s5) values(3, 3.0, 3, FALSE, 'non_aligned_test3')",
        "insert into root.sg1.d2(time, s1, s2, s4, s5) values(4, 4.0, 4, TRUE, 'non_aligned_test4')",
        "insert into root.sg1.d2(time, s1, s2, s4, s5) values(5, 5.0, 5, TRUE, 'non_aligned_test5')",
        "insert into root.sg1.d2(time, s1, s2, s3, s4) values(6, 6.0, 6, 6, TRUE)",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5) values(7, 7.0, 7, 7, FALSE, 'non_aligned_test7')",
        "insert into root.sg1.d2(time, s1, s2, s3, s5) values(8, 8.0, 8, 8, 'non_aligned_test8')",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5) values(9, 9.0, 9, 9, FALSE, 'non_aligned_test9')",
        "insert into root.sg1.d2(time, s2, s3, s4, s5) values(10, 10, 10, TRUE, 'non_aligned_test10')",
        "flush",
        "insert into root.sg1.d1(time, s1, s3, s4, s5) aligned values(3, 30000.0, 30000, TRUE, 'aligned_unseq_test3')",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(11, 11.0, 11, 11)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(12, 12.0, 12, 12)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(13, 13.0, 13, 13)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(14, 14.0, 14, 14)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(15, 15.0, 15, 15)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(16, 16.0, 16, 16)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(17, 17.0, 17, 17)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(18, 18.0, 18, 18)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(19, 19.0, 19, 19)",
        "insert into root.sg1.d1(time, s1, s2, s3) aligned values(20, 20.0, 20, 20)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(11, 11.0, 11, 11)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(12, 12.0, 12, 12)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(13, 13.0, 13, 13)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(14, 14.0, 14, 14)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(15, 15.0, 15, 15)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(16, 16.0, 16, 16)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(17, 17.0, 17, 17)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(18, 18.0, 18, 18)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(19, 19.0, 19, 19)",
        "insert into root.sg1.d2(time, s1, s2, s3) values(20, 20.0, 20, 20)",
        "flush",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5) aligned values(13, 130000.0, 130000, 130000, TRUE, 'aligned_unseq_test13')",
        "insert into root.sg1.d1(time, s3, s4) aligned values(21, 21, TRUE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(22, 22, TRUE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(23, 23, TRUE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(24, 24, TRUE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(25, 25, TRUE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(26, 26, FALSE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(27, 27, FALSE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(28, 28, FALSE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(29, 29, FALSE)",
        "insert into root.sg1.d1(time, s3, s4) aligned values(30, 30, FALSE)",
        "insert into root.sg1.d2(time, s3, s4) values(21, 21, TRUE)",
        "insert into root.sg1.d2(time, s3, s4) values(22, 22, TRUE)",
        "insert into root.sg1.d2(time, s3, s4) values(23, 23, TRUE)",
        "insert into root.sg1.d2(time, s3, s4) values(24, 24, TRUE)",
        "insert into root.sg1.d2(time, s3, s4) values(25, 25, TRUE)",
        "insert into root.sg1.d2(time, s3, s4) values(26, 26, FALSE)",
        "insert into root.sg1.d2(time, s3, s4) values(27, 27, FALSE)",
        "insert into root.sg1.d2(time, s3, s4) values(28, 28, FALSE)",
        "insert into root.sg1.d2(time, s3, s4) values(29, 29, FALSE)",
        "insert into root.sg1.d2(time, s3, s4) values(30, 30, FALSE)",
        "flush",
        "insert into root.sg1.d1(time, s1, s3, s4) aligned values(23, 230000.0, 230000, FALSE)",
        "insert into root.sg1.d1(time, s2, s5) aligned values(31, 31, 'aligned_test31')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(32, 32, 'aligned_test32')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(33, 33, 'aligned_test33')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(34, 34, 'aligned_test34')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(35, 35, 'aligned_test35')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(36, 36, 'aligned_test36')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(37, 37, 'aligned_test37')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(38, 38, 'aligned_test38')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(39, 39, 'aligned_test39')",
        "insert into root.sg1.d1(time, s2, s5) aligned values(40, 40, 'aligned_test40')",
        "insert into root.sg1.d2(time, s2, s5) values(31, 31, 'non_aligned_test31')",
        "insert into root.sg1.d2(time, s2, s5) values(32, 32, 'non_aligned_test32')",
        "insert into root.sg1.d2(time, s2, s5) values(33, 33, 'non_aligned_test33')",
        "insert into root.sg1.d2(time, s2, s5) values(34, 34, 'non_aligned_test34')",
        "insert into root.sg1.d2(time, s2, s5) values(35, 35, 'non_aligned_test35')",
        "insert into root.sg1.d2(time, s2, s5) values(36, 36, 'non_aligned_test36')",
        "insert into root.sg1.d2(time, s2, s5) values(37, 37, 'non_aligned_test37')",
        "insert into root.sg1.d2(time, s2, s5) values(38, 38, 'non_aligned_test38')",
        "insert into root.sg1.d2(time, s2, s5) values(39, 39, 'non_aligned_test39')",
        "insert into root.sg1.d2(time, s2, s5) values(40, 40, 'non_aligned_test40')",
      };

  public static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // create aligned and non-aligned time series
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
}
