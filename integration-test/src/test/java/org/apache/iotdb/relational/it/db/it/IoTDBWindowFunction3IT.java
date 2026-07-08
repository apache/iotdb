/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowFunction3IT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table demo (device string tag, value double field)",
        "insert into demo values (2021-01-01T09:05:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:07:00, 'd1', 5)",
        "insert into demo values (2021-01-01T09:09:00, 'd1', 3)",
        "insert into demo values (2021-01-01T09:10:00, 'd1', 1)",
        "insert into demo values (2021-01-01T09:08:00, 'd2', 2)",
        "insert into demo values (2021-01-01T09:15:00, 'd2', 4)",
        "create table stock_rank_cases (symbol string tag, amplitude float field, close_hfq float field, ma_3 float field, ma_5 float field, ma_13 float field, ma_64 float field, ma_120 float field)",
        "insert into stock_rank_cases values (2026-03-20T00:00:00.000+08:00, '600000', 0.023391813, 131.21382, 129.4694, 127.74202, 136.61397, 146.17342, 124.21007)",
        "insert into stock_rank_cases values (2026-03-27T00:00:00.000+08:00, '600000', 0.03696498, 127.89519, 130.02252, 128.07388, 134.97429, 146.16643, 124.58377)",
        "insert into stock_rank_cases values (2026-04-03T00:00:00.000+08:00, '600000', 0.045908183, 129.17159, 129.42686, 129.095, 133.40334, 146.23744, 124.978065)",
        "insert into stock_rank_cases values (2026-04-10T00:00:00.000+08:00, '600000', 0.03063241, 126.23586, 127.76755, 129.095, 130.89963, 146.24904, 125.36582)",
        "insert into stock_rank_cases values (2026-04-17T00:00:00.000+08:00, '600000', 0.03943377, 125.85294, 127.0868, 128.07388, 129.25014, 146.22755, 125.76035)",
        "insert into stock_rank_cases values (2026-04-24T00:00:00.000+08:00, '600000', 0.048681542, 120.61971, 124.236176, 125.955055, 127.688995, 146.10107, 126.11027)",
        "insert into stock_rank_cases values (2026-05-01T00:00:00.000+08:00, '600000', 0.026455026, 118.32219, 121.59828, 124.04046, 126.47151, 145.88837, 126.43706)",
        "insert into stock_rank_cases values (2026-05-08T00:00:00.000+08:00, '600000', 0.025889968, 115.769394, 118.2371, 121.36002, 125.51912, 145.69579, 126.736595)",
        "insert into stock_rank_cases values (2026-05-15T00:00:00.000+08:00, '600000', 0.033076074, 115.769394, 116.62032, 119.26673, 124.48818, 145.48964, 127.05406)",
        "create table stock_rank_date_cases (symbol string tag, amplitude float field, close_hfq float field, ma_5 float field, ma_64 float field, ma_120 float field)",
        "insert into stock_rank_date_cases values (2026-04-17, '600000', 0.03943377, 125.85294, 128.07388, 146.22755, 125.76035)",
        "insert into stock_rank_date_cases values (2026-04-24, '600000', 0.048681542, 120.61971, 125.955055, 146.10107, 126.11027)",
        "insert into stock_rank_date_cases values (2026-05-01, '600000', 0.026455026, 118.32219, 124.04046, 145.88837, 126.43706)",
        "insert into stock_rank_date_cases values (2026-05-08, '600000', 0.025889968, 115.769394, 121.36002, 145.69579, 126.736595)",
        "insert into stock_rank_date_cases values (2026-05-15, '600000', 0.033076074, 115.769394, 119.26673, 145.48964, 127.05406)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSortBufferSize(128 * 1024)
        .setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMergeWindowFunctions() {
    String[] expectedHeader = new String[] {"time", "device", "value", "a", "b"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,3.0,4.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,5.0,6.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3.0,4.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,2.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,4.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,4.0,6.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, a + min(value) OVER (PARTITION BY device ORDER BY value) as b FROM (SELECT *, max(value) OVER (PARTITION BY device ORDER BY value) as a FROM demo) ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSwapWindowFunctions() {
    String[] expectedHeader = new String[] {"time", "device", "value", "p1", "p2"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1.0,6.0,",
          "2021-01-01T09:07:00.000Z,d1,5.0,1.0,5.0,",
          "2021-01-01T09:09:00.000Z,d1,3.0,1.0,6.0,",
          "2021-01-01T09:10:00.000Z,d1,1.0,1.0,1.0,",
          "2021-01-01T09:08:00.000Z,d2,2.0,2.0,2.0,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2.0,4.0,",
        };
    tableResultSetEqualTest(
        "SELECT *, min(value) OVER (PARTITION BY device) as p1, sum(value) OVER (PARTITION BY device, value) as p2 FROM demo ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDifferentOrdering() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rank_time", "rank_value"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,2,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,4,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,2,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,1,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, rank() OVER (PARTITION BY device ORDER BY \"time\") AS rank_time, rank() OVER (PARTITION BY device ORDER BY value) AS rank_value FROM demo ORDER BY device, \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDifferentOrderingWithoutPartition() {
    String[] expectedHeader =
        new String[] {"time", "device", "value", "rank_time_desc", "rank_value", "rank_time_asc"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,6,3,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,5,6,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,4,2,3,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,3,4,",
          "2021-01-01T09:10:00.000Z,d1,1.0,2,1,5,",
          "2021-01-01T09:15:00.000Z,d2,4.0,1,5,6,",
        };
    tableResultSetEqualTest(
        "SELECT *, rank() OVER (ORDER BY \"time\" DESC) AS rank_time_desc, rank() OVER (ORDER BY value) AS rank_value, rank() OVER (ORDER BY \"time\") AS rank_time_asc FROM demo ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDifferentFieldOrderingWithoutPartition() {
    String[] expectedHeader =
        new String[] {"time", "ma_3", "ma_13", "ma_120", "rank_m3", "rank_ma13", "rank_ma120"};
    String[] retArray =
        new String[] {
          "2026-03-19T16:00:00.000Z,129.4694,136.61397,124.21007,2,3,1,",
          "2026-03-26T16:00:00.000Z,130.02252,134.97429,124.58377,3,2,2,",
          "2026-04-02T16:00:00.000Z,129.42686,133.40334,124.978065,1,1,3,",
        };
    tableResultSetEqualTest(
        "SELECT \"time\", ma_3, ma_13, ma_120, rank() OVER (ORDER BY ma_3) AS rank_m3, rank() OVER (ORDER BY ma_13) AS rank_ma13, rank() OVER (ORDER BY ma_120) AS rank_ma120 FROM stock_rank_cases WHERE \"time\" >= 2026-03-20T00:00:00.000+08:00 AND \"time\" <= 2026-04-03T00:00:00.000+08:00 AND symbol = '600000' ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDifferentStockOrderingWithoutTimeRank() {
    String[] expectedHeader =
        new String[] {"time", "amplitude", "close_hfq", "rank_amplitude", "rank_close_hfq"};
    String[] retArray =
        new String[] {
          "2026-04-16T16:00:00.000Z,0.03943377,125.85294,2,1,",
          "2026-04-23T16:00:00.000Z,0.048681542,120.61971,1,2,",
          "2026-04-30T16:00:00.000Z,0.026455026,118.32219,4,3,",
          "2026-05-07T16:00:00.000Z,0.025889968,115.769394,5,4,",
          "2026-05-14T16:00:00.000Z,0.033076074,115.769394,3,4,",
        };
    tableResultSetEqualTest(
        "SELECT \"time\", amplitude, close_hfq, rank() OVER (ORDER BY amplitude DESC) AS rank_amplitude, rank() OVER (ORDER BY close_hfq DESC) AS rank_close_hfq FROM stock_rank_cases WHERE \"time\" >= 2026-04-11T00:00:00.000+08:00 AND symbol = '600000' ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDifferentStockOrderingAfterTimeRank() {
    String[] expectedHeader =
        new String[] {
          "time",
          "amplitude",
          "close_hfq",
          "rank_time_desc",
          "rank_amplitude",
          "rank_time_asc",
          "rank_close_hfq"
        };
    String[] retArray =
        new String[] {
          "2026-04-16T16:00:00.000Z,0.03943377,125.85294,5,4,1,5,",
          "2026-04-23T16:00:00.000Z,0.048681542,120.61971,4,5,2,4,",
          "2026-04-30T16:00:00.000Z,0.026455026,118.32219,3,2,3,3,",
          "2026-05-07T16:00:00.000Z,0.025889968,115.769394,2,1,4,1,",
          "2026-05-14T16:00:00.000Z,0.033076074,115.769394,1,3,5,1,",
        };
    tableResultSetEqualTest(
        "SELECT \"time\", amplitude, close_hfq, rank() OVER (ORDER BY \"time\" DESC) AS rank_time_desc, rank() OVER (ORDER BY amplitude) AS rank_amplitude, rank() OVER (ORDER BY \"time\") AS rank_time_asc, rank() OVER (ORDER BY close_hfq) AS rank_close_hfq FROM stock_rank_cases WHERE \"time\" >= 2026-04-11T00:00:00.000+08:00 AND symbol = '600000' ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithOriginalStockOrderingAfterTimeRank() {
    String[] expectedHeader =
        new String[] {
          "time",
          "amplitude",
          "close_hfq",
          "ma_5",
          "ma_64",
          "ma_120",
          "rank_time",
          "rank_amplitude",
          "rank_close_hfq",
          "rank_ma5",
          "rank_ma64",
          "rank_ma120"
        };
    String[] retArray =
        new String[] {
          "2026-04-16T16:00:00.000Z,0.03943377,125.85294,128.07388,146.22755,125.76035,1,2,1,5,1,1,",
          "2026-04-23T16:00:00.000Z,0.048681542,120.61971,125.955055,146.10107,126.11027,2,1,2,4,2,2,",
          "2026-04-30T16:00:00.000Z,0.026455026,118.32219,124.04046,145.88837,126.43706,3,4,3,3,3,3,",
          "2026-05-07T16:00:00.000Z,0.025889968,115.769394,121.36002,145.69579,126.736595,4,5,4,2,4,4,",
          "2026-05-14T16:00:00.000Z,0.033076074,115.769394,119.26673,145.48964,127.05406,5,3,4,1,5,5,",
        };
    tableResultSetEqualTest(
        "SELECT \"time\", amplitude, close_hfq, ma_5, ma_64, ma_120, rank() OVER (ORDER BY \"time\") AS rank_time, rank() OVER (ORDER BY amplitude DESC) AS rank_amplitude, rank() OVER (ORDER BY close_hfq DESC) AS rank_close_hfq, rank() OVER (ORDER BY ma_5) AS rank_ma5, rank() OVER (ORDER BY ma_64 DESC) AS rank_ma64, rank() OVER (ORDER BY ma_120) AS rank_ma120 FROM stock_rank_cases WHERE \"time\" >= 2026-04-11T00:00:00.000+08:00 AND symbol = '600000' ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSameWindowFunctionWithDateOnlyTimeAfterTimeRank() {
    String[] expectedHeader =
        new String[] {
          "time",
          "amplitude",
          "close_hfq",
          "ma_5",
          "ma_64",
          "ma_120",
          "rank_time",
          "rank_amplitude",
          "rank_close_hfq",
          "rank_ma5",
          "rank_ma64",
          "rank_ma120"
        };
    String[] retArray =
        new String[] {
          "2026-04-17T00:00:00.000Z,0.03943377,125.85294,128.07388,146.22755,125.76035,1,2,1,5,1,1,",
          "2026-04-24T00:00:00.000Z,0.048681542,120.61971,125.955055,146.10107,126.11027,2,1,2,4,2,2,",
          "2026-05-01T00:00:00.000Z,0.026455026,118.32219,124.04046,145.88837,126.43706,3,4,3,3,3,3,",
          "2026-05-08T00:00:00.000Z,0.025889968,115.769394,121.36002,145.69579,126.736595,4,5,4,2,4,4,",
          "2026-05-15T00:00:00.000Z,0.033076074,115.769394,119.26673,145.48964,127.05406,5,3,4,1,5,5,",
        };
    tableResultSetEqualTest(
        "SELECT \"time\", amplitude, close_hfq, ma_5, ma_64, ma_120, rank() OVER (ORDER BY \"time\") AS rank_time, rank() OVER (ORDER BY amplitude DESC) AS rank_amplitude, rank() OVER (ORDER BY close_hfq DESC) AS rank_close_hfq, rank() OVER (ORDER BY ma_5) AS rank_ma5, rank() OVER (ORDER BY ma_64 DESC) AS rank_ma64, rank() OVER (ORDER BY ma_120) AS rank_ma120 FROM stock_rank_date_cases WHERE \"time\" >= 2026-04-11 AND symbol = '600000' ORDER BY \"time\"",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPushDownFilterIntoWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:10:00.000Z,d1,1.0,1,",
          "2021-01-01T09:05:00.000Z,d1,3.0,2,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY device ORDER BY value) as rn FROM demo) WHERE rn <= 2 ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPushDownLimitIntoWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,2,", "2021-01-01T09:07:00.000Z,d1,5.0,4,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY device ORDER BY value) as rn FROM demo) ORDER BY device, time LIMIT 2 ",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReplaceWindowWithRowNumber() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,d1,3.0,1,",
          "2021-01-01T09:07:00.000Z,d1,5.0,2,",
          "2021-01-01T09:09:00.000Z,d1,3.0,3,",
          "2021-01-01T09:10:00.000Z,d1,1.0,4,",
          "2021-01-01T09:08:00.000Z,d2,2.0,1,",
          "2021-01-01T09:15:00.000Z,d2,4.0,2,",
        };
    tableResultSetEqualTest(
        "SELECT *, row_number() OVER (PARTITION BY device) AS rn FROM demo ORDER BY device, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testRemoveRedundantWindow() {
    String[] expectedHeader = new String[] {"time", "device", "value", "rn"};
    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "SELECT *, row_number() OVER (PARTITION BY device) AS rn FROM demo WHERE 1 = 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
