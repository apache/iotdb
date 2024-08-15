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

package org.apache.iotdb.db.it.selectinto;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSelectIntoIT {

  public static final List<String> SELECT_INTO_SQL_LIST =
      new ArrayList<>(
          Arrays.asList(
              "CREATE DATABASE root.sg",
              "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
              "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
              "CREATE TIMESERIES root.sg.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE",
              "CREATE TIMESERIES root.sg.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
              "INSERT INTO root.sg.d1(time, s1) VALUES (1, 1)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (2, 2, 2)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (3, 3, 3)",
              "INSERT INTO root.sg.d1(time, s2) VALUES (4, 4)",
              "INSERT INTO root.sg.d1(time, s1) VALUES (5, 5)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (6, 6, 6)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (7, 7, 7)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (8, 8, 8)",
              "INSERT INTO root.sg.d1(time, s2) VALUES (9, 9)",
              "INSERT INTO root.sg.d1(time, s1) VALUES (10, 10)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (11, 11, 11)",
              "INSERT INTO root.sg.d1(time, s1, s2) VALUES (12, 12, 12)",
              "INSERT INTO root.sg.d2(time, s1, s2) VALUES (1, 1, 1)",
              "INSERT INTO root.sg.d2(time, s2) VALUES (2, 2)",
              "INSERT INTO root.sg.d2(time, s1) VALUES (3, 3)",
              "INSERT INTO root.sg.d2(time, s1, s2) VALUES (4, 4, 4)",
              "INSERT INTO root.sg.d2(time, s2) VALUES (5, 5)",
              "INSERT INTO root.sg.d2(time, s2) VALUES (6, 6)",
              "INSERT INTO root.sg.d2(time, s1) VALUES (7, 7)",
              "INSERT INTO root.sg.d2(time, s1, s2) VALUES (8, 8, 8)",
              "INSERT INTO root.sg.d2(time, s1, s2) VALUES (10, 10, 10)",
              "INSERT INTO root.sg.d2(time, s2) VALUES (11, 11)",
              "INSERT INTO root.sg.d2(time, s1) VALUES (12, 12)",
              "flush",
              "CREATE DATABASE root.sg1",
              "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
              "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
              "create timeseries root.db.d1.s1 BOOLEAN encoding=PLAIN",
              "create timeseries root.db.d1.s2 FLOAT encoding=RLE",
              "create timeseries root.db.d1.s3 TEXT encoding=PLAIN",
              "create timeseries root.db.d1.s4 INT32 encoding=PLAIN",
              "create timeseries root.db.d1.s5 INT64 encoding=PLAIN",
              "create timeseries root.db.d1.s6 DOUBLE encoding=PLAIN",
              "create timeseries root.db.d1.s7 timestamp encoding=PLAIN",
              "create timeseries root.db.d1.s8 string encoding=PLAIN",
              "create timeseries root.db.d1.s9 blob encoding=PLAIN",
              "create timeseries root.db.d1.s10 date encoding=PLAIN",
              "CREATE ALIGNED TIMESERIES root.db.d2(s1 BOOLEAN encoding=PLAIN, s2 FLOAT encoding=RLE,s3 TEXT encoding=PLAIN,s4 INT32 encoding=PLAIN,s5 INT64 encoding=PLAIN,s6 DOUBLE encoding=PLAIN,s7 timestamp,s8 string,s9 blob,s10 date) ",
              "insert into root.db.d1(time,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1,true,1.1,'hello1',1,1,1.9,1997-01-01T08:00:00.001+08:00,'Hong Kong',X'486f6e67204b6f6e6720426c6f6221','1997-07-01')",
              "insert into root.db.d1(time,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(3,false,1.3,'hello3',3,3,2.1,1997-01-03T08:00:00.001+08:00,'Hong Kong-3',X'486f6e67204b6f6e6720426c6f6224','1997-07-03')",
              "insert into root.db.d1(time,s2) values(2,1.2)"));

  static {
    SELECT_INTO_SQL_LIST.add("CREATE DATABASE root.sg_type");
    for (int deviceId = 0; deviceId < 6; deviceId++) {
      for (TSDataType dataType : TSDataType.values()) {
        if (!dataType.equals(TSDataType.VECTOR) && !dataType.equals(TSDataType.UNKNOWN)) {
          SELECT_INTO_SQL_LIST.add(
              String.format(
                  "CREATE TIMESERIES root.sg_type.d_%d.s_%s %s",
                  deviceId, dataType.name().toLowerCase(), dataType));
        }
      }
    }
    for (int time = 0; time < 12; time++) {
      SELECT_INTO_SQL_LIST.add(
          String.format(
              Locale.ENGLISH,
              "INSERT INTO root.sg_type.d_0(time, s_int32, s_int64, s_float, s_double, s_boolean, s_text) "
                  + "VALUES (%d, %d, %d, %f, %f, %s, 'text%d')",
              time,
              time,
              time,
              (float) time,
              (double) time,
              time % 2 == 0,
              time));
    }
  }

  protected static final String selectIntoHeader = "SourceColumn,TargetTimeseries,Written,";
  protected static final String selectIntoAlignByDeviceHeader =
      "SourceDevice,SourceColumn,TargetTimeseries,Written,";

  protected static final String[] rawDataSet =
      new String[] {
        "1,1,null,1,1.0,",
        "2,2,2.0,null,2.0,",
        "3,3,3.0,3,null,",
        "4,null,4.0,4,4.0,",
        "5,5,null,null,5.0,",
        "6,6,6.0,null,6.0,",
        "7,7,7.0,7,null,",
        "8,8,8.0,8,8.0,",
        "9,null,9.0,null,null,",
        "10,10,null,10,10.0,",
        "11,11,11.0,null,11.0,",
        "12,12,12.0,12,null,"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setQueryThreadCount(1);
    // if we don't change this configuration, we may get an error like: Cannot reserve XXXX bytes of
    // direct buffer memory
    EnvFactory.getEnv().getConfig().getCommonConfig().setWalBufferSize(1024 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SELECT_INTO_SQL_LIST);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // -------------------------------------- ALIGN BY TIME ---------------------------------------

  @Test
  public void testRawDataQuery1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk1.new_d.t1,10,",
          "root.sg.d2.s1,root.sg_bk1.new_d.t2,7,",
          "root.sg.d1.s2,root.sg_bk1.new_d.t3,9,",
          "root.sg.d2.s2,root.sg_bk1.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk1.new_d(t1, t2, t3, t4) from root.sg.*;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk1.new_d.t1,root.sg_bk1.new_d.t3,root.sg_bk1.new_d.t2,root.sg_bk1.new_d.t4,";
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk1.new_d;", expectedQueryHeader, rawDataSet);
  }

  @Test
  public void testRawDataQuery2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk2.new_d1.s1,10,",
          "root.sg.d2.s1,root.sg_bk2.new_d2.s1,7,",
          "root.sg.d1.s2,root.sg_bk2.new_d1.s2,9,",
          "root.sg.d2.s2,root.sg_bk2.new_d2.s2,8,"
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk2.new_${2}(::) from root.sg.*;",
        selectIntoHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk2.new_d1.s1,root.sg_bk2.new_d1.s2,root.sg_bk2.new_d2.s1,root.sg_bk2.new_d2.s2,";
    resultSetEqualTest(
        "select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_bk2;",
        expectedQueryHeader,
        rawDataSet);
  }

  @Test
  public void testSamePathQuery() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk3.new_d1.t1,10,",
          "root.sg.d1.s2,root.sg_bk3.new_d1.t2,9,",
          "root.sg.d1.s1,root.sg_bk3.new_d2.t1,10,",
          "root.sg.d1.s2,root.sg_bk3.new_d2.t2,9,"
        };
    resultSetEqualTest(
        "select s1, s2, s1, s2 into root.sg_bk3.new_d1(t1, t2), aligned root.sg_bk3.new_d2(t1, t2) from root.sg.d1;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk3.new_d1.t1,root.sg_bk3.new_d1.t2,root.sg_bk3.new_d2.t1,root.sg_bk3.new_d2.t2,";
    String[] queryRetArray =
        new String[] {
          "1,1,null,1,null,",
          "2,2,2.0,2,2.0,",
          "3,3,3.0,3,3.0,",
          "4,null,4.0,null,4.0,",
          "5,5,null,5,null,",
          "6,6,6.0,6,6.0,",
          "7,7,7.0,7,7.0,",
          "8,8,8.0,8,8.0,",
          "9,null,9.0,null,9.0,",
          "10,10,null,10,null,",
          "11,11,11.0,11,11.0,",
          "12,12,12.0,12,12.0,"
        };
    resultSetEqualTest(
        "select new_d1.t1, new_d1.t2, new_d2.t1, new_d2.t2 from root.sg_bk3;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testEmptyQuery() {
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk4.new_${2}(::) from root.sg1.d1, root.sg1.d2;",
        selectIntoHeader, new String[] {});
  }

  @Test
  public void testAggregationQuery1() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg.d1.s1),root.sg_agg1.d1.count_s1,1,",
          "last_value(root.sg.d1.s2),root.sg_agg1.d1.last_value_s2,1,",
          "count(root.sg.d2.s1),root.sg_agg1.d2.count_s1,1,",
          "last_value(root.sg.d2.s2),root.sg_agg1.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(d1.s1), last_value(d1.s2), count(d2.s1), last_value(d2.s2) "
            + "into root.sg_agg1.d1(count_s1, last_value_s2), aligned root.sg_agg1.d2(count_s1, last_value_s2) "
            + "from root.sg;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg1.d1.count_s1,root.sg_agg1.d2.count_s1,root.sg_agg1.d1.last_value_s2,root.sg_agg1.d2.last_value_s2,";
    String[] queryRetArray = new String[] {"0,10,7,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg1.d1, root.sg_agg1.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testAggregationQuery2() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg.d1.s1),root.sg_agg2.d1.count_s1,4,",
          "last_value(root.sg.d1.s2),root.sg_agg2.d1.last_value_s2,4,",
          "count(root.sg.d2.s1),root.sg_agg2.d2.count_s1,4,",
          "last_value(root.sg.d2.s2),root.sg_agg2.d2.last_value_s2,4,"
        };
    resultSetEqualTest(
        "select count(d1.s1), last_value(d1.s2), count(d2.s1), last_value(d2.s2) "
            + "into aligned root.sg_agg2.d1(count_s1, last_value_s2), aligned root.sg_agg2.d2(count_s1, last_value_s2) "
            + "from root.sg group by ([1, 13), 3ms);",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg2.d1.count_s1,root.sg_agg2.d2.count_s1,root.sg_agg2.d1.last_value_s2,root.sg_agg2.d2.last_value_s2,";
    String[] queryRetArray =
        new String[] {"1,3,2,3.0,2.0,", "4,2,1,6.0,6.0,", "7,2,2,9.0,8.0,", "10,3,2,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg2.d1, root.sg_agg2.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testExpression() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1 + root.sg.d2.s1,root.sg_expr.d.k1,6,",
          "-sin(root.sg.d1.s2),root.sg_expr.d.k2,9,",
          "top_k(root.sg.d2.s2, \"k\"=\"3\"),root.sg_expr.d.k3,3,"
        };
    resultSetEqualTest(
        "select  d1.s1 + d2.s1, -sin(d1.s2), top_k(d2.s2,'k'='3') "
            + "into root.sg_expr.d(k1, k2, k3) from root.sg;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader = "Time,root.sg_expr.d.k1,root.sg_expr.d.k2,root.sg_expr.d.k3,";
    String[] queryRetArray =
        new String[] {
          "1,2.0,null,null,",
          "2,null,-0.9092974268256817,null,",
          "3,6.0,-0.1411200080598672,null,",
          "4,null,0.7568024953079282,null,",
          "6,null,0.27941549819892586,null,",
          "7,14.0,-0.6569865987187891,null,",
          "8,16.0,-0.9893582466233818,8.0,",
          "9,null,-0.4121184852417566,null,",
          "10,20.0,null,10.0,",
          "11,null,0.9999902065507035,11.0,",
          "12,24.0,0.5365729180004349,null,"
        };
    resultSetEqualTest(
        "select k1, k2, k3 from root.sg_expr.d;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testUsingUnMatchedAlignment() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk1.new_aligned_d.t1,10,",
          "root.sg.d2.s1,root.sg_bk1.new_aligned_d.t2,7,",
          "root.sg.d1.s2,root.sg_bk1.new_aligned_d.t3,9,",
          "root.sg.d2.s2,root.sg_bk1.new_aligned_d.t4,8,",
        };
    executeNonQuery(
        "CREATE ALIGNED TIMESERIES root.sg_bk1.new_aligned_d(t1 INT32, t2 INT32, t3 FLOAT, t4 FLOAT);");
    // use matched interface (aligned == aligned)
    resultSetEqualTest(
        "select s1, s2 into aligned root.sg_bk1.new_aligned_d(t1, t2, t3, t4) from root.sg.*;",
        selectIntoHeader,
        intoRetArray);
    String expectedQueryHeader =
        "Time,root.sg_bk1.new_aligned_d.t1,root.sg_bk1.new_aligned_d.t3,root.sg_bk1.new_aligned_d.t2,root.sg_bk1.new_aligned_d.t4,";
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk1.new_aligned_d;", expectedQueryHeader, rawDataSet);

    // use unmatched interface (non-aligned != aligned)
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk1.new_aligned_d(t1, t2, t3, t4) from root.sg.*;",
        selectIntoHeader,
        intoRetArray);
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk1.new_aligned_d;", expectedQueryHeader, rawDataSet);
  }

  // -------------------------------------- ALIGN BY DEVICE -------------------------------------

  @Test
  public void testRawDataQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_abd_bk1.new_d.t1,10,",
          "root.sg.d1,s2,root.sg_abd_bk1.new_d.t2,9,",
          "root.sg.d2,s1,root.sg_abd_bk1.new_d.t3,7,",
          "root.sg.d2,s2,root.sg_abd_bk1.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_abd_bk1.new_d(t1, t2), root.sg_abd_bk1.new_d(t3, t4) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_bk1.new_d.t1,root.sg_abd_bk1.new_d.t2,root.sg_abd_bk1.new_d.t3,root.sg_abd_bk1.new_d.t4,";
    resultSetEqualTest(
        "select t1, t2, t3, t4 from root.sg_abd_bk1.new_d;", expectedQueryHeader, rawDataSet);
  }

  @Test
  public void testRawDataQueryAlignByDevice2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_abd_bk2.new_d1.s1,10,",
          "root.sg.d1,s2,root.sg_abd_bk2.new_d1.s2,9,",
          "root.sg.d2,s1,root.sg_abd_bk2.new_d2.s1,7,",
          "root.sg.d2,s2,root.sg_abd_bk2.new_d2.s2,8,"
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_abd_bk2.new_${2}(::) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_bk2.new_d1.s1,root.sg_abd_bk2.new_d1.s2,root.sg_abd_bk2.new_d2.s1,root.sg_abd_bk2.new_d2.s2,";
    resultSetEqualTest(
        "select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_abd_bk2;",
        expectedQueryHeader,
        rawDataSet);
  }

  @Test
  public void testSamePathQueryAlignByDevice() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_abd_bk3.new_d1.t1,10,",
          "root.sg.d1,s2,root.sg_abd_bk3.new_d1.t2,9,",
          "root.sg.d1,s1,root.sg_abd_bk3.new_d1.t3,10,",
          "root.sg.d1,s2,root.sg_abd_bk3.new_d1.t4,9,"
        };
    resultSetEqualTest(
        "select s1, s2, s1, s2 into root.sg_abd_bk3.new_d1(t1, t2, t3, t4) from root.sg.d1 align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_bk3.new_d1.t1,root.sg_abd_bk3.new_d1.t2,root.sg_abd_bk3.new_d1.t3,root.sg_abd_bk3.new_d1.t4,";
    String[] queryRetArray =
        new String[] {
          "1,1,null,1,null,",
          "2,2,2.0,2,2.0,",
          "3,3,3.0,3,3.0,",
          "4,null,4.0,null,4.0,",
          "5,5,null,5,null,",
          "6,6,6.0,6,6.0,",
          "7,7,7.0,7,7.0,",
          "8,8,8.0,8,8.0,",
          "9,null,9.0,null,9.0,",
          "10,10,null,10,null,",
          "11,11,11.0,11,11.0,",
          "12,12,12.0,12,12.0,"
        };
    resultSetEqualTest(
        "select t1, t2, t3, t4 from root.sg_abd_bk3.new_d1;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testEmptyQueryAlignByDevice() {
    resultSetEqualTest(
        "select s1, s2 into root.sg_abd_bk4.new_${2}(::) from root.sg1.d1, root.sg1.d2 align by device;",
        selectIntoAlignByDeviceHeader, new String[] {});
  }

  @Test
  public void testAggregationQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,count(s1),root.sg_abd_agg1.d1.count_s1,1,",
          "root.sg.d1,last_value(s2),root.sg_abd_agg1.d1.last_value_s2,1,",
          "root.sg.d2,count(s1),root.sg_abd_agg1.d2.count_s1,1,",
          "root.sg.d2,last_value(s2),root.sg_abd_agg1.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(s1), last_value(s2) "
            + "into root.sg_abd_agg1.${2}(count_s1, last_value_s2) "
            + "from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_agg1.d1.count_s1,root.sg_abd_agg1.d2.count_s1,"
            + "root.sg_abd_agg1.d1.last_value_s2,root.sg_abd_agg1.d2.last_value_s2,";
    String[] queryRetArray = new String[] {"0,10,7,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_abd_agg1.d1, root.sg_abd_agg1.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testAggregationQueryAlignByDevice2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,count(s1),root.sg_abd_agg2.d1.count_s1,4,",
          "root.sg.d1,last_value(s2),root.sg_abd_agg2.d1.last_value_s2,4,",
          "root.sg.d2,count(s1),root.sg_abd_agg2.d2.count_s1,4,",
          "root.sg.d2,last_value(s2),root.sg_abd_agg2.d2.last_value_s2,4,"
        };
    resultSetEqualTest(
        "select count(s1), last_value(s2) "
            + "into aligned root.sg_abd_agg2.${2}(count_s1, last_value_s2) "
            + "from root.sg.* group by ([1, 13), 3ms) align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_agg2.d1.count_s1,root.sg_abd_agg2.d2.count_s1,"
            + "root.sg_abd_agg2.d1.last_value_s2,root.sg_abd_agg2.d2.last_value_s2,";
    String[] queryRetArray =
        new String[] {"1,3,2,3.0,2.0,", "4,2,1,6.0,6.0,", "7,2,2,9.0,8.0,", "10,3,2,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_abd_agg2.d1, root.sg_abd_agg2.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testExpressionAlignByDevice() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1 + s2,root.sg_abd_expr.d1.k1,7,",
          "root.sg.d1,-sin(s1),root.sg_abd_expr.d1.k2,10,",
          "root.sg.d1,top_k(s2, \"k\"=\"3\"),root.sg_abd_expr.d1.k3,3,",
          "root.sg.d2,s1 + s2,root.sg_abd_expr.d2.k1,4,",
          "root.sg.d2,-sin(s1),root.sg_abd_expr.d2.k2,7,",
          "root.sg.d2,top_k(s2, \"k\"=\"3\"),root.sg_abd_expr.d2.k3,3,",
        };
    resultSetEqualTest(
        "select s1 + s2, -sin(s1), top_k(s2,'k'='3') "
            + "into root.sg_abd_expr.::(k1, k2, k3) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_expr.d1.k1,root.sg_abd_expr.d2.k1,root.sg_abd_expr.d1.k2,"
            + "root.sg_abd_expr.d2.k2,root.sg_abd_expr.d1.k3,root.sg_abd_expr.d2.k3,";
    String[] queryRetArray =
        new String[] {
          "1,null,2.0,-0.8414709848078965,-0.8414709848078965,null,null,",
          "2,4.0,null,-0.9092974268256817,null,null,null,",
          "3,6.0,null,-0.1411200080598672,-0.1411200080598672,null,null,",
          "4,null,8.0,null,0.7568024953079282,null,null,",
          "5,null,null,0.9589242746631385,null,null,null,",
          "6,12.0,null,0.27941549819892586,null,null,null,",
          "7,14.0,null,-0.6569865987187891,-0.6569865987187891,null,null,",
          "8,16.0,16.0,-0.9893582466233818,-0.9893582466233818,null,8.0,",
          "9,null,null,null,null,9.0,null,",
          "10,null,20.0,0.5440211108893698,0.5440211108893698,null,10.0,",
          "11,22.0,null,0.9999902065507035,null,11.0,11.0,",
          "12,24.0,null,0.5365729180004349,0.5365729180004349,12.0,null,"
        };
    resultSetEqualTest(
        "select k1, k2, k3 from root.sg_abd_expr.*;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testExpressionAlignByDevice2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,avg(s1),root.agg_expr.d1.avg_s1,1,",
          "root.sg.d1,sum(s1) + sum(s1),root.agg_expr.d1.sum_s1_add_s1,1,",
          "root.sg.d1,count(s2),root.agg_expr.d1.count_s2,1,",
          "root.sg.d2,avg(s1),root.agg_expr.d2.avg_s1,1,",
          "root.sg.d2,sum(s1) + sum(s1),root.agg_expr.d2.sum_s1_add_s1,1,",
          "root.sg.d2,count(s2),root.agg_expr.d2.count_s2,1,",
        };
    resultSetEqualTest(
        "select avg(s1), sum(s1) + sum(s1), count(s2)"
            + " into root.agg_expr.${2}(avg_s1, sum_s1_add_s1, count_s2)"
            + " from root.sg.d1, root.sg.d2 align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.agg_expr.d1.avg_s1,root.agg_expr.d2.avg_s1,root.agg_expr.d1.sum_s1_add_s1,"
            + "root.agg_expr.d2.sum_s1_add_s1,root.agg_expr.d1.count_s2,root.agg_expr.d2.count_s2,";
    String[] queryRetArray =
        new String[] {
          "0,6.5,6.428571428571429,130.0,90.0,9,8,",
        };
    resultSetEqualTest(
        "select avg_s1, sum_s1_add_s1, count_s2 from root.agg_expr.*;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testAliasAlignByDevice() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_abd_alias.d1.k1,10,",
        };
    resultSetEqualTest(
        "select s1 as k1 " + "into root.sg_abd_alias.d1(::) from root.sg.d1 align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    intoRetArray =
        new String[] {
          "k1,root.sg_abd_alias.d2.k1,10,",
        };
    resultSetEqualTest(
        "select s1 as k1 " + "into root.sg_abd_alias.d2(::) from root.sg.d1;",
        selectIntoHeader,
        intoRetArray);
  }

  // -------------------------------------- CHECK EXCEPTION -------------------------------------

  @Test
  public void testPermission1() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser1 'temppw1'");
      adminStmt.execute("GRANT WRITE_DATA on root.sg_bk.** TO USER tempuser1;");
      ResultSet resultSet;

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser1", "temppw1");
          Statement userStmt = userCon.createStatement()) {
        userStmt.executeQuery(
            "select s1, s2 into root.sg_bk.new_d(t1, t2, t3, t4) from root.sg.*;");
        resultSet = userStmt.executeQuery("select * from root.sg_bk.new_d");
        Assert.assertEquals(resultSet.next(), false);
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains("No permissions for this operation, please add privilege READ_DATA"));
      }
    }
  }

  @Test
  public void testPermission2() throws SQLException {
    try (Connection adminCon = EnvFactory.getEnv().getConnection();
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("CREATE USER tempuser2 'temppw2'");
      adminStmt.execute("GRANT WRITE_DATA on root.sg.** TO USER tempuser2;");

      try (Connection userCon = EnvFactory.getEnv().getConnection("tempuser2", "temppw2");
          Statement userStmt = userCon.createStatement()) {
        userStmt.executeQuery(
            "select s1, s2 into root.sg_bk.new_d(t1, t2, t3, t4) from root.sg.*;");
        fail("No exception!");
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains("No permissions for this operation, please add privilege WRITE_DATA"));
      }
    }
  }

  // -------------------------------------- DATATYPE CAST TEST -------------------------------------

  @Test
  public void testDataTypeIncompatible() {
    // test INT32
    assertTestFail(
        "select s_int32 into root.sg_type.d_1(s_boolean) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_boolean[BOOLEAN]) is not compatible with the data type of source column (root.sg_type.d_0.s_int32[INT32]).");
    assertTestFail(
        "select s_int32 into root.sg_type.d_1(s_text) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_text[TEXT]) is not compatible with the data type of source column (root.sg_type.d_0.s_int32[INT32]).");

    // test INT64
    assertTestFail(
        "select s_int64 into root.sg_type.d_1(s_int32) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int32[INT32]) is not compatible with the data type of source column (root.sg_type.d_0.s_int64[INT64]).");
    assertTestFail(
        "select s_int64 into root.sg_type.d_1(s_float) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_float[FLOAT]) is not compatible with the data type of source column (root.sg_type.d_0.s_int64[INT64]).");
    assertTestFail(
        "select s_int64 into root.sg_type.d_1(s_boolean) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_boolean[BOOLEAN]) is not compatible with the data type of source column (root.sg_type.d_0.s_int64[INT64]).");
    assertTestFail(
        "select s_int64 into root.sg_type.d_1(s_text) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_text[TEXT]) is not compatible with the data type of source column (root.sg_type.d_0.s_int64[INT64]).");

    // test FLOAT
    assertTestFail(
        "select s_float into root.sg_type.d_1(s_int32) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int32[INT32]) is not compatible with the data type of source column (root.sg_type.d_0.s_float[FLOAT]).");
    assertTestFail(
        "select s_float into root.sg_type.d_1(s_int64) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int64[INT64]) is not compatible with the data type of source column (root.sg_type.d_0.s_float[FLOAT]).");
    assertTestFail(
        "select s_float into root.sg_type.d_1(s_boolean) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_boolean[BOOLEAN]) is not compatible with the data type of source column (root.sg_type.d_0.s_float[FLOAT]).");
    assertTestFail(
        "select s_float into root.sg_type.d_1(s_text) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_text[TEXT]) is not compatible with the data type of source column (root.sg_type.d_0.s_float[FLOAT]).");

    // test DOUBLE
    assertTestFail(
        "select s_double into root.sg_type.d_1(s_int32) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int32[INT32]) is not compatible with the data type of source column (root.sg_type.d_0.s_double[DOUBLE]).");
    assertTestFail(
        "select s_double into root.sg_type.d_1(s_int64) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int64[INT64]) is not compatible with the data type of source column (root.sg_type.d_0.s_double[DOUBLE]).");
    assertTestFail(
        "select s_double into root.sg_type.d_1(s_float) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_float[FLOAT]) is not compatible with the data type of source column (root.sg_type.d_0.s_double[DOUBLE]).");
    assertTestFail(
        "select s_double into root.sg_type.d_1(s_boolean) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_boolean[BOOLEAN]) is not compatible with the data type of source column (root.sg_type.d_0.s_double[DOUBLE]).");
    assertTestFail(
        "select s_double into root.sg_type.d_1(s_text) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_text[TEXT]) is not compatible with the data type of source column (root.sg_type.d_0.s_double[DOUBLE]).");

    // test BOOLEAN
    assertTestFail(
        "select s_boolean into root.sg_type.d_1(s_int32) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int32[INT32]) is not compatible with the data type of source column (root.sg_type.d_0.s_boolean[BOOLEAN]).");
    assertTestFail(
        "select s_boolean into root.sg_type.d_1(s_int64) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int64[INT64]) is not compatible with the data type of source column (root.sg_type.d_0.s_boolean[BOOLEAN]).");
    assertTestFail(
        "select s_boolean into root.sg_type.d_1(s_float) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_float[FLOAT]) is not compatible with the data type of source column (root.sg_type.d_0.s_boolean[BOOLEAN]).");
    assertTestFail(
        "select s_boolean into root.sg_type.d_1(s_double) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_double[DOUBLE]) is not compatible with the data type of source column (root.sg_type.d_0.s_boolean[BOOLEAN]).");
    assertTestFail(
        "select s_boolean into root.sg_type.d_1(s_text) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_text[TEXT]) is not compatible with the data type of source column (root.sg_type.d_0.s_boolean[BOOLEAN]).");

    // test TEXT
    assertTestFail(
        "select s_text into root.sg_type.d_1(s_int32) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int32[INT32]) is not compatible with the data type of source column (root.sg_type.d_0.s_text[TEXT]).");
    assertTestFail(
        "select s_text into root.sg_type.d_1(s_int64) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_int64[INT64]) is not compatible with the data type of source column (root.sg_type.d_0.s_text[TEXT]).");
    assertTestFail(
        "select s_text into root.sg_type.d_1(s_float) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_float[FLOAT]) is not compatible with the data type of source column (root.sg_type.d_0.s_text[TEXT]).");
    assertTestFail(
        "select s_text into root.sg_type.d_1(s_double) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_double[DOUBLE]) is not compatible with the data type of source column (root.sg_type.d_0.s_text[TEXT]).");
    assertTestFail(
        "select s_text into root.sg_type.d_1(s_boolean) from root.sg_type.d_0;",
        "The data type of target path (root.sg_type.d_1.s_boolean[BOOLEAN]) is not compatible with the data type of source column (root.sg_type.d_0.s_text[TEXT]).");
  }

  @Test
  public void testDataTypeAutoCast() {
    String[] intoRetArray =
        new String[] {
          "root.sg_type.d_0.s_int32,root.sg_type.d_1.s_int64,12,",
          "root.sg_type.d_0.s_int32,root.sg_type.d_1.s_float,12,",
          "root.sg_type.d_0.s_int32,root.sg_type.d_1.s_double,12,",
          "root.sg_type.d_0.s_int64,root.sg_type.d_2.s_double,12,",
          "root.sg_type.d_0.s_float,root.sg_type.d_3.s_double,12,",
        };
    resultSetEqualTest(
        "select s_int32, s_int32, s_int32, s_int64, s_float "
            + " into root.sg_type.d_1(s_int64, s_float, s_double), root.sg_type.d_2(s_double), root.sg_type.d_3(s_double) "
            + " from root.sg_type.d_0;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_type.d_1.s_int64,root.sg_type.d_1.s_float,root.sg_type.d_1.s_double,"
            + "root.sg_type.d_2.s_double,root.sg_type.d_3.s_double,";
    String[] queryRetArray =
        new String[] {
          "0,0,0.0,0.0,0.0,0.0,",
          "1,1,1.0,1.0,1.0,1.0,",
          "2,2,2.0,2.0,2.0,2.0,",
          "3,3,3.0,3.0,3.0,3.0,",
          "4,4,4.0,4.0,4.0,4.0,",
          "5,5,5.0,5.0,5.0,5.0,",
          "6,6,6.0,6.0,6.0,6.0,",
          "7,7,7.0,7.0,7.0,7.0,",
          "8,8,8.0,8.0,8.0,8.0,",
          "9,9,9.0,9.0,9.0,9.0,",
          "10,10,10.0,10.0,10.0,10.0,",
          "11,11,11.0,11.0,11.0,11.0,"
        };
    resultSetEqualTest(
        "select d_1.s_int64, d_1.s_float, d_1.s_double, d_2.s_double, d_3.s_double from root.sg_type;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testNewDataType() {
    String[] intoRetArray =
        new String[] {
          "root.db.d1.s7,root.db.d2.s7,2,",
          "root.db.d1.s8,root.db.d2.s8,2,",
          "root.db.d1.s9,root.db.d2.s9,2,",
          "root.db.d1.s10,root.db.d2.s10,2,",
        };

    resultSetEqualTest(
        "select s7 into root.db.d2(s7) from root.db.d1;",
        selectIntoHeader,
        new String[] {intoRetArray[0]});
    resultSetEqualTest(
        "select s8 into root.db.d2(s8) from root.db.d1;",
        selectIntoHeader,
        new String[] {intoRetArray[1]});
    resultSetEqualTest(
        "select s9 into root.db.d2(s9) from root.db.d1;",
        selectIntoHeader,
        new String[] {intoRetArray[2]});
    resultSetEqualTest(
        "select s10 into root.db.d2(s10) from root.db.d1;",
        selectIntoHeader,
        new String[] {intoRetArray[3]});

    String[] resultSet =
        new String[] {
          "1,1997-01-01T00:00:00.001Z,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "3,1997-01-03T00:00:00.001Z,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    String expectedQueryHeader = "Time,root.db.d2.s7,root.db.d2.s8,root.db.d2.s9,root.db.d2.s10,";
    resultSetEqualTest("select s7,s8,s9,s10 from root.db.d2;", expectedQueryHeader, resultSet);
  }
}
