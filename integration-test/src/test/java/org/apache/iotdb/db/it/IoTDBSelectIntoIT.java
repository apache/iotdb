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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSelectIntoIT {

  private int selectIntoInsertTabletPlanRowLimit;

  private static final String[] SQLs =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
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
        "SET STORAGE GROUP TO root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE"
      };

  private static final String selectIntoHeader = "source column,target timeseries,written,";
  private static final String selectIntoAlignByDeviceHeader =
      "source device,source column,target timeseries,written,";

  private static final String[] rawDataSet =
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

  @Before
  public void setUp() throws Exception {
    selectIntoInsertTabletPlanRowLimit =
        ConfigFactory.getConfig().getSelectIntoInsertTabletPlanRowLimit();
    ConfigFactory.getConfig().setSelectIntoInsertTabletPlanRowLimit(5);
    EnvFactory.getEnv().initBeforeTest();
    prepareData(SQLs);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig()
        .setSelectIntoInsertTabletPlanRowLimit(selectIntoInsertTabletPlanRowLimit);
  }

  // -------------------------------------- ALIGN BY TIME ---------------------------------------

  @Test
  public void testRawDataQuery1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk.new_d.t1,10,",
          "root.sg.d2.s1,root.sg_bk.new_d.t2,7,",
          "root.sg.d1.s2,root.sg_bk.new_d.t3,9,",
          "root.sg.d2.s2,root.sg_bk.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_d(t1, t2, t3, t4) from root.sg.*;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d.t1,root.sg_bk.new_d.t3,root.sg_bk.new_d.t2,root.sg_bk.new_d.t4,";
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk.new_d;", expectedQueryHeader, rawDataSet);
  }

  @Test
  public void testRawDataQuery2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk.new_d1.s1,10,",
          "root.sg.d2.s1,root.sg_bk.new_d2.s1,7,",
          "root.sg.d1.s2,root.sg_bk.new_d1.s2,9,",
          "root.sg.d2.s2,root.sg_bk.new_d2.s2,8,"
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_${2}(::) from root.sg.*;",
        selectIntoHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d1.s1,root.sg_bk.new_d1.s2,root.sg_bk.new_d2.s1,root.sg_bk.new_d2.s2,";
    resultSetEqualTest(
        "select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_bk;",
        expectedQueryHeader,
        rawDataSet);
  }

  @Test
  public void testAggregationQuery1() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg.d1.s1),root.sg_agg.d1.count_s1,1,",
          "last_value(root.sg.d1.s2),root.sg_agg.d1.last_value_s2,1,",
          "count(root.sg.d2.s1),root.sg_agg.d2.count_s1,1,",
          "last_value(root.sg.d2.s2),root.sg_agg.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(d1.s1), last_value(d1.s2), count(d2.s1), last_value(d2.s2) "
            + "into root.sg_agg.d1(count_s1, last_value_s2), root.sg_agg.d2(count_s1, last_value_s2) "
            + "from root.sg;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg.d1.count_s1,root.sg_agg.d2.count_s1,root.sg_agg.d1.last_value_s2,root.sg_agg.d2.last_value_s2,";
    String[] queryRetArray = new String[] {"0,10,7,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg.d1, root.sg_agg.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testAggregationQuery2() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg.d1.s1),root.sg_agg.d1.count_s1,4,",
          "last_value(root.sg.d1.s2),root.sg_agg.d1.last_value_s2,4,",
          "count(root.sg.d2.s1),root.sg_agg.d2.count_s1,4,",
          "last_value(root.sg.d2.s2),root.sg_agg.d2.last_value_s2,4,"
        };
    resultSetEqualTest(
        "select count(d1.s1), last_value(d1.s2), count(d2.s1), last_value(d2.s2) "
            + "into root.sg_agg.d1(count_s1, last_value_s2), root.sg_agg.d2(count_s1, last_value_s2) "
            + "from root.sg group by ([1, 13), 3ms);",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg.d1.count_s1,root.sg_agg.d2.count_s1,root.sg_agg.d1.last_value_s2,root.sg_agg.d2.last_value_s2,";
    String[] queryRetArray =
        new String[] {"1,3,2,3.0,2.0,", "4,2,1,6.0,6.0,", "7,2,2,9.0,8.0,", "10,3,2,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg.d1, root.sg_agg.d2;",
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
  public void testSamePathQuery() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk.new_d1.t1,10,",
          "root.sg.d1.s2,root.sg_bk.new_d1.t2,9,",
          "root.sg.d1.s1,root.sg_bk.new_d2.t1,10,",
          "root.sg.d1.s2,root.sg_bk.new_d2.t2,9,"
        };
    resultSetEqualTest(
        "select s1, s2, s1, s2 into root.sg_bk.new_d1(t1, t2), root.sg_bk.new_d2(t1, t2) from root.sg.d1;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d1.t1,root.sg_bk.new_d1.t2,root.sg_bk.new_d2.t1,root.sg_bk.new_d2.t2,";
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
        "select new_d1.t1, new_d1.t2, new_d2.t1, new_d2.t2 from root.sg_bk;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testEmptyQuery() {
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_${2}(::) from root.sg1.d1, root.sg1.d2;",
        selectIntoHeader, new String[] {});
  }

  // -------------------------------------- ALIGN BY DEVICE -------------------------------------

  @Test
  public void testRawDataQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_bk.new_d.t1,10,",
          "root.sg.d1,s2,root.sg_bk.new_d.t2,9,",
          "root.sg.d2,s1,root.sg_bk.new_d.t3,7,",
          "root.sg.d2,s2,root.sg_bk.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_d(t1, t2), root.sg_bk.new_d(t3, t4) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d.t1,root.sg_bk.new_d.t2,root.sg_bk.new_d.t3,root.sg_bk.new_d.t4,";
    resultSetEqualTest(
        "select t1, t2, t3, t4 from root.sg_bk.new_d;", expectedQueryHeader, rawDataSet);
  }

  @Test
  public void testRawDataQueryAlignByDevice2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_bk.new_d1.s1,10,",
          "root.sg.d1,s2,root.sg_bk.new_d1.s2,9,",
          "root.sg.d2,s1,root.sg_bk.new_d2.s1,7,",
          "root.sg.d2,s2,root.sg_bk.new_d2.s2,8,"
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_${2}(::) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d1.s1,root.sg_bk.new_d1.s2,root.sg_bk.new_d2.s1,root.sg_bk.new_d2.s2,";
    resultSetEqualTest(
        "select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_bk;",
        expectedQueryHeader,
        rawDataSet);
  }

  @Test
  public void testAggregationQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,count(s1),root.sg_agg.d1.count_s1,1,",
          "root.sg.d1,last_value(s2),root.sg_agg.d1.last_value_s2,1,",
          "root.sg.d2,count(s1),root.sg_agg.d2.count_s1,1,",
          "root.sg.d2,last_value(s2),root.sg_agg.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(s1), last_value(s2) "
            + "into root.sg_agg.${2}(count_s1, last_value_s2) "
            + "from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg.d1.count_s1,root.sg_agg.d2.count_s1,root.sg_agg.d1.last_value_s2,root.sg_agg.d2.last_value_s2,";
    String[] queryRetArray = new String[] {"0,10,7,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg.d1, root.sg_agg.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testAggregationQueryAlignByDevice2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,count(s1),root.sg_agg.d1.count_s1,4,",
          "root.sg.d1,last_value(s2),root.sg_agg.d1.last_value_s2,4,",
          "root.sg.d2,count(s1),root.sg_agg.d2.count_s1,4,",
          "root.sg.d2,last_value(s2),root.sg_agg.d2.last_value_s2,4,"
        };
    resultSetEqualTest(
        "select count(s1), last_value(s2) "
            + "into root.sg_agg.${2}(count_s1, last_value_s2) "
            + "from root.sg.* group by ([1, 13), 3ms) align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_agg.d1.count_s1,root.sg_agg.d2.count_s1,root.sg_agg.d1.last_value_s2,root.sg_agg.d2.last_value_s2,";
    String[] queryRetArray =
        new String[] {"1,3,2,3.0,2.0,", "4,2,1,6.0,6.0,", "7,2,2,9.0,8.0,", "10,3,2,12.0,11.0,"};
    resultSetEqualTest(
        "select count_s1, last_value_s2 from root.sg_agg.d1, root.sg_agg.d2;",
        expectedQueryHeader,
        queryRetArray);
  }

  @Test
  public void testExpressionAlignByDevice() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1 + s2,root.sg_expr.d1.k1,7,",
          "root.sg.d1,-sin(s1),root.sg_expr.d1.k2,10,",
          "root.sg.d1,top_k(s2, \"k\"=\"3\"),root.sg_expr.d1.k3,3,",
          "root.sg.d2,s1 + s2,root.sg_expr.d2.k1,4,",
          "root.sg.d2,-sin(s1),root.sg_expr.d2.k2,7,",
          "root.sg.d2,top_k(s2, \"k\"=\"3\"),root.sg_expr.d2.k3,3,",
        };
    resultSetEqualTest(
        "select s1 + s2, -sin(s1), top_k(s2,'k'='3') "
            + "into root.sg_expr.::(k1, k2, k3) from root.sg.* align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_expr.d1.k1,root.sg_expr.d2.k1,root.sg_expr.d1.k2,"
            + "root.sg_expr.d2.k2,root.sg_expr.d1.k3,root.sg_expr.d2.k3,";
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
        "select k1, k2, k3 from root.sg_expr.*;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testSamePathQueryAlignByDevice() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1,s1,root.sg_bk.new_d1.t1,10,",
          "root.sg.d1,s2,root.sg_bk.new_d1.t2,9,",
          "root.sg.d1,s1,root.sg_bk.new_d1.t3,10,",
          "root.sg.d1,s2,root.sg_bk.new_d1.t4,9,"
        };
    resultSetEqualTest(
        "select s1, s2, s1, s2 into root.sg_bk.new_d1(t1, t2, t3, t4) from root.sg.d1 align by device;",
        selectIntoAlignByDeviceHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d1.t1,root.sg_bk.new_d1.t2,root.sg_bk.new_d1.t3,root.sg_bk.new_d1.t4,";
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
        "select t1, t2, t3, t4 from root.sg_bk.new_d1;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testEmptyQueryAlignByDevice() {
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_${2}(::) from root.sg1.d1, root.sg1.d2 align by device;",
        selectIntoAlignByDeviceHeader, new String[] {});
  }
}
