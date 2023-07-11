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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.selectinto.IoTDBSelectIntoIT.SELECT_INTO_SQL_LIST;
import static org.apache.iotdb.db.it.selectinto.IoTDBSelectIntoIT.rawDataSet;
import static org.apache.iotdb.db.it.selectinto.IoTDBSelectIntoIT.selectIntoAlignByDeviceHeader;
import static org.apache.iotdb.db.it.selectinto.IoTDBSelectIntoIT.selectIntoHeader;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSelectIntoWithViewIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setQueryThreadCount(1);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SELECT_INTO_SQL_LIST);
    prepareData(VIEW_SQL_LIST);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static final List<String> VIEW_SQL_LIST =
      Arrays.asList(
          "CREATE VIEW root.sg_view.d1.s1 AS SELECT s1 FROM root.sg.d1;",
          "CREATE VIEW root.sg_view.d1.s2 AS SELECT s2 FROM root.sg.d1;",
          "CREATE VIEW root.sg_view.d2.s1 AS SELECT s1 FROM root.sg.d2;",
          "CREATE VIEW root.sg_view.d2.s2 AS SELECT s2 FROM root.sg.d2;",
          "CREATE VIEW root.sg_view.da.sa AS SELECT s1 FROM root.sg.d1;",
          "CREATE VIEW root.sg_view.non_write.t1 AS SELECT s1/2 FROM root.sg.d1;");

  private static final String[] s1saDataSet =
      new String[] {
        "1,1,1,",
        "2,2,2,",
        "3,3,3,",
        "5,5,5,",
        "6,6,6,",
        "7,7,7,",
        "8,8,8,",
        "10,10,10,",
        "11,11,11,",
        "12,12,12,"
      };

  @Test
  public void testRawDataQuery() {
    String[] intoRetArray =
        new String[] {
          "root.sg_view.d1.s1,root.sg_bk1.new_d.t1,10,",
          "root.sg_view.d2.s1,root.sg_bk1.new_d.t2,7,",
          "root.sg_view.d1.s2,root.sg_bk1.new_d.t3,9,",
          "root.sg_view.d2.s2,root.sg_bk1.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk1.new_d(t1, t2, t3, t4) from root.sg_view.*;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk1.new_d.t1,root.sg_bk1.new_d.t3,root.sg_bk1.new_d.t2,root.sg_bk1.new_d.t4,";
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk1.new_d;", expectedQueryHeader, rawDataSet);
  }

  /**
   * Source column has two timeseries, while one is base series and another is view series, but the
   * original path of view path is same base series
   */
  @Test
  public void testRawDataQueryWhenSourceHasSamePaths() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk_s1sa.d1.s1,10,", "root.sg_view.da.sa,root.sg_bk_s1sa.da.sa,10,",
        };
    resultSetEqualTest(
        "SELECT s1,sa into root.sg_bk_s1sa.::(::) from root.sg.d1, root.sg_view.da;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader = "Time,root.sg_bk_s1sa.d1.s1,root.sg_bk_s1sa.da.sa,";
    resultSetEqualTest("select s1, sa from root.sg_bk_s1sa.**;", expectedQueryHeader, s1saDataSet);
  }

  @Test
  public void testSamePathQuery() {
    String[] intoRetArray =
        new String[] {
          "root.sg_view.d1.s1,root.sg_bk3.new_d1.t1,10,",
          "root.sg_view.d1.s2,root.sg_bk3.new_d1.t2,9,",
          "root.sg_view.d1.s1,root.sg_bk3.new_d2.t1,10,",
          "root.sg_view.d1.s2,root.sg_bk3.new_d2.t2,9,"
        };
    resultSetEqualTest(
        "select s1, s2, s1, s2 into root.sg_bk3.new_d1(t1, t2), aligned root.sg_bk3.new_d2(t1, t2) from root.sg_view.d1;",
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
  public void testAggregationQuery1() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg_view.d1.s1),root.sg_agg1.d1.count_s1,1,",
          "last_value(root.sg_view.d1.s2),root.sg_agg1.d1.last_value_s2,1,",
          "count(root.sg_view.d2.s1),root.sg_agg1.d2.count_s1,1,",
          "last_value(root.sg_view.d2.s2),root.sg_agg1.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(d1.s1), last_value(d1.s2), count(d2.s1), last_value(d2.s2) "
            + "into root.sg_agg1.d1(count_s1, last_value_s2), aligned root.sg_agg1.d2(count_s1, last_value_s2) "
            + "from root.sg_view;",
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
  public void testExpression() {
    String[] intoRetArray =
        new String[] {
          "root.sg_view.d1.s1 + root.sg_view.d2.s1,root.sg_expr.d.k1,6,",
          "-sin(root.sg_view.d1.s2),root.sg_expr.d.k2,9,",
          "top_k(root.sg_view.d2.s2, \"k\"=\"3\"),root.sg_expr.d.k3,3,"
        };
    resultSetEqualTest(
        "select  d1.s1 + d2.s1, -sin(d1.s2), top_k(d2.s2,'k'='3') "
            + "into root.sg_expr.d(k1, k2, k3) from root.sg_view;",
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
  public void testRawDataQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg_view.d1,s1,root.sg_abd_bk1.new_d.t1,10,",
          "root.sg_view.d1,s2,root.sg_abd_bk1.new_d.t2,9,",
          "root.sg_view.d2,s1,root.sg_abd_bk1.new_d.t3,7,",
          "root.sg_view.d2,s2,root.sg_abd_bk1.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_abd_bk1.new_d(t1, t2), root.sg_abd_bk1.new_d(t3, t4) from root.sg_view.* align by device;",
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
          "root.sg_view.d1,s1,root.sg_abd_bk2.new_d1.s1,10,",
          "root.sg_view.d1,s2,root.sg_abd_bk2.new_d1.s2,9,",
          "root.sg_view.d2,s1,root.sg_abd_bk2.new_d2.s1,7,",
          "root.sg_view.d2,s2,root.sg_abd_bk2.new_d2.s2,8,"
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_abd_bk2.new_${2}(::) from root.sg_view.* align by device;",
        selectIntoAlignByDeviceHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_abd_bk2.new_d1.s1,root.sg_abd_bk2.new_d1.s2,root.sg_abd_bk2.new_d2.s1,root.sg_abd_bk2.new_d2.s2,";
    resultSetEqualTest(
        "select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_abd_bk2;",
        expectedQueryHeader,
        rawDataSet);
  }

  @Test
  public void testAggregationQueryAlignByDevice1() {
    String[] intoRetArray =
        new String[] {
          "root.sg_view.d1,count(s1),root.sg_abd_agg1.d1.count_s1,1,",
          "root.sg_view.d1,last_value(s2),root.sg_abd_agg1.d1.last_value_s2,1,",
          "root.sg_view.d2,count(s1),root.sg_abd_agg1.d2.count_s1,1,",
          "root.sg_view.d2,last_value(s2),root.sg_abd_agg1.d2.last_value_s2,1,"
        };
    resultSetEqualTest(
        "select count(s1), last_value(s2) "
            + "into root.sg_abd_agg1.${2}(count_s1, last_value_s2) "
            + "from root.sg_view.* align by device;",
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
  public void testNonWriteView() {
    String[] intoRetArray =
        new String[] {
          "count(root.sg_view.non_write.t1),root.sg_view_test.non_write.count_t1,1,",
        };
    resultSetEqualTest(
        "select count(t1) into root.sg_view_test.non_write(count_t1) from root.sg_view.non_write;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader = "Time,root.sg_view_test.non_write.count_t1,";
    String[] queryRetArray = new String[] {"0,10,"};
    resultSetEqualTest(
        "select count_t1 from root.sg_view_test.non_write;", expectedQueryHeader, queryRetArray);
  }
}
