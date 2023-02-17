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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBNullOperandIT {
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s5 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "INSERT INTO root.test.sg1(timestamp,s1,s3,s4) values(1, 1, true, false)",
        "INSERT INTO root.test.sg1(timestamp,s1,s3) values(2, 2, true)",
        "INSERT INTO root.test.sg1(timestamp,s1,s4) values(3, 3, false)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * ArithmeticOperations: +, -, *, /, %
   *
   * <p>The result will be NULL if any Operand of ArithmeticOperations is NULL.
   */
  @Test
  public void testArithmeticOperations() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.test.sg1.s1 + root.test.sg1.s2",
          "root.test.sg1.s1 - root.test.sg1.s2",
          "root.test.sg1.s1 * root.test.sg1.s2",
          "root.test.sg1.s1 / root.test.sg1.s2",
          "root.test.sg1.s1 % root.test.sg1.s2",
          "root.test.sg1.s2 % root.test.sg1.s2",
        };
    String[] retArray =
        new String[] {
          "1,null,null,null,null,null,null,",
          "2,null,null,null,null,null,null,",
          "3,null,null,null,null,null,null,",
        };
    resultSetEqualTest(
        "select s1+s2, s1-s2, s1*s2, s1/s2, s1%s2, s2%s2 from root.test.sg1",
        expectedHeader, retArray);
  }

  /**
   * CompareOperations: =, >, <, like, in, between...(don't include `is null`)
   *
   * <p>The result will be NULL if any Operand of CompareOperations is NULL.
   */
  @Test
  public void testCompareOperations() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.test.sg1.s1 = root.test.sg1.s2",
          "root.test.sg1.s1 > root.test.sg1.s2",
          "root.test.sg1.s1 < root.test.sg1.s2",
          "root.test.sg1.s5 LIKE '^test$'",
          "root.test.sg1.s2 IN (1,2)",
          "root.test.sg1.s2 BETWEEN 1 AND 3",
        };
    String[] retArray =
        new String[] {
          "1,null,null,null,null,null,null,",
          "2,null,null,null,null,null,null,",
          "3,null,null,null,null,null,null,",
        };
    resultSetEqualTest(
        "select s1=s2, s1>s2, s1<s2, s5 like \"test\" , s2 in (1,2), s2 between 1 and 3 from root.test.sg1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testIsNullOperation() {
    // Currently, if one predicate in SELECT contains not existed series, we won't show this column.
    // So we test IS_NULL in WHERE.
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.test.sg1.s1",
          "root.test.sg1.s2 IS NULL",
          "root.test.sg1.s2 IS NOT NULL",
          "root.test.sg1.s5 IS NULL",
          "root.test.sg1.s5 IS NOT NULL",
        };
    String[] retArray =
        new String[] {
          "1,1,true,false,true,false,", "2,2,true,false,true,false,", "3,3,true,false,true,false,",
        };
    resultSetEqualTest(
        "select s1,s2 is null, s2 is not null, s5 is null, s5 is not null from root.test.sg1",
        expectedHeader,
        retArray);
  }

  /**
   * LogicOperations: !, &&, ||
   *
   * <p>See
   * 'https://learn.microsoft.com/zh-cn/sql/connect/ado-net/sql/handle-null-values?view=sql-server-ver16'
   */
  @Test
  public void testLogicOperations() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.test.sg1.s3 | root.test.sg1.s3",
          "root.test.sg1.s4 & root.test.sg1.s4",
          "root.test.sg1.s3 | root.test.sg1.s4",
          "root.test.sg1.s3 & root.test.sg1.s4",
        };
    String[] retArray =
        new String[] {
          "1,true,false,true,false,", "2,true,null,true,null,", "3,null,false,null,false,",
        };
    resultSetEqualTest(
        "select s3||s3, s4&&s4, s3||s4, s3&&s4  from root.test.sg1", expectedHeader, retArray);
  }

  @Test
  public void testWhere() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.test.sg1.s1", "root.test.sg1.s3", "root.test.sg1.s4",
        };
    String[] retArray = new String[] {};
    resultSetEqualTest("select s1, s3, s4 from root.** where s2>0", expectedHeader, retArray);

    resultSetEqualTest(
        "select s1, s3, s4 from root.** where s2 is not null", expectedHeader, retArray);

    retArray =
        new String[] {
          "1,1,true,false,", "2,2,true,null,", "3,3,null,false,",
        };
    resultSetEqualTest(
        "select s1, s3, s4 from root.** where s2 is null && s5 is null", expectedHeader, retArray);

    retArray =
        new String[] {
          "1,root.test.sg1,1,true,false,",
          "2,root.test.sg1,2,true,null,",
          "3,root.test.sg1,3,null,false,",
        };
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "Device", "s1", "s3", "s4",
        };
    resultSetEqualTest(
        "select s1, s3, s4 from root.** where s2 is null && s5 is null align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testHaving() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "count(root.test.sg1.s1)", "count(root.test.sg1.s2)",
        };
    String[] retArray = new String[] {};
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>0",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "1,1,0,", "2,1,0,", "3,1,0,",
        };
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>=0",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "1,root.test.sg1,1,0,", "2,root.test.sg1,1,0,", "3,root.test.sg1,1,0,",
        };
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "Device", "count(s1)", "count(s2)",
        };
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>=0 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSeriesNotExist() {
    // series in select not exist
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "root.test.sg1.s1", "root.test.sg1.s3", "root.test.sg1.s4",
        };
    String[] retArray =
        new String[] {
          "1,1,true,false,", "2,2,true,null,", "3,3,null,false,",
        };
    resultSetEqualTest("select s1, s3, s4, notExist from root.**", expectedHeader, retArray);

    // series in where not exist

    retArray = new String[] {};
    resultSetEqualTest("select s1, s3, s4 from root.** where notExist>0", expectedHeader, retArray);

    resultSetEqualTest(
        "select s1, s3, s4 from root.** where diff(notExist)>0", expectedHeader, retArray);

    retArray =
        new String[] {
          "1,1,true,false,", "2,2,true,null,", "3,3,null,false,",
        };
    resultSetEqualTest(
        "select s1, s3, s4 from root.** where diff(notExist) is null", expectedHeader, retArray);

    // series in having not exist
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "count(root.test.sg1.s1)", "count(root.test.sg1.s2)",
        };
    retArray = new String[] {};
    resultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(notExist)>0",
        expectedHeader,
        retArray);
  }
}
