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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNullOperandTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg1(device STRING ID, s1 INT32 MEASUREMENT, s2 INT32 MEASUREMENT, s3 BOOLEAN MEASUREMENT, s4 BOOLEAN MEASUREMENT, s5 TEXT MEASUREMENT)",
        "INSERT INTO sg1(time,device,s1,s3,s4) values(1, 'd1', 1, true, false)",
        "INSERT INTO sg1(time,device,s1,s3) values(2, 'd1', 2, true)",
        "INSERT INTO sg1(time,device,s1,s4) values(3, 'd1', 3, false)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLs);
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
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5",
        };
    String[] retArray =
        new String[] {
          "null,null,null,null,null,null,",
          "null,null,null,null,null,null,",
          "null,null,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select s1+s2, s1-s2, s1*s2, s1/s2, s1%s2, s2%s2 from sg1",
        expectedHeader, retArray, DATABASE_NAME);
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
          "_col0", "_col1", "_col2", "_col3", "_col4", "_col5",
        };
    String[] retArray =
        new String[] {
          "null,null,null,null,null,null,",
          "null,null,null,null,null,null,",
          "null,null,null,null,null,null,",
        };
    tableResultSetEqualTest(
        "select s1=s2, s1>s2, s1<s2, s5 like 'test' , s2 in (1,2), s2 between 1 and 3 from sg1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testIsNullOperation() {
    // Currently, if one predicate in SELECT contains not existed series, we won't show this column.
    // So we test IS_NULL in WHERE.
    String[] expectedHeader =
        new String[] {
          "s1", "_col1", "_col2", "_col3", "_col4",
        };
    String[] retArray =
        new String[] {
          "1,true,false,true,false,", "2,true,false,true,false,", "3,true,false,true,false,",
        };
    tableResultSetEqualTest(
        "select s1,s2 is null, s2 is not null, s5 is null, s5 is not null from sg1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
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
          "_col0", "_col1", "_col2", "_col3",
        };
    String[] retArray =
        new String[] {
          "true,false,true,false,", "true,null,true,null,", "null,false,null,false,",
        };
    tableResultSetEqualTest(
        "select s3 or s3, s4 and s4, s3 or s4, s3 and s4  from sg1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testWhere() {
    String[] expectedHeader =
        new String[] {
          "s1", "s3", "s4",
        };
    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "select s1, s3, s4 from sg1 where s2>0", expectedHeader, retArray, DATABASE_NAME);

    tableResultSetEqualTest(
        "select s1, s3, s4 from sg1 where s2 is not null", expectedHeader, retArray, DATABASE_NAME);

    retArray =
        new String[] {
          "1,true,false,", "2,true,null,", "3,null,false,",
        };
    tableResultSetEqualTest(
        "select s1, s3, s4 from sg1 where s2 is null and s5 is null",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1,true,false,", "2,true,null,", "3,null,false,",
        };
    expectedHeader =
        new String[] {
          "s1", "s3", "s4",
        };
    tableResultSetEqualTest(
        "select s1, s3, s4 from sg1 where s2 is null and s5 is null order by device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Ignore // TODO
  @Test
  public void testHaving() {
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR, "count(s1)", "count(s2)",
        };
    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>0",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1,1,0,", "2,1,0,", "3,1,0,",
        };
    tableResultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>=0",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          "1,root.test.sg1,1,0,", "2,root.test.sg1,1,0,", "3,root.test.sg1,1,0,",
        };
    expectedHeader =
        new String[] {
          TIMESTAMP_STR, "Device", "count(s1)", "count(s2)",
        };
    tableResultSetEqualTest(
        "select count(s1), count(s2) from root.** group by ([1,4),1ms) having count(s2)>=0 align by device",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
