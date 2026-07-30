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
package org.apache.iotdb.db.it.alignbydevice;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceWithTemplateAggregationIT {
  private static final String[] sqls =
      new String[] {
        // non-aligned template
        "CREATE database root.sg1;",
        "CREATE schema template t1 (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t1 to root.sg1;",
        "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2), (5,5.5,true,5), (1314000000000,13.14,true,1314);",
        "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22), (5,50.0,false,5), (1314000000001,13.15,false,1315);",
        "flush;",
        "INSERT INTO root.sg1.d3(timestamp,s1,s2,s3) values(1,111.1,true,null), (4,444.4,true,44), (8,8.8,false,4), (1314000000002,13.16,false,1316);",
        "INSERT INTO root.sg1.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111), (5,5555.5,false,5555), (8,0.8,true,10), (1314000000003,13.14,true,1314);",

        // aligned template
        "CREATE database root.sg2;",
        "CREATE schema template t2 aligned (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t2 to root.sg2;",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2), (5,5.5,true,5), (1314000000000,13.14,true,1314);",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22), (5,50.0,false,5), (1314000000001,13.15,false,1315);",
        "flush;",
        "INSERT INTO root.sg2.d3(timestamp,s1,s2,s3) values(1,111.1,true,null), (4,444.4,true,44), (8,8.8,false,4), (1314000000002,13.16,false,1316);",
        "INSERT INTO root.sg2.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111), (5,5555.5,false,5555), (8,0.8,true,10), (1314000000003,13.14,true,1314);",

        // aligned template with delete
        "CREATE database root.sg3;",
        "SET SCHEMA TEMPLATE t2 to root.sg3;",
        "INSERT INTO root.sg3.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2), (5,5.5,true,5), (1314000000000,13.14,true,1314);",
        "INSERT INTO root.sg3.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22), (5,50.0,false,5), (1314000000001,13.15,false,1315);",
        "flush;",
        "INSERT INTO root.sg3.d3(timestamp,s1,s2,s3) values(1,111.1,true,null), (4,444.4,true,44), (8,8.8,false,4), (1314000000002,13.16,false,1316);",
        "INSERT INTO root.sg3.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111), (5,5555.5,false,5555), (8,0.8,true,10), (1314000000003,13.14,true,1314);",
        "delete from root.sg3.d1.s1 where time <= 5;"
      };

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void aggregationTest() {
    // only descending test
    // no value filter
    String[] expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),last_value(s2)"};
    String[] retArray =
        new String[] {
          "root.sg1.d1,1314000000000,13.14,true,",
          "root.sg1.d2,1314000000001,13.15,false,",
          "root.sg1.d3,1314000000002,13.16,false,",
          "root.sg1.d4,1314000000003,13.14,true,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg1.** align by device;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "root.sg2.d1,1314000000000,13.14,true,",
          "root.sg2.d2,1314000000001,13.15,false,",
          "root.sg2.d3,1314000000002,13.16,false,",
          "root.sg2.d4,1314000000003,13.14,true,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** align by device;",
        expectedHeader,
        retArray);

    // __endTime result is ambiguous

    // not supported: group by session, condition, agg(s1+1), count(s1+s2), non-aligned
    // template
  }

  @Test
  public void filterTest() {
    String[] expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),last_value(s2)"};
    String[] retArray =
        new String[] {
          "root.sg1.d2,1314000000001,13.15,false,",
          "root.sg1.d3,1314000000002,13.16,false,",
          "root.sg1.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg1.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.sg2.d2,1314000000001,13.15,false,",
          "root.sg2.d3,1314000000002,13.16,false,",
          "root.sg2.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    // supported: ascending with descending aggregation descriptors
    expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),count(s2),first_value(s3)"};
    retArray =
        new String[] {
          "root.sg1.d2,1314000000001,13.15,4,11,",
          "root.sg1.d3,1314000000002,13.16,2,4,",
          "root.sg1.d4,5,5555.5,1,5555,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3) FROM root.sg1.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.sg2.d2,1314000000001,13.15,4,11,",
          "root.sg2.d3,1314000000002,13.16,2,4,",
          "root.sg2.d4,5,5555.5,1,5555,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3) FROM root.sg2.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    // duplicate select expressions
    expectedHeader =
        new String[] {
          "Device,max_time(s1),last_value(s1),count(s2),first_value(s3),last_value(s1)"
        };
    retArray =
        new String[] {
          "root.sg1.d2,1314000000001,13.15,4,11,13.15,",
          "root.sg1.d3,1314000000002,13.16,2,4,13.16,",
          "root.sg1.d4,5,5555.5,1,5555,5555.5,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3), last_value(s1) FROM root.sg1.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d2,1314000000001,13.15,4,11,13.15,",
          "root.sg2.d3,1314000000002,13.16,2,4,13.16,",
          "root.sg2.d4,5,5555.5,1,5555,5555.5,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3), last_value(s1) FROM root.sg2.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    // alias
    expectedHeader = new String[] {"Device,c1,first_value(s3),c2"};
    retArray =
        new String[] {
          "root.sg1.d2,4,11,4,", "root.sg1.d3,2,4,2,", "root.sg1.d4,1,5555,1,",
        };
    resultSetEqualTest(
        "SELECT count(s1) as c1, first_value(s3), count(s1) as c2 FROM root.sg1.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d2,4,11,4,", "root.sg2.d3,2,4,2,", "root.sg2.d4,1,5555,1,",
        };
    resultSetEqualTest(
        "SELECT count(s1) as c1, first_value(s3), count(s1) as c2 FROM root.sg2.** where s3+1=1316 or s2=false having avg(s1)>2 align by device;",
        expectedHeader,
        retArray);

    // arithmetic expression
    expectedHeader =
        new String[] {"Device,max_time(s1),count(s1),last_value(s2),count(s1) + last_value(s3)"};
    retArray =
        new String[] {
          "root.sg1.d2,1314000000001,4,false,1319.0,",
          "root.sg1.d3,1314000000002,2,false,1318.0,",
          "root.sg1.d4,5,1,false,5556.0,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), count(s1), last_value(s2), count(s1)+last_value(s3) FROM root.sg1.** where s3+1=1316 or s2=false having avg(s1)+sum(s3)>5 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d2,1314000000001,4,false,1319.0,",
          "root.sg2.d3,1314000000002,2,false,1318.0,",
          "root.sg2.d4,5,1,false,5556.0,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), count(s1), last_value(s2), count(s1)+last_value(s3) FROM root.sg2.** where s3+1=1316 or s2=false having avg(s1)+sum(s3)>5 align by device;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),count(s2),first_value(s3)"};
    retArray =
        new String[] {
          "root.sg1.d1,1314000000000,13.14,3,2,",
          "root.sg1.d2,1314000000001,13.15,4,11,",
          "root.sg1.d3,1314000000002,13.16,3,44,",
          "root.sg1.d4,1314000000003,13.14,4,1111,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3) FROM root.sg1.** where s3>1 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,1314000000000,13.14,3,2,",
          "root.sg2.d2,1314000000001,13.15,4,11,",
          "root.sg2.d3,1314000000002,13.16,3,44,",
          "root.sg2.d4,1314000000003,13.14,4,1111,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), count(s2), first_value(s3) FROM root.sg2.** where s3>1 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void countTimeTest() {
    // no filter
    String[] expectedHeader = new String[] {"Device,count_time(*)"};
    String[] retArray =
        new String[] {
          "root.sg1.d1,4,", "root.sg1.d2,4,", "root.sg1.d3,4,", "root.sg1.d4,4,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg1.** align by device;", expectedHeader, retArray);
    retArray =
        new String[] {
          "root.sg2.d1,4,", "root.sg2.d2,4,", "root.sg2.d3,4,", "root.sg2.d4,4,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg2.** align by device;", expectedHeader, retArray);

    // and filter
    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray =
        new String[] {
          "root.sg1.d1,2,", "root.sg1.d2,4,", "root.sg1.d3,2,", "root.sg1.d4,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg1.** where s3>0 and s2=false align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,2,", "root.sg2.d2,4,", "root.sg2.d3,2,", "root.sg2.d4,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg2.** where s3>0 and s2=false align by device;",
        expectedHeader,
        retArray);

    // or filter
    expectedHeader = new String[] {"Device,count_time(*)"};
    retArray =
        new String[] {
          "root.sg1.d1,2,", "root.sg1.d2,4,", "root.sg1.d3,2,", "root.sg1.d4,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg1.** where s3+1=1316 or s2=false align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,2,", "root.sg2.d2,4,", "root.sg2.d3,2,", "root.sg2.d4,1,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg2.** where s3+1=1316 or s2=false align by device;",
        expectedHeader,
        retArray);

    // group by
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "1,root.sg1.d1,2,",
          "6,root.sg1.d1,0,",
          "1,root.sg1.d2,3,",
          "6,root.sg1.d2,0,",
          "1,root.sg1.d3,0,",
          "6,root.sg1.d3,1,",
          "1,root.sg1.d4,1,",
          "6,root.sg1.d4,0,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg1.** where s3+1=1316 or s2=false group by ([1,10), 5ms) align by device;",
        expectedHeader,
        retArray);
    expectedHeader = new String[] {"Time,Device,count_time(*)"};
    retArray =
        new String[] {
          "1,root.sg2.d1,2,",
          "6,root.sg2.d1,0,",
          "1,root.sg2.d2,3,",
          "6,root.sg2.d2,0,",
          "1,root.sg2.d3,0,",
          "6,root.sg2.d3,1,",
          "1,root.sg2.d4,1,",
          "6,root.sg2.d4,0,",
        };
    resultSetEqualTest(
        "SELECT count_time(*) FROM root.sg2.** where s3+1=1316 or s2=false group by ([1,10), 5ms) align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByTest() {
    String[] expectedHeader =
        new String[] {"Time,Device,max_time(s1),last_value(s1),last_value(s2)"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,2,2.2,false,",
          "5,root.sg1.d1,5,5.5,true,",
          "1,root.sg1.d2,2,22.2,false,",
          "5,root.sg1.d2,5,50.0,false,",
          "1,root.sg1.d3,1,111.1,true,",
        };
    resultSetEqualTest(
        "select max_time(s1), last_value(s1), last_value(s2) from root.sg1.** group by ([1,10), 2ms) having last_value(s2) is not null limit 5 align by device;",
        expectedHeader,
        retArray);
    expectedHeader = new String[] {"Time,Device,max_time(s1),last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "1,root.sg2.d1,2,2.2,false,",
          "5,root.sg2.d1,5,5.5,true,",
          "1,root.sg2.d2,2,22.2,false,",
          "5,root.sg2.d2,5,50.0,false,",
          "1,root.sg2.d3,1,111.1,true,",
        };
    resultSetEqualTest(
        "select max_time(s1), last_value(s1), last_value(s2) from root.sg2.** group by ([1,10), 2ms) having last_value(s2) is not null limit 5 align by device;",
        expectedHeader,
        retArray);

    // sliding window
    expectedHeader = new String[] {"Time,Device,max_time(s1),last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "1,root.sg1.d1,2,2.2,false,",
          "1,root.sg1.d2,2,22.2,false,",
          "3,root.sg1.d2,5,50.0,false,",
          "5,root.sg1.d2,5,50.0,false,",
          "7,root.sg1.d3,8,8.8,false,",
          "3,root.sg1.d4,5,5555.5,false,",
          "5,root.sg1.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg1.** where s3+1=1316 or s2=false group by ([1,10),3ms,2ms) having avg(s1)>0 align by device;",
        expectedHeader,
        retArray);
    expectedHeader = new String[] {"Time,Device,max_time(s1),last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "1,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d2,2,22.2,false,",
          "3,root.sg2.d2,5,50.0,false,",
          "5,root.sg2.d2,5,50.0,false,",
          "7,root.sg2.d3,8,8.8,false,",
          "3,root.sg2.d4,5,5555.5,false,",
          "5,root.sg2.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s3+1=1316 or s2=false group by ([1,10),3ms,2ms) having avg(s1)>0 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void havingTest() {
    String[] expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),last_value(s2)"};
    String[] retArray =
        new String[] {
          "root.sg1.d1,2,2.2,false,",
          "root.sg1.d2,1314000000001,13.15,false,",
          "root.sg1.d3,1314000000002,13.16,false,",
          "root.sg1.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg1.** where s2=false having avg(s3) > 1 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,2,2.2,false,",
          "root.sg2.d2,1314000000001,13.15,false,",
          "root.sg2.d3,1314000000002,13.16,false,",
          "root.sg2.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s2=false having avg(s3) > 1 align by device;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.sg1.d1,2,2.2,false,",
          "root.sg1.d2,1314000000001,13.15,false,",
          "root.sg1.d3,1314000000002,13.16,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg1.** where s2=false having count(s3)+count(s1) > 2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,2,2.2,false,",
          "root.sg2.d2,1314000000001,13.15,false,",
          "root.sg2.d3,1314000000002,13.16,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s2=false having count(s3)+count(s1) > 2 align by device;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "root.sg2.d2,1314000000001,13.15,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s2=false having count(s3+s1) > 2 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void orderByTest() {
    String[] expectedHeader = new String[] {"Time,Device,sum(s3)"};
    String[] retArray =
        new String[] {
          "4,root.sg1.d1,5.0,",
          "4,root.sg1.d2,5.0,",
          "4,root.sg1.d3,44.0,",
          "4,root.sg1.d4,5555.0,",
          "2,root.sg1.d1,2.0,",
        };
    resultSetEqualTest(
        "select sum(s3) from root.sg1.** where s1>1 GROUP BY([0, 10), 2ms) order by time desc offset 8 limit 5 align by device;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,sum(s3)"};
    retArray =
        new String[] {
          "4,root.sg2.d1,5.0,",
          "4,root.sg2.d2,5.0,",
          "4,root.sg2.d3,44.0,",
          "4,root.sg2.d4,5555.0,",
          "2,root.sg2.d1,2.0,",
        };
    resultSetEqualTest(
        "select sum(s3) from root.sg2.** where s1>1 GROUP BY([0, 10), 2ms) order by time desc offset 8 limit 5 align by device;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,sum(s3)"};
    retArray =
        new String[] {
          "0,root.sg1.d1,1.0,",
          "2,root.sg1.d1,2.0,",
          "4,root.sg1.d1,5.0,",
          "0,root.sg1.d2,11.0,",
          "2,root.sg1.d2,22.0,",
        };
    resultSetEqualTest(
        "select sum(s3) from root.sg1.** where s1>1 GROUP BY([0, 10), 2ms) order by count(s2) desc limit 5 align by device;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,sum(s3)"};
    retArray =
        new String[] {
          "0,root.sg2.d1,1.0,",
          "2,root.sg2.d1,2.0,",
          "4,root.sg2.d1,5.0,",
          "0,root.sg2.d2,11.0,",
          "2,root.sg2.d2,22.0,",
        };
    resultSetEqualTest(
        "select sum(s3) from root.sg2.** where s1>1 GROUP BY([0, 10), 2ms) order by count(s2) desc limit 5 align by device;",
        expectedHeader,
        retArray);

    // order by non-existent measurement
    assertTestFail(
        "select sum(s3) from root.sg1.** where s1>1 order by count(s_null) desc limit 5 align by device;",
        "count(s_null) in order by clause doesn't exist.");
    assertTestFail(
        "select sum(s3) from root.sg2.** where s1>1 order by count(s_null) desc limit 5 align by device;",
        "count(s_null) in order by clause doesn't exist.");
  }

  @Test
  public void wildCardTest() {
    String[] expectedHeader =
        new String[] {
          "Device,max_time(s3),max_time(s1),max_time(s2),last_value(s1),last_value(s3),last_value(s1),last_value(s2)"
        };
    String[] retArray =
        new String[] {
          "root.sg1.d1,1314000000000,1314000000000,1314000000000,13.14,1314,13.14,true,",
          "root.sg1.d2,1314000000001,1314000000001,1314000000001,13.15,1315,13.15,false,",
          "root.sg1.d3,1314000000002,1314000000002,1314000000002,13.16,1316,13.16,false,",
          "root.sg1.d4,1314000000003,1314000000003,1314000000003,13.14,1314,13.14,true,",
        };
    resultSetEqualTest(
        "SELECT max_time(*), last_value(s1), last_value(*) FROM root.sg1.** align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,1314000000000,1314000000000,1314000000000,13.14,1314,13.14,true,",
          "root.sg2.d2,1314000000001,1314000000001,1314000000001,13.15,1315,13.15,false,",
          "root.sg2.d3,1314000000002,1314000000002,1314000000002,13.16,1316,13.16,false,",
          "root.sg2.d4,1314000000003,1314000000003,1314000000003,13.14,1314,13.14,true,",
        };
    resultSetEqualTest(
        "SELECT max_time(*), last_value(s1), last_value(*) FROM root.sg2.** align by device;",
        expectedHeader,
        retArray);

    // filter test
    expectedHeader =
        new String[] {"Device,max_time(s1),last_value(s3),last_value(s1),last_value(s2),count(s2)"};
    retArray =
        new String[] {
          "root.sg1.d1,1314000000000,1314,13.14,true,3,",
          "root.sg1.d2,1314000000001,1315,13.15,false,4,",
          "root.sg1.d3,1314000000002,1316,13.16,false,3,",
          "root.sg1.d4,1314000000003,1314,13.14,true,4,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(*), count(s2) FROM root.sg1.** where s3>1 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d1,1314000000000,1314,13.14,true,3,",
          "root.sg2.d2,1314000000001,1315,13.15,false,4,",
          "root.sg2.d3,1314000000002,1316,13.16,false,3,",
          "root.sg2.d4,1314000000003,1314,13.14,true,4,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(*), count(s2) FROM root.sg2.** where s3>1 align by device;",
        expectedHeader,
        retArray);

    // sliding window
    expectedHeader = new String[] {"Time,Device,last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "1,root.sg1.d1,2.2,false,",
          "1,root.sg1.d2,22.2,false,",
          "3,root.sg1.d2,50.0,false,",
          "5,root.sg1.d2,50.0,false,",
          "7,root.sg1.d3,8.8,false,",
          "3,root.sg1.d4,5555.5,false,",
          "5,root.sg1.d4,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT last_value(*) FROM root.sg1.** where s3+1=1316 or s2=false group by ([1,10),3ms,2ms) having avg(s1)>0 soffset 1 slimit 2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,2.2,false,",
          "1,root.sg2.d2,22.2,false,",
          "3,root.sg2.d2,50.0,false,",
          "5,root.sg2.d2,50.0,false,",
          "7,root.sg2.d3,8.8,false,",
          "3,root.sg2.d4,5555.5,false,",
          "5,root.sg2.d4,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT last_value(*) FROM root.sg2.** where s3+1=1316 or s2=false group by ([1,10),3ms,2ms) having avg(s1)>0 soffset 1 slimit 2 align by device;",
        expectedHeader,
        retArray);

    // having
    expectedHeader =
        new String[] {
          "Device,last_value(s3),last_value(s1),last_value(s2),first_value(s3),first_value(s1),first_value(s2)"
        };
    retArray =
        new String[] {
          "root.sg1.d2,1315,13.15,false,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT last_value(*), first_value(*) FROM root.sg1.** where s2=false having count(s3+s1) > 2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d2,1315,13.15,false,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT last_value(*), first_value(*) FROM root.sg2.** where s2=false having count(s3+s1) > 2 align by device;",
        expectedHeader,
        retArray);

    // not supported expression: agg1(*)+agg2(*), agg(*)/2
    expectedHeader = new String[] {"Device,count(s3) + 1,count(s1) + 1,count(s2) + 1"};
    retArray =
        new String[] {
          "root.sg1.d2,5.0,5.0,5.0,",
        };
    resultSetEqualTest(
        "SELECT count(*)+1 FROM root.sg1.** where s2=false having count(s3+s1) > 2 align by device;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "root.sg2.d2,5.0,5.0,5.0,",
        };
    resultSetEqualTest(
        "SELECT count(*)+1 FROM root.sg2.** where s2=false having count(s3+s1) > 2 align by device;",
        expectedHeader,
        retArray);
  }

  @Test
  public void orderByTimeWithDeleteTest() {
    String[] expectedHeader = new String[] {"Device,count(s1)"};
    String[] retArray =
        new String[] {
          "root.sg3.d1,1,", "root.sg3.d2,4,", "root.sg3.d3,4,", "root.sg3.d4,4,",
        };
    resultSetEqualTest(
        "select count(s1) from root.sg3.** order by device align by device;",
        expectedHeader,
        retArray);

    // to test visitSingeDeviceViewNode in AggregationPushDown
    resultSetEqualTest(
        "select count(s1) from root.sg3.** order by time align by device;",
        expectedHeader,
        retArray);
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
