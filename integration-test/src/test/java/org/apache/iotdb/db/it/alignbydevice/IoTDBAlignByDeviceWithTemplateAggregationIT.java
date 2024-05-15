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
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void aggregationTest() {
    // only descending test
    // no value filter
    String[] expectedHeader = new String[] {"Device,max_time(s1),last_value(s1),last_value(s2)"};
    String[] retArray =
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

    // value filter + having
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

    // value filter + having + group by time
    expectedHeader = new String[] {"Time,Device,max_time(s1),last_value(s1),last_value(s2)"};
    retArray =
        new String[] {
          "1,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d2,2,22.2,false,",
          "5,root.sg2.d2,5,50.0,false,",
          "7,root.sg2.d3,8,8.8,false,",
          "5,root.sg2.d4,5,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT max_time(s1), last_value(s1), last_value(s2) FROM root.sg2.** where s3+1=1316 or s2=false group by ([1,10),2ms) having avg(s1)>0 align by device;",
        expectedHeader,
        retArray);

    // agg operation expression

    // group by session,

    // order by

    // wildcald

    // ascending with descending

    // count_time
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
