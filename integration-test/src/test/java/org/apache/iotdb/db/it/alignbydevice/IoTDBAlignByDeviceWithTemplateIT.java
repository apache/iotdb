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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

public class IoTDBAlignByDeviceWithTemplateIT {
  private static final String[] sqls =
      new String[] {
        // non-aligned template
        "CREATE database root.sg1;",
        "CREATE schema template t1 (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t1 to root.sg1;",
        "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2);",
        "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22);",
        "INSERT INTO root.sg1.d3(timestamp,s1,s2,s3) values(1,111.1,true,null);",
        "INSERT INTO root.sg1.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111);",

        // aligned template
        "CREATE database root.sg2;",
        "CREATE schema template t2 aligned (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t2 to root.sg2;",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2);",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22);",
        "INSERT INTO root.sg2.d3(timestamp,s1,s2,s3) values(1,111.1,true,null);",
        "INSERT INTO root.sg2.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111);",
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

  @Test
  public void selectWildcardWithoutFilterTest() {
    // 1. original
    String[] expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,false,",
          "2,root.sg1.d1,2,2.2,false,",
          "1,root.sg1.d2,11,11.1,false,",
          "2,root.sg1.d2,22,22.2,false,",
          "1,root.sg1.d3,null,111.1,true,",
          "1,root.sg1.d4,1111,1111.1,true,",
        };
    resultSetEqualTest("SELECT * FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,false,",
          "2,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d2,11,11.1,false,",
          "2,root.sg2.d2,22,22.2,false,",
          "1,root.sg2.d3,null,111.1,true,",
          "1,root.sg2.d4,1111,1111.1,true,",
        };
    resultSetEqualTest("SELECT * FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);
  }
}
