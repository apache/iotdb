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

package org.apache.iotdb.db.it.fill;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFillWithNewDataTypeIT {

  private static String[] creationSqls =
      new String[] {
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
        "insert into root.db.d1(time,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1,true,1.1,'hello1',1,1,1.9,1997-01-01T08:00:00.001+08:00,'Hong Kong',X'486f6e67204b6f6e6720426c6f6221','1997-07-01')",
        "insert into root.db.d1(time,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(3,false,1.3,'hello3',3,3,2.1,1997-01-03T08:00:00.001+08:00,'Hong Kong-3',X'486f6e67204b6f6e6720426c6f6224','1997-07-03')",
        "insert into root.db.d1(time,s2) values(2,1.2)"
      };

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testFill() {

    String[] resultSet =
        new String[] {
          "1,1.1,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "2,1.2,2,2,null,null,",
          "3,1.3,852249600001,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    String expectedQueryHeader =
        "Time,root.db.d1.s2,root.db.d1.s7,root.db.d1.s8,root.db.d1.s9,root.db.d1.s10,";
    resultSetEqualTest(
        "select s2,s7,s8,s9,s10 from root.db.d1 fill(2);", expectedQueryHeader, resultSet);

    resultSet =
        new String[] {
          "1,1.1,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "2,1.2,null,1997-07-02,null,1997-07-02,",
          "3,1.3,852249600001,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    resultSetEqualTest(
        "select s2,s7,s8,s9,s10 from root.db.d1 fill('1997-07-02');",
        expectedQueryHeader,
        resultSet);

    resultSet =
        new String[] {
          "1,1.1,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "2,1.2,null,Hong Kong-2,null,null,",
          "3,1.3,852249600001,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    resultSetEqualTest(
        "select s2,s7,s8,s9,s10 from root.db.d1 fill('Hong Kong-2');",
        expectedQueryHeader,
        resultSet);

    resultSet =
        new String[] {
          "1,1.1,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "2,1.2,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "3,1.3,852249600001,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    resultSetEqualTest(
        "select s2,s7,s8,s9,s10 from root.db.d1 fill(previous);", expectedQueryHeader, resultSet);

    resultSet =
        new String[] {
          "1,1.1,852076800001,Hong Kong,0x486f6e67204b6f6e6720426c6f6221,1997-07-01,",
          "2,1.2,852163200001,null,null,1997-07-02,",
          "3,1.3,852249600001,Hong Kong-3,0x486f6e67204b6f6e6720426c6f6224,1997-07-03,",
        };

    resultSetEqualTest(
        "select s2,s7,s8,s9,s10 from root.db.d1 fill(linear);", expectedQueryHeader, resultSet);
  }
}
