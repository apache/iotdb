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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTagLimitIT extends AbstractSchemaIT {

  public IoTDBTagLimitIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  protected static final String[] SQLs =
      new String[] {
        "create database root.turbine",
      };

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTagAttributeTotalSize(50);
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Test
  public void testBasicFunctionInMultiTagAttributeTotalSize() throws Exception {
    List<List<String>> rets = new java.util.ArrayList<>();
    List<String> ret1 =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"oldTag1\":\"oldV1\",\"tag2\":\"v2\"},{\"att2\":\"a2\",\"att1\":\"a1\"}");
    rets.add(ret1);
    List<String> ret2 =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"oldV1\",\"tag2\":\"v2\"},{\"att2\":\"a2\",\"att1\":\"a1\"}");
    rets.add(ret2);
    List<String> ret3 =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"att2\":\"a2\",\"att1\":\"a1\"}");
    rets.add(ret3);
    List<String> ret4 =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"v1\",\"tag4\":\"v4\",\"tag5\":\"v5\",\"tag2\":\"v2\",\"tag3\":\"v3\"},{\"att2\":\"a2\",\"att1\":\"a1\"}");
    rets.add(ret4);
    List<String> ret5 =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag4\":\"v4\",\"tag1\":\"v1\",\"tag5\":\"v5\"},{\"att2\":\"a2\",\"att1\":\"a1\"}");
    rets.add(ret5);

    List<String> sqls = new java.util.ArrayList<>();
    String sql1 =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('oldTag1'='oldV1', 'tag2'='v2') "
            + "attributes('att1'='a1', 'att2'='a2')";
    sqls.add(sql1);
    String sql2 = "ALTER timeseries root.turbine.d1.s1 RENAME oldTag1 TO tag1";
    sqls.add(sql2);
    String sql3 = "ALTER timeseries root.turbine.d1.s1 SET tag1=v1";
    sqls.add(sql3);
    String sql4 = "ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4, tag5=v5";
    sqls.add(sql4);
    String sql5 = "ALTER timeseries root.turbine.d1.s1 DROP tag2, tag3";
    sqls.add(sql5);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < sqls.size(); i++) {
        statement.execute(sqls.get(i));
        ResultSet resultSet = statement.executeQuery("show timeseries");
        int count = 0;
        try {
          while (resultSet.next()) {
            String ans =
                resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.ALIAS)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.DATABASE)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.ENCODING)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.COMPRESSION)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.TAGS)
                    + ","
                    + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES);
            assertTrue(rets.get(i).contains(ans));
            count++;
          }
        } finally {
          resultSet.close();
        }
        assertEquals(rets.get(i).size(), count);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    clearSchema();
  }
}
