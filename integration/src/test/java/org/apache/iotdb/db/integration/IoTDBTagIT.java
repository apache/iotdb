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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IoTDBTagIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
  }

  @Test
  public void deleteTest() {
    List<String> ret1 =
        Arrays.asList(
            "root.turbine.d7.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"t1\",\"tag2\":\"t2\"},{\"attr2\":\"a2\",\"attr1\":\"a1\"}",
            "root.turbine.d7.s2,status,root.turbine,INT32,RLE,SNAPPY,{\"tag2\""
                + ":\"t2\",\"tag3\":\"t3\"},{\"attr4\":\"a4\",\"attr3\":\"a3\"}");
    List<String> ret2 =
        Collections.singletonList(
            "root.turbine.d7.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"t1\",\"tag2\":\"t2\"},{\"attr2\":\"a2\",\"attr1\":\"a1\"}");

    String sql1 =
        "create timeseries root.turbine.d7.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='t1', 'tag2'='t2') "
            + "attributes('attr1'='a1', 'attr2'='a2')";
    String sql2 =
        "create timeseries root.turbine.d7.s2(status) with datatype=INT32, encoding=RLE "
            + "tags('tag2'='t2', 'tag3'='t3') "
            + "attributes('attr3'='a3', 'attr4'='a4')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql1);
      statement.execute(sql2);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      int count = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ALIAS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ENCODING)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_COMPRESSION)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_ATTRIBUTES);

          assertTrue(ret1.contains(ans));
          count++;
        }
      }
      assertEquals(ret1.size(), count);

      statement.execute("delete timeseries root.turbine.d7.s2");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      count = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ALIAS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ENCODING)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_COMPRESSION)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_ATTRIBUTES);

          assertTrue(ret2.contains(ans));
          count++;
        }
      }
      assertEquals(ret2.size(), count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void deleteWithAliasTest() {
    List<String> ret1 =
        Arrays.asList(
            "root.turbine.d7.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"t1\",\"tag2\":\"t2\"},{\"attr2\":\"a2\",\"attr1\":\"a1\"}",
            "root.turbine.d7.s2,status,root.turbine,INT32,RLE,SNAPPY,"
                + "{\"tag2\":\"t2\",\"tag3\":\"t3\"},{\"attr4\":\"a4\",\"attr3\":\"a3\"}");
    List<String> ret2 =
        Collections.singletonList(
            "root.turbine.d7.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"t1\",\"tag2\":\"t2\"},{\"attr2\":\"a2\",\"attr1\":\"a1\"}");

    String sql1 =
        "create timeseries root.turbine.d7.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='t1', 'tag2'='t2') "
            + "attributes('attr1'='a1', 'attr2'='a2')";
    String sql2 =
        "create timeseries root.turbine.d7.s2(status) with datatype=INT32, encoding=RLE "
            + "tags('tag2'='t2', 'tag3'='t3') "
            + "attributes('attr3'='a3', 'attr4'='a4')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql1);
      statement.execute(sql2);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      int count = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ALIAS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ENCODING)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_COMPRESSION)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_ATTRIBUTES);

          assertTrue(ret1.contains(ans));
          count++;
        }
      }
      assertEquals(ret1.size(), count);

      statement.execute("delete timeseries root.turbine.d7.status");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      count = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ALIAS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ENCODING)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_COMPRESSION)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_ATTRIBUTES);

          assertTrue(ret2.contains(ans));
          count++;
        }
      }
      assertEquals(ret2.size(), count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void queryWithWhereAndDeleteTest() {
    Set<String> ret = new HashSet<>();
    ret.add(
        "root.turbine.d0.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\""
            + "turbine this is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"100\",\"M_Alarm\":\"50\"}");
    ret.add(
        "root.ln.d0.s0,temperature,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this "
            + "is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"1000\",\"M_Alarm\":\"500\"}");

    String[] sqls = {
      "create timeseries root.turbine.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='f', 'description'='turbine this is a test1') "
          + "attributes('H_Alarm'='100', 'M_Alarm'='50')",
      "create timeseries root.turbine.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='kw', 'description'='turbine this is a test2') "
          + "attributes('H_Alarm'='99.9', 'M_Alarm'='44.4')",
      "create timeseries root.turbine.d1.s0(status) with datatype=INT32, encoding=RLE "
          + "tags('description'='turbine this is a test3') "
          + "attributes('H_Alarm'='9', 'M_Alarm'='5')",
      "create timeseries root.turbine.d2.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='f', 'description'='turbine d2 this is a test1') "
          + "attributes('MaxValue'='100', 'MinValue'='1')",
      "create timeseries root.turbine.d2.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='kw', 'description'='turbine d2 this is a test2') "
          + "attributes('MaxValue'='99.9', 'MinValue'='44.4')",
      "create timeseries root.turbine.d2.s3(status) with datatype=INT32, encoding=RLE "
          + "tags('description'='turbine d2 this is a test3') "
          + "attributes('MaxValue'='9', 'MinValue'='5')",
      "create timeseries root.ln.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='f', 'description'='ln this is a test1') "
          + "attributes('H_Alarm'='1000', 'M_Alarm'='500')",
      "create timeseries root.ln.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
          + "tags('unit'='w', 'description'='ln this is a test2') "
          + "attributes('H_Alarm'='9.9', 'M_Alarm'='4.4')",
      "create timeseries root.ln.d1.s0(status) with datatype=INT32, encoding=RLE "
          + "tags('description'='ln this is a test3') "
          + "attributes('H_Alarm'='90', 'M_Alarm'='50')",
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }

      statement.execute("delete timeseries root.turbine.d2.s0");

      // with *
      boolean hasResult = statement.execute("show timeseries where 'unit'='f'");
      assertTrue(hasResult);
      int count = 0;
      Set<String> res = new HashSet<>();
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ALIAS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_STORAGE_GROUP)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_ENCODING)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TIMESERIES_COMPRESSION)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.COLUMN_ATTRIBUTES);

          res.add(ans);
          count++;
        }
      }
      assertEquals(ret, res);
      assertEquals(ret.size(), count);

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
