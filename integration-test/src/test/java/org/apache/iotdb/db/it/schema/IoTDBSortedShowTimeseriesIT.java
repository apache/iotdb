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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSortedShowTimeseriesIT extends AbstractSchemaIT {

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE root.turbine",
        "CREATE DATABASE root.ln",
        "create timeseries root.turbine.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='f', 'description'='turbine this is a test1') "
            + "attributes('H_Alarm'='100', 'M_Alarm'='50')",
        "create timeseries root.turbine.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='kw', 'description'='turbine this is a test2') "
            + "attributes('H_Alarm'='99.9', 'M_Alarm'='44.4')",
        "create timeseries root.turbine.d0.s2(cpu) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='cores', 'description'='turbine this is a cpu') "
            + "attributes('H_Alarm'='99.9', 'M_Alarm'='44.4')",
        "create timeseries root.turbine.d0.s3(gpu0) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='cores', 'description'='turbine this is a gpu') "
            + "attributes('H_Alarm'='99.9', 'M_Alarm'='44.4')",
        "create timeseries root.turbine.d0.s4(tpu0) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='cores', 'description'='turbine this is a tpu') "
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
            + "tags('unit'='c', 'description'='ln this is a test1') "
            + "attributes('H_Alarm'='1000', 'M_Alarm'='500')",
        "create timeseries root.ln.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('unit'='w', 'description'='ln this is a test2') "
            + "attributes('H_Alarm'='9.9', 'M_Alarm'='4.4')",
        "create timeseries root.ln.d1.s0(status) with datatype=INT32, encoding=RLE "
            + "tags('description'='ln this is a test3') "
            + "attributes('H_Alarm'='90', 'M_Alarm'='50')",
        "insert into root.turbine.d0(timestamp,s0) values(1, 1)",
        "insert into root.turbine.d0(timestamp,s1) values(2, 2)",
        "insert into root.turbine.d0(timestamp,s2) values(3, 3)",
        "insert into root.turbine.d0(timestamp,s3) values(4, 4)",
        "insert into root.turbine.d0(timestamp,s4) values(5, 5)",
        "insert into root.turbine.d1(timestamp,s0) values(1, 11)",
        "insert into root.turbine.d2(timestamp,s0,s1,s3) values(6,6,6,6)"
      };

  public IoTDBSortedShowTimeseriesIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    SchemaTestMode schemaTestMode = setUpEnvironment();
    if (schemaTestMode.equals(SchemaTestMode.PBTree)) {
      allocateMemoryForSchemaRegion(10000);
    }
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    createSchema();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void showTimeseriesOrderByHeatTest1() {

    List<String> retArray1 =
        Arrays.asList(
            "root.turbine.d0.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\""
                + "turbine this is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"100\",\"M_Alarm\":\"50\"}",
            "root.turbine.d0.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine "
                + "this is a test2\",\"unit\":\"kw\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d1.s0,status,root.turbine,INT32,RLE,LZ4,{\"description\":\"turbine this "
                + "is a test3\"},{\"H_Alarm\":\"9\",\"M_Alarm\":\"5\"}",
            "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine"
                + " d2 this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
            "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this"
                + " is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
            "root.turbine.d2.s3,status,root.turbine,INT32,RLE,LZ4,{\"description\":\"turbine d2"
                + " this is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
            "root.ln.d0.s0,temperature,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a "
                + "test1\",\"unit\":\"c\"},{\"H_Alarm\":\"1000\",\"M_Alarm\":\"500\"}",
            "root.ln.d0.s1,power,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test2\",\""
                + "unit\":\"w\"},{\"H_Alarm\":\"9.9\",\"M_Alarm\":\"4.4\"}",
            "root.ln.d1.s0,status,root.ln,INT32,RLE,LZ4,{\"description\":\"ln this is a test3\"},"
                + "{\"H_Alarm\":\"90\",\"M_Alarm\":\"50\"}");

    List<String> retArray2 =
        Arrays.asList(
            "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 "
                + "this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
            "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this "
                + "is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
            "root.turbine.d2.s3,status,root.turbine,INT32,RLE,LZ4,{\"description\":\"turbine d2 this "
                + "is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
            "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
                + " tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
                + " gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                + "cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                + "test2\",\"unit\":\"kw\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine"
                + " this is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"100\",\"M_Alarm\":\"50\"}",
            "root.turbine.d1.s0,status,root.turbine,INT32,RLE,LZ4,{\"description\":\"turbine this is a "
                + "test3\"},{\"H_Alarm\":\"9\",\"M_Alarm\":\"5\"}",
            "root.ln.d0.s0,temperature,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test1\""
                + ",\"unit\":\"c\"},{\"H_Alarm\":\"1000\",\"M_Alarm\":\"500\"}",
            "root.ln.d0.s1,power,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test2\",\""
                + "unit\":\"w\"},{\"H_Alarm\":\"9.9\",\"M_Alarm\":\"4.4\"}",
            "root.ln.d1.s0,status,root.ln,INT32,RLE,LZ4,{\"description\":\"ln this is a test3\"},"
                + "{\"H_Alarm\":\"90\",\"M_Alarm\":\"50\"}");

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("show timeseries");
      int count = 0;
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

        assertTrue(retArray1.contains(ans));
        count++;
      }
      assertEquals(retArray1.size(), count);
      resultSet.close();

      resultSet = statement.executeQuery("show LATEST timeseries");
      count = 0;
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
        System.out.println("\"" + ans + "\",");
        assertTrue(retArray2.contains(ans));
        count++;
      }
      assertEquals(retArray2.size(), count);
      resultSet.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showTimeseriesOrderByHeatWithLimitTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2"
                    + " this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
                "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this "
                    + "is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
                "root.turbine.d2.s3,status,root.turbine,INT32,RLE,LZ4,{\"description\":\"turbine d2 this "
                    + "is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
                "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                    + "tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
                "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                    + "gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("show LATEST timeseries limit 5");
      int count = 0;
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

        System.out.println(ans);
        assertTrue(retSet.contains(ans));
        count++;
      }
      assertEquals(retSet.size(), count);
      resultSet.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showTimeseriesOrderByHeatWithWhereTest() {

    String[] retArray =
        new String[] {
          "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
              + " tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
          "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
              + "gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
          "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
              + " cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet =
          statement.executeQuery("show LATEST timeseries where TAGS(unit)='cores'");
      int count = 0;
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

        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      resultSet.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void createSchema() {
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
