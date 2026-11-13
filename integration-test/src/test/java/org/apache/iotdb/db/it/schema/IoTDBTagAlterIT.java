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
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTagAlterIT extends AbstractSchemaIT {

  public IoTDBTagAlterIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void renameTest() {
    String[] ret1 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag2\":\"v2\",\"tagNew1\":\"v1\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT,"
            + " encoding=RLE, compression=SNAPPY tags('tag1'='v1', 'tag2'='v2') "
            + "attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret1[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret1.length, count);

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME tag3 TO 'tagNew3'");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains("TimeSeries [root.turbine.d1.s1] does not have tag/attribute [tag3]."));
      }

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'tag2'");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains(
                    "TimeSeries [root.turbine.d1.s1] already has a tag/attribute named [tag2]."));
      }

      statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'tagNew1'");
      resultSet = statement.executeQuery("show timeseries root.turbine.**");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void setTest() {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"newV1\",\"tag2\":\"v2\"},{\"attr2\":\"newV2\",\"attr1\":\"v1\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 SET 'tag3'='v3'");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains("TimeSeries [root.turbine.d1.s1] does not have tag/attribute [tag3]."));
      }

      statement.execute("ALTER timeseries root.turbine.d1.s1 SET 'tag1'='newV1', 'attr2'='newV2'");
      resultSet = statement.executeQuery("show timeseries root.turbine.**");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void dropTest() {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag2\":\"v2\"},{\"attr2\":\"v2\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 DROP attr1,'tag1'");
      resultSet = statement.executeQuery("show timeseries root.turbine.**");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);

      try (ResultSet rs = statement.executeQuery("show timeseries where TAGS(tag1)='v1'")) {
        assertFalse(rs.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void addTagTest() {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag4\":\"v4\",\"tag2\":\"v2\",\"tag3\":\"v3\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3'='v3', 'tag4'='v4'");
      resultSet = statement.executeQuery("show timeseries where TAGS(tag3)='v3'");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void addAttributeTest() {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\",\"attr4\":\"v4\",\"attr3\":\"v3\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute(
          "ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3'='v3', 'attr4'='v4'");
      resultSet = statement.executeQuery("show timeseries root.turbine.**");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void upsertTest() {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag1\":\"v1\",\"tag2\":\""
          + "newV2\",\"tag3\":\"v3\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret3 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag1\":\"newV1\",\"tag2\":\""
          + "newV2\",\"tag3\":\"newV3\"},{\"attr2\":\"v2\",\"attr1\":\"newA1\",\"attr3\":\"v3\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags('tag1'='v1', 'tag2'='v2') attributes('attr1'='v1', 'attr2'='v2')";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      ResultSet resultSet = statement.executeQuery("show timeseries root.turbine.**");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute(
          "ALTER timeseries root.turbine.d1.s1 UPSERT TAGS('tag3'='v3', 'tag2'='newV2')");
      resultSet = statement.executeQuery("show timeseries where TAGS(tag3)='v3'");
      count = 0;
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
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);

      statement.execute(
          "ALTER timeseries root.turbine.d1.s1 UPSERT TAGS('tag1'='newV1', 'tag3'='newV3') "
              + "ATTRIBUTES('attr1'='newA1', 'attr3'='v3')");
      resultSet = statement.executeQuery("show timeseries where TAGS(tag3)='newV3'");
      count = 0;
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
          assertEquals(ret3[count], ans);
          count++;
        }
        assertEquals(ret3.length, count);

        resultSet = statement.executeQuery("show timeseries where TAGS(tag3)='v3'");
        assertFalse(resultSet.next());
      } finally {
        resultSet.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void tagAliasIndexSurvivesRestartAndCanBeRebuilt() throws Exception {
    String path = "root.metadata_restore.d1.s1";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.metadata_restore");
      statement.execute(
          "CREATE TIMESERIES "
              + path
              + "(oldAlias) WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSION=SNAPPY "
              + "TAGS('kind'='old', 'source'='sensor') ATTRIBUTES('owner'='team1')");
      statement.execute("INSERT INTO root.metadata_restore.d1(timestamp, s1) VALUES (1, 10)");
      statement.execute(
          "CREATE TIMESERIES root.metadata_restore.retained.s1(keepAlias) "
              + "WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSION=SNAPPY "
              + "TAGS('kind_new'='retained') ATTRIBUTES('owner'='stable')");
      statement.execute("INSERT INTO root.metadata_restore.retained(timestamp, s1) VALUES (1, 99)");
      statement.execute("ALTER TIMESERIES " + path + " RENAME 'kind' TO 'kind_new'");
      statement.execute("ALTER TIMESERIES " + path + " SET 'kind_new'='renamed'");
      statement.execute("ALTER TIMESERIES " + path + " DROP 'source'");
      statement.execute(
          "ALTER TIMESERIES "
              + path
              + " UPSERT TAGS('kind_new'='final', 'generation'='before_restart') "
              + "ATTRIBUTES('owner'='team2', 'rack'='r1')");
      statement.execute("ALTER TIMESERIES " + path + " UPSERT ALIAS=currentAlias");
      statement.execute("FLUSH");
    }

    TestUtils.restartDataNodes();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      assertTagIndexResult(statement, "kind_new", "final", true);
      assertTagIndexResult(statement, "kind", "old", false);
      assertTagIndexResult(statement, "kind_new", "old", false);
      assertTagIndexResult(statement, "kind_new", "renamed", false);
      assertTagIndexResult(statement, "source", "sensor", false);
      assertTagIndexResult(statement, "generation", "before_restart", true);
      assertRestoredSeriesMetadata(
          statement,
          path,
          "currentAlias",
          "{\"kind_new\":\"final\",\"generation\":\"before_restart\"}",
          "{\"owner\":\"team2\",\"rack\":\"r1\"}");
      assertOldAliasAbsent(statement);
      assertRetainedSeries(statement);
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT currentAlias FROM " + path.replace(".s1", "") + " WHERE time=1")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(10, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }

      statement.execute("DELETE TIMESERIES " + path);
      statement.execute(
          "CREATE TIMESERIES "
              + path
              + "(currentAlias) WITH DATATYPE=INT32, ENCODING=RLE, COMPRESSION=SNAPPY "
              + "TAGS('kind_new'='rebuilt') ATTRIBUTES('owner'='team3')");
      statement.execute("INSERT INTO root.metadata_restore.d1(timestamp, s1) VALUES (2, 20)");

      assertTagIndexResult(statement, "kind_new", "final", false);
      assertTagIndexResult(statement, "kind_new", "rebuilt", true);
      try (ResultSet resultSet =
          statement.executeQuery("SELECT currentAlias FROM root.metadata_restore.d1")) {
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertEquals(20, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
      assertRestoredSeriesMetadata(
          statement, path, "currentAlias", "{\"kind_new\":\"rebuilt\"}", "{\"owner\":\"team3\"}");
      assertOldAliasAbsent(statement);
      assertTagIndexResult(statement, "generation", "before_restart", false);
      assertRetainedSeries(statement);
    }
  }

  private static void assertRestoredSeriesMetadata(
      Statement statement, String path, String alias, String tags, String attributes)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES " + path)) {
      assertTrue(resultSet.next());
      assertEquals(path, resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      assertEquals(alias, resultSet.getString(ColumnHeaderConstant.ALIAS));
      assertEquals("root.metadata_restore", resultSet.getString(ColumnHeaderConstant.DATABASE));
      assertEquals("INT32", resultSet.getString(ColumnHeaderConstant.DATATYPE));
      assertEquals("RLE", resultSet.getString(ColumnHeaderConstant.ENCODING));
      assertEquals("SNAPPY", resultSet.getString(ColumnHeaderConstant.COMPRESSION));
      assertEquals(
          JsonParser.parseString(tags),
          JsonParser.parseString(resultSet.getString(ColumnHeaderConstant.TAGS)));
      assertEquals(
          JsonParser.parseString(attributes),
          JsonParser.parseString(resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)));
      assertFalse(resultSet.next());
    }
  }

  private static void assertOldAliasAbsent(Statement statement) throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery("SELECT oldAlias FROM root.metadata_restore.d1")) {
      assertFalse(
          "The removed alias must not resolve to the restored or rebuilt series", resultSet.next());
    }
  }

  private static void assertRetainedSeries(Statement statement) throws Exception {
    assertRestoredSeriesMetadata(
        statement,
        "root.metadata_restore.retained.s1",
        "keepAlias",
        "{\"kind_new\":\"retained\"}",
        "{\"owner\":\"stable\"}");
    assertTagIndexResult(
        statement, "kind_new", "retained", true, "root.metadata_restore.retained.s1");
    try (ResultSet resultSet =
        statement.executeQuery("SELECT keepAlias FROM root.metadata_restore.retained")) {
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong(1));
      assertEquals(99, resultSet.getInt(2));
      assertFalse(resultSet.next());
    }
  }

  private static void assertTagIndexResult(
      Statement statement, String tagName, String tagValue, boolean expectedMatch)
      throws Exception {
    assertTagIndexResult(
        statement, tagName, tagValue, expectedMatch, "root.metadata_restore.d1.s1");
  }

  private static void assertTagIndexResult(
      Statement statement,
      String tagName,
      String tagValue,
      boolean expectedMatch,
      String expectedPath)
      throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SHOW TIMESERIES root.metadata_restore.** WHERE TAGS("
                + tagName
                + ")='"
                + tagValue
                + "'")) {
      assertEquals(expectedMatch, resultSet.next());
      if (expectedMatch) {
        assertEquals(expectedPath, resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        assertFalse(resultSet.next());
      }
    }
    try (ResultSet resultSet =
        statement.executeQuery(
            "COUNT TIMESERIES root.metadata_restore.** WHERE TAGS("
                + tagName
                + ")='"
                + tagValue
                + "'")) {
      assertTrue(resultSet.next());
      assertEquals(expectedMatch ? 1 : 0, resultSet.getInt(1));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void alterDuplicateAliasTest() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine.d1.s1(a1) with datatype=FLOAT, encoding=RLE, compression=SNAPPY;");
      statement.execute("create timeseries root.turbine.d1.s2 with datatype=INT32, encoding=RLE;");
      try {
        statement.execute("alter timeseries root.turbine.d1.s2 upsert alias=s1;");
        fail();
      } catch (final Exception e) {
        assertTrue(
            e.getMessage()
                .contains("The alias is duplicated with the name or alias of other measurement"));
      }
      try {
        statement.execute("alter timeseries root.turbine.d1.s2 upsert alias=a1;");
        fail();
      } catch (final Exception e) {
        assertTrue(
            e.getMessage()
                .contains("The alias is duplicated with the name or alias of other measurement"));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
