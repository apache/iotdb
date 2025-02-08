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
      resultSet = statement.executeQuery("show timeseries");
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
      resultSet = statement.executeQuery("show timeseries");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 DROP attr1,'tag1'");
      resultSet = statement.executeQuery("show timeseries");
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
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute(
          "ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3'='v3', 'attr4'='v4'");
      resultSet = statement.executeQuery("show timeseries");
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
