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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeleteStorageGroupIT extends AbstractSchemaIT {

  public IoTDBDeleteStorageGroupIT(SchemaTestMode schemaTestMode) {
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
  public void testDeleteStorageGroup() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln.wf01.wt01");
      statement.execute("CREATE DATABASE root.ln.wf01.wt02");
      statement.execute("CREATE DATABASE root.ln.wf01.wt03");
      statement.execute("CREATE DATABASE root.ln.wf01.wt04");
      statement.execute("DELETE DATABASE root.ln.wf01.wt01");
      ;
      String[] expected =
          new String[] {"root.ln.wf01.wt02", "root.ln.wf01.wt03", "root.ln.wf01.wt04"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  @Test
  public void testDeleteMultipleStorageGroupWithQuote() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln1.wf01.wt01");
      statement.execute("CREATE DATABASE root.ln1.wf01.wt02");
      statement.execute("CREATE DATABASE root.ln1.wf02.wt03");
      statement.execute("CREATE DATABASE root.ln1.wf02.wt04");
      statement.execute("DELETE DATABASE root.ln1.wf01.wt01, root.ln1.wf02.wt03");
      String[] expected = new String[] {"root.ln1.wf01.wt02", "root.ln1.wf02.wt04"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  @Test(expected = SQLException.class)
  public void deleteNonExistStorageGroup() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln2.wf01.wt01");
      statement.execute("DELETE DATABASE root.ln2.wf01.wt02");
    }
  }

  @Test
  public void testDeleteStorageGroupWithStar() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln3.wf01.wt01");
      statement.execute("CREATE DATABASE root.ln3.wf01.wt02");
      statement.execute("CREATE DATABASE root.ln3.wf02.wt03");
      statement.execute("CREATE DATABASE root.ln3.wf02.wt04");
      statement.execute("DELETE DATABASE root.ln3.wf02.*");
      String[] expected = new String[] {"root.ln3.wf01.wt01", "root.ln3.wf01.wt02"};
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  @Test
  public void testDeleteAllStorageGroups() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln4.wf01.wt01");
      statement.execute("CREATE DATABASE root.ln4.wf01.wt02");
      statement.execute("CREATE DATABASE root.ln4.wf02.wt03");
      statement.execute("CREATE DATABASE root.ln4.wf02.wt04");
      statement.execute("DELETE DATABASE root.**");
      List<String> result = new ArrayList<>();
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          result.add(resultSet.getString(1));
        }
      }
      assertEquals(0, result.size());
    }
  }

  @Test
  public void testDeleteStorageGroupAndThenQuery() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg1.d1(time,s1) values(1,1);");
      statement.execute("flush");
      statement.execute("select count(*) from root.**;");
      statement.execute("delete database root.sg1");
      statement.execute("insert into root.sg1.sdhkajhd(time,s1) values(1,1);");
      statement.execute("flush");
      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(*) from root.**")) {
        while (resultSet.next()) {
          count++;
          assertEquals(1, resultSet.getLong("count(root.sg1.sdhkajhd.s1)"));
        }
      }
      assertEquals(1, count);
    }
  }

  @Test
  public void testDeleteStorageGroupInvalidateCache() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("insert into root.sg1.d1(s1) values(1);");
        statement.execute("insert into root.sg2(s2) values(1);");
        statement.execute("select last(s1) from root.sg1.d1;");
        statement.execute("select last(s2) from root.sg2;");
        statement.execute("insert into root.sg1.d1(s1) values(1);");
        statement.execute("insert into root.sg2(s2) values(1);");
        statement.execute("delete database root.**");
        statement.execute("insert into root.sg1.d1(s1) values(\"2001-08-01\");");
        statement.execute("insert into root.sg2(s2) values(\"2001-08-01\");");
      } catch (final Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }
}
