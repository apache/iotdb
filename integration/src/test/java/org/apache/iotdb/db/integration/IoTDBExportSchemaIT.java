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

import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.tools.mlog.ExportSchema;
import org.apache.iotdb.db.tools.mlog.MLogLoader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@Category({LocalStandaloneTest.class})
public class IoTDBExportSchemaIT {
  private Connection connection;
  private File targetDir = new File("target/tmp/exportSchema");

  private Set<String> showTimeseriesResultSet = new HashSet<>();
  private Set<String> showDevicesResultSet = new HashSet<>();
  private Set<String> showTemplatesResultSet = new HashSet<>();
  private Set<String> showPathsSetSchemaTemplateT1ResultSet = new HashSet<>();
  private Set<String> showPathsUsingSchemaTemplateT1ResultSet = new HashSet<>();
  private Set<String> showPathsSetSchemaTemplateT2ResultSet = new HashSet<>();
  private Set<String> showPathsUsingSchemaTemplateT2ResultSet = new HashSet<>();

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    prepareSchema();
  }

  @After
  public void tearDown() throws Exception {
    connection.close();
    EnvironmentUtils.cleanEnv();
    FileUtils.deleteDirectory(targetDir);
  }

  private void prepareSchema() {
    try (Statement statement = connection.createStatement()) {
      // create storage group
      statement.execute("CREATE STORAGE GROUP root.ln");
      statement.execute("CREATE STORAGE GROUP root.sgcc");
      // create time series
      statement.execute(
          "create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN;");
      statement.execute("create timeseries root.ln.wf01.wt01.temperature FLOAT encoding=RLE;");
      statement.execute("create timeseries root.sgcc.wf03.wt01.status BOOLEAN encoding=PLAIN;");
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY);");
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(status BOOLEAN encoding=PLAIN compressor=SNAPPY);");
      statement.execute(
          "create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2);");
      // create template
      statement.execute(
          "create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY);");
      statement.execute(
          "create schema template t2 aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla);");
      statement.execute("set schema template t1 to root.sg1.d1;");
      statement.execute("set schema template t2 to root.sg1.d2;");
      statement.execute("set schema template t2 to root.sg1.d3;");
      statement.execute("create timeseries of schema template on root.sg1.d1;");
      statement.execute("create timeseries of schema template on root.sg1.d2;");
      // upsert
      statement.execute(
          "ALTER timeseries root.ln.wf01.wt01.temperature UPSERT ALIAS=newAlias TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4)");
      // delete timeseries
      statement.execute("delete timeseries root.ln.wf01.wt01.status;");
      // delete storage group
      statement.execute("delete storage group root.sgcc;");
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void saveExpectedResult() {
    Set<String>[] sets =
        new Set[] {
          showTimeseriesResultSet,
          showDevicesResultSet,
          showTemplatesResultSet,
          showPathsSetSchemaTemplateT1ResultSet,
          showPathsSetSchemaTemplateT2ResultSet,
          showPathsUsingSchemaTemplateT1ResultSet,
          showPathsUsingSchemaTemplateT2ResultSet
        };
    String[] queries =
        new String[] {
          "show timeseries",
          "show devices",
          "show schema templates",
          "show paths set schema template t1",
          "show paths set schema template t2",
          "show paths using schema template t1",
          "show paths using schema template t2"
        };
    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < queries.length; i++) {
        statement.execute(queries[i]);
        try (ResultSet resultSet = statement.getResultSet()) {
          int cnt = resultSet.getMetaData().getColumnCount();
          while (resultSet.next()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 1; j <= cnt; j++) {
              stringBuilder.append(resultSet.getString(j)).append(",");
            }
            sets[i].add(stringBuilder.toString());
          }
        }
      }
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void checkExpectedResult() {
    Set<String>[] sets =
        new Set[] {
          showTimeseriesResultSet,
          showDevicesResultSet,
          showTemplatesResultSet,
          showPathsSetSchemaTemplateT1ResultSet,
          showPathsSetSchemaTemplateT2ResultSet,
          showPathsUsingSchemaTemplateT1ResultSet,
          showPathsUsingSchemaTemplateT2ResultSet
        };
    String[] queries =
        new String[] {
          "show timeseries",
          "show devices",
          "show schema templates",
          "show paths set schema template t1",
          "show paths set schema template t2",
          "show paths using schema template t1",
          "show paths using schema template t2"
        };
    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < queries.length; i++) {
        statement.execute(queries[i]);
        try (ResultSet resultSet = statement.getResultSet()) {
          int cnt = resultSet.getMetaData().getColumnCount();
          while (resultSet.next()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 1; j <= cnt; j++) {
              stringBuilder.append(resultSet.getString(j)).append(",");
            }
            Assert.assertTrue(sets[i].contains(stringBuilder.toString()));
          }
        }
      }
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void cleanAndRestart() throws Exception {
    connection.close();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
  }

  @Test
  public void testExportSchemaFail() throws Exception {
    File file = new File(targetDir, MetadataConstant.METADATA_LOG);
    if (!targetDir.exists()) {
      targetDir.mkdirs();
    }
    file.createNewFile();

    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format("export schema '%s'", targetDir.getAbsolutePath()));
      Assert.fail("Expect failure but success");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("already exist"));
    }
  }

  @Test
  public void testExportSchemaAndLoadToEmptyIoTDB() throws Exception {
    saveExpectedResult();

    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format("export schema '%s'", targetDir.getAbsolutePath()));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }

    File[] files = targetDir.listFiles();
    Assert.assertNotEquals(0, files.length);
    cleanAndRestart();
    // load mlog
    MLogLoader.main(
        new String[] {
          "-mlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.METADATA_LOG,
          "-tlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.TAG_LOG
        });
    checkExpectedResult();
  }

  @Test
  public void testExportSchemaAndLoadToNonEmptyIoTDB() throws Exception {
    saveExpectedResult();

    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format("export schema '%s'", targetDir.getAbsolutePath()));
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }

    File[] files = targetDir.listFiles();
    Assert.assertNotEquals(0, files.length);
    cleanAndRestart();
    // create some timeseries
    try (Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.sgcc.wf03.wt01.status BOOLEAN encoding=PLAIN;");
      statement.execute("create timeseries root.ln.d1.s1 FLOAT encoding=RLE;");
      statement.execute("create timeseries root.sg1.d1.s1 FLOAT encoding=RLE;");
      statement.execute("show timeseries");
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int j = 1; j <= cnt; j++) {
            stringBuilder.append(resultSet.getString(j)).append(",");
          }
          showTimeseriesResultSet.add(stringBuilder.toString());
        }
      }
      statement.execute("show devices");
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int j = 1; j <= cnt; j++) {
            stringBuilder.append(resultSet.getString(j)).append(",");
          }
          showDevicesResultSet.add(stringBuilder.toString());
        }
      }
    } catch (SQLException e) {
      Assert.fail(e.getMessage());
    }
    // load mlog
    MLogLoader.main(
        new String[] {
          "-mlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.METADATA_LOG,
          "-tlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.TAG_LOG
        });
    checkExpectedResult();
  }

  @Test
  public void testExportSchemaAndLoadWithScript() throws Exception {
    saveExpectedResult();

    String[] args = new String[] {"-o", targetDir.getAbsolutePath()};
    ExportSchema.main(args);

    File[] files = targetDir.listFiles();
    Assert.assertNotEquals(0, files.length);
    cleanAndRestart();
    // load mlog
    MLogLoader.main(
        new String[] {
          "-mlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.METADATA_LOG,
          "-tlog",
          targetDir.getAbsolutePath() + "/" + MetadataConstant.TAG_LOG
        });
    checkExpectedResult();
  }
}
