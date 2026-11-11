/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.commons.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.db.queryengine.plan.relational.function.DataNodeTableBuiltinTableFunction;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_AGG_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSQLFunctionManagementIT {

  private static final String GENERATED_UDF_PACKAGE = "org.apache.iotdb.test.generated.crossmodel";

  private static final int BUILTIN_SCALAR_FUNCTIONS_COUNT =
      TableBuiltinScalarFunction.getBuiltInScalarFunctionName().size();
  private static final int BUILTIN_AGGREGATE_FUNCTIONS_COUNT =
      TableBuiltinAggregationFunction.values().length;
  private static final int BUILTIN_TABLE_FUNCTIONS_COUNT =
      TableBuiltinTableFunction.values().length + DataNodeTableBuiltinTableFunction.values().length;
  private static final int BUILTIN_FUNCTIONS_COUNT =
      BUILTIN_SCALAR_FUNCTIONS_COUNT
          + BUILTIN_AGGREGATE_FUNCTIONS_COUNT
          + BUILTIN_TABLE_FUNCTIONS_COUNT;

  private static final String UDF_LIB_PREFIX =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  private static final String UDF_JAR_PREFIX = new File(UDF_LIB_PREFIX).toURI().toString();

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void dropAll() {
    SQLFunctionUtils.dropAllUDF();
  }

  @Test
  public void testCreateShowDropScalarFunction() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC)) {
            Assert.assertEquals(
                String.format(
                    "UDSF,%s,org.apache.iotdb.db.query.udf.example.relational.ContainNull,AVAILABLE,",
                    FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC),
                result);
          }
          ++count;
        }
        Assert.assertEquals(1 + BUILTIN_FUNCTIONS_COUNT, count);
      }
      statement.execute("drop function udsf");
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC)) {
            Assert.fail();
          }
          ++count;
        }
        Assert.assertEquals(BUILTIN_FUNCTIONS_COUNT, count);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testInformationSchemaFunctionsShowsExternalScalarType() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      try (ResultSet resultSet =
          statement.executeQuery(
              "select function_type from information_schema.functions where function_table = 'UDSF'")) {
        assertTrue(resultSet.next());
        assertEquals(FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC, resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateShowDropAggregateFunction() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      SQLFunctionUtils.createUDF(
          "udaf", "org.apache.iotdb.db.query.udf.example.relational.MyCount");

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_USER_DEFINED_AGG_FUNC)) {
            Assert.assertEquals(
                String.format(
                    "UDAF,%s,org.apache.iotdb.db.query.udf.example.relational.MyCount,AVAILABLE,",
                    FUNCTION_TYPE_USER_DEFINED_AGG_FUNC),
                result);
          }
          ++count;
        }
        Assert.assertEquals(1 + BUILTIN_FUNCTIONS_COUNT, count);
      }
      statement.execute("drop function udaf");
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        assertEquals(4, resultSet.getMetaData().getColumnCount());
        int count = 0;
        while (resultSet.next()) {
          StringBuilder stringBuilder = new StringBuilder();
          for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
            stringBuilder.append(resultSet.getString(i)).append(",");
          }
          String result = stringBuilder.toString();
          if (result.contains(FUNCTION_TYPE_USER_DEFINED_AGG_FUNC)) {
            Assert.fail();
          }
          ++count;
        }
        Assert.assertEquals(BUILTIN_FUNCTIONS_COUNT, count);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateFunctionWithBuiltinFunctionName() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create function COS as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains("the given function name conflicts with the built-in function name"));
      }
      try {
        statement.execute(
            "create function aVg as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable
                .getMessage()
                .contains("the given function name conflicts with the built-in function name"));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCreateFunctionTwice() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      try {
        statement.execute(
            "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("the same name UDF has been created"));
      }
    }
  }

  @Test
  public void testCreateInvalidFunction() throws SQLException { // create function twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull123'");
        fail();
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("invalid"));
      }
    }
  }

  @Test
  public void testCreateFunctionWithURI() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));
      statement.execute(
          String.format(
              "create function udsf2 as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
              UDF_JAR_PREFIX + "udf-example.jar"));

      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        int count = 0;
        while (resultSet.next()) {
          ++count;
        }
        Assert.assertEquals(2 + BUILTIN_FUNCTIONS_COUNT, count);
        assertEquals(4, resultSet.getMetaData().getColumnCount());
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void testCreateFunctionWithInvalidURI() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            String.format(
                "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
                ""));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("701: Untrusted uri "));
      }

      try {
        statement.execute(
            String.format(
                "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull' using URI '%s'",
                "file:///data/udf/upload-test.jar"));
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("URI"));
      }
    } catch (SQLException throwable) {
      fail();
    }
  }

  @Test
  public void testDropFunctionTwice() throws SQLException { // create + drop twice
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");
      statement.execute("drop function udsf");

      try {
        // drop UDF that does not exist will not throw exception now.
        statement.execute("drop function udsf");
      } catch (SQLException throwable) {
        assertTrue(throwable.getMessage().contains("this UDF has not been created"));
      }
    }
  }

  @Test
  public void testDropNotExistFunction() { // drop
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      // drop UDF that does not exist will not throw exception now.
      statement.execute("drop function udsf");
    } catch (SQLException throwable) {
      assertTrue(throwable.getMessage().contains("this UDF has not been created"));
    }
  }

  @Test
  public void testDropBuiltInFunction() throws SQLException { // drop
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("drop function abs");
        fail();
      } catch (SQLException throwable) {
        assertTrue(
            throwable.getMessage().contains("Built-in function ABS can not be deregistered"));
      }
      // ensure that abs is not dropped
      statement.execute("CREATE DATABASE db");
      statement.execute("USE db");
      statement.execute("CREATE TABLE table0 (device string TAG, s1 INT32)");
      statement.execute("INSERT INTO table0 (time, device, s1) VALUES (1, 'd1', -10)");
      try (ResultSet rs = statement.executeQuery("SELECT time, ABS(s1) FROM table0")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong(1));
        Assert.assertEquals(10, rs.getInt(2));
        Assert.assertFalse(rs.next());
      } finally {
        statement.execute("DROP DATABASE db");
      }
    }
  }

  @Test
  public void testTableUdfLazyDependencySurvivesTreeModelRegistration() throws Exception {
    Path jarPath = buildCrossModelUDFJar();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database udf_cross_model");
      try {
        statement.execute("use udf_cross_model");
        statement.execute("create table t(device_id STRING TAG, v INT32 FIELD)");
        statement.execute("insert into t(time, device_id, v) values(1, 'd1', 1), (2, 'd1', 2)");
        statement.execute(
            String.format(
                "create function lazy_f as '%s.LazyScalarFunction' using URI '%s'",
                GENERATED_UDF_PACKAGE, jarPath.toUri()));
        statement.execute(
            String.format(
                "create function tree_f as '%s.TreeIdentity' using URI '%s'",
                GENERATED_UDF_PACKAGE, jarPath.toUri()));

        try (ResultSet resultSet = statement.executeQuery("select lazy_f(v) from t")) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
      } finally {
        statement.execute("drop database udf_cross_model");
      }
    }
  }

  @Test
  public void testUdfRejectedInUpdateAndDeleteDevice() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database udf_write_guard");
      try {
        statement.execute("use udf_write_guard");
        statement.execute(
            "create table t(device_id STRING TAG, attr1 STRING ATTRIBUTE, v INT64 FIELD)");
        statement.execute(
            "insert into t(time, device_id, attr1, v) values(1, 'd1', 'a', 1), (2, 'd2', 'b', 2)");
        statement.execute(
            "create function write_guard_udf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'");

        assertUdfRejected(
            statement, "update t set attr1 = write_guard_udf(device_id) where device_id = 'd1'");
        assertUdfRejected(
            statement, "update t set attr1 = 'x' where write_guard_udf(device_id) = true");

        // DELETE DEVICE currently rejects function expressions before the UDF-specific check.
        // Keep this assertion message-agnostic so future changes cannot allow UDFs here.
        assertRejected(statement, "delete devices from t where write_guard_udf(device_id)");

        try (ResultSet resultSet =
            statement.executeQuery(
                "select write_guard_udf(device_id) from t where device_id = 'd1'")) {
          assertTrue(resultSet.next());
        }

        statement.execute("update t set attr1 = upper(device_id) where device_id = 'd2'");
        try (ResultSet resultSet =
            statement.executeQuery("select attr1 from t where device_id = 'd2'")) {
          assertTrue(resultSet.next());
          assertEquals("D2", resultSet.getString(1));
        }
      } finally {
        statement.execute("drop function write_guard_udf");
        statement.execute("drop database udf_write_guard");
      }
    }
  }

  private Path buildCrossModelUDFJar() throws Exception {
    Path sourceRoot = temporaryFolder.newFolder("cross-model-source").toPath();
    Path sourceDir = sourceRoot.resolve(GENERATED_UDF_PACKAGE.replace('.', File.separatorChar));
    Files.createDirectories(sourceDir);

    Files.writeString(
        sourceDir.resolve("LazyScalarFunction.java"),
        "package "
            + GENERATED_UDF_PACKAGE
            + ";\n"
            + "import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;\n"
            + "import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;\n"
            + "import org.apache.iotdb.udf.api.relational.ScalarFunction;\n"
            + "import org.apache.iotdb.udf.api.relational.access.Record;\n"
            + "import org.apache.iotdb.udf.api.type.Type;\n"
            + "public class LazyScalarFunction implements ScalarFunction {\n"
            + "  public ScalarFunctionAnalysis analyze(FunctionArguments arguments) {\n"
            + "    try {\n"
            + "      Class.forName(\""
            + GENERATED_UDF_PACKAGE
            + ".LazyHelper\", true, LazyScalarFunction.class.getClassLoader());\n"
            + "    } catch (ClassNotFoundException e) {\n"
            + "      throw new RuntimeException(e);\n"
            + "    }\n"
            + "    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.INT32).build();\n"
            + "  }\n"
            + "  public Object evaluate(Record input) { return null; }\n"
            + "}\n",
        StandardCharsets.UTF_8);
    Files.writeString(
        sourceDir.resolve("LazyHelper.java"),
        "package " + GENERATED_UDF_PACKAGE + ";\npublic class LazyHelper {}\n",
        StandardCharsets.UTF_8);
    Files.writeString(
        sourceDir.resolve("TreeIdentity.java"),
        "package "
            + GENERATED_UDF_PACKAGE
            + ";\n"
            + "import java.io.IOException;\n"
            + "import org.apache.iotdb.udf.api.UDTF;\n"
            + "import org.apache.iotdb.udf.api.access.Row;\n"
            + "import org.apache.iotdb.udf.api.collector.PointCollector;\n"
            + "import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;\n"
            + "import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;\n"
            + "import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;\n"
            + "import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;\n"
            + "import org.apache.iotdb.udf.api.type.Type;\n"
            + "public class TreeIdentity implements UDTF {\n"
            + "  public void validate(UDFParameterValidator validator) throws Exception {\n"
            + "    validator.validateInputSeriesNumber(1).validateInputSeriesDataType(0, Type.INT32);\n"
            + "  }\n"
            + "  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {\n"
            + "    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.INT32);\n"
            + "  }\n"
            + "  public void transform(Row row, PointCollector collector) throws IOException {\n"
            + "    if (!row.isNull(0)) { collector.putInt(row.getTime(), row.getInt(0)); }\n"
            + "  }\n"
            + "}\n",
        StandardCharsets.UTF_8);

    Path classesDir = temporaryFolder.newFolder("cross-model-classes").toPath();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertNotNull("The test requires a JDK compiler", compiler);
    String testClassPath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    ByteArrayOutputStream compilerOutput = new ByteArrayOutputStream();
    int compilerExitCode =
        compiler.run(
            null,
            compilerOutput,
            compilerOutput,
            "-classpath",
            testClassPath,
            "-d",
            classesDir.toString(),
            sourceDir.resolve("LazyScalarFunction.java").toString(),
            sourceDir.resolve("LazyHelper.java").toString(),
            sourceDir.resolve("TreeIdentity.java").toString());
    assertEquals(compilerOutput.toString(StandardCharsets.UTF_8), 0, compilerExitCode);

    Path jarPath = temporaryFolder.newFile("cross-model-udf.jar").toPath();
    try (JarOutputStream outputStream = new JarOutputStream(Files.newOutputStream(jarPath));
        Stream<Path> paths = Files.walk(classesDir)) {
      paths
          .filter(Files::isRegularFile)
          .forEach(
              path -> {
                try {
                  String entryName =
                      classesDir.relativize(path).toString().replace(File.separatorChar, '/');
                  outputStream.putNextEntry(new JarEntry(entryName));
                  Files.copy(path, outputStream);
                  outputStream.closeEntry();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
    return jarPath;
  }

  private static void assertUdfRejected(final Statement statement, final String sql) {
    try {
      statement.execute(sql);
      fail("Expected external UDF to be rejected in write statement: " + sql);
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("External user-defined function"));
    }
  }

  private static void assertRejected(final Statement statement, final String sql) {
    try {
      statement.execute(sql);
      fail("Expected statement to be rejected: " + sql);
    } catch (SQLException ignored) {
      // The current error may come from an earlier expression validation.
    }
  }
}
