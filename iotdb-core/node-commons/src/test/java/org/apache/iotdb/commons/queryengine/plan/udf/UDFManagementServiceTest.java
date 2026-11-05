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

package org.apache.iotdb.commons.queryengine.plan.udf;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.UDFType;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.udf.api.relational.ScalarFunction;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class UDFManagementServiceTest {

  private static final String PACKAGE_NAME = "org.apache.iotdb.test.generated.lazy";
  private static final String FUNCTION_CLASS = PACKAGE_NAME + ".LazyScalarFunction";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testReflectPinsDefiningClassLoaderForLazyDependency() throws Exception {
    UDFClassLoaderManager classLoaderManager = UDFClassLoaderManager.setupAndGetInstance();
    Path jarPath = buildUDFJar();
    UDFClassLoader classLoader = new UDFClassLoader(jarPath.toString());
    Class<?> functionClass = Class.forName(FUNCTION_CLASS, true, classLoader);
    assertSame(classLoader, functionClass.getClassLoader());

    UDFManagementService service = UDFManagementService.getInstance();
    UDFTable udfTable = getUDFTable(service);
    String functionName = "lazy_scope_probe";
    UDFInformation information =
        new UDFInformation(functionName, FUNCTION_CLASS, UDFType.TABLE_AVAILABLE_SCALAR);
    udfTable.addUDFInformation(functionName, information);
    udfTable.addFunctionAndClass(Model.TABLE, functionName, functionClass);

    UDFClassLoaderManager.QueryScope queryScope =
        classLoaderManager.openQueryScope("lazy-scope-probe");
    try {
      classLoaderManager.withQueryScope(
          queryScope,
          () -> {
            ScalarFunction function = service.reflect(functionName, ScalarFunction.class);
            markAsDeprecated(classLoader);
            classLoaderManager.finalizeUDFQuery(queryScope);

            assertFalse(isClosed(classLoader));
            assertNotNull(function.analyze(null));
            assertFalse(isClosed(classLoader));
            return null;
          });
      assertTrue(isClosed(classLoader));
    } finally {
      classLoaderManager.finalizeUDFQuery(queryScope);
      udfTable.removeUDFInformation(Model.TABLE, functionName);
      udfTable.removeFunctionClass(Model.TABLE, functionName);
    }
  }

  private Path buildUDFJar() throws Exception {
    Path sourceRoot = temporaryFolder.newFolder("source").toPath();
    Path sourceDir = sourceRoot.resolve(PACKAGE_NAME.replace('.', File.separatorChar));
    Files.createDirectories(sourceDir);

    Path functionSource = sourceDir.resolve("LazyScalarFunction.java");
    Files.writeString(
        functionSource,
        "package "
            + PACKAGE_NAME
            + ";\n"
            + "import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;\n"
            + "import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;\n"
            + "import org.apache.iotdb.udf.api.relational.ScalarFunction;\n"
            + "import org.apache.iotdb.udf.api.relational.access.Record;\n"
            + "import org.apache.iotdb.udf.api.type.Type;\n"
            + "public class LazyScalarFunction implements ScalarFunction {\n"
            + "  public ScalarFunctionAnalysis analyze(FunctionArguments arguments) {\n"
            + "    Class.forName(\""
            + PACKAGE_NAME
            + ".LazyHelper\", true, LazyScalarFunction.class.getClassLoader());\n"
            + "    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.INT32).build();\n"
            + "  }\n"
            + "  public Object evaluate(Record input) { return null; }\n"
            + "}\n",
        StandardCharsets.UTF_8);

    Path helperSource = sourceDir.resolve("LazyHelper.java");
    Files.writeString(
        helperSource,
        "package " + PACKAGE_NAME + ";\npublic class LazyHelper {}\n",
        StandardCharsets.UTF_8);

    Path classesDir = temporaryFolder.newFolder("classes").toPath();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertNotNull(compiler);
    assertEquals(
        0,
        compiler.run(
            null,
            null,
            null,
            "-classpath",
            System.getProperty("java.class.path"),
            "-d",
            classesDir.toString(),
            functionSource.toString(),
            helperSource.toString()));

    Path jarPath = temporaryFolder.newFile("lazy-udf.jar").toPath();
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
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }
    return jarPath;
  }

  private static UDFTable getUDFTable(UDFManagementService service) throws Exception {
    Field field = UDFManagementService.class.getDeclaredField("udfTable");
    field.setAccessible(true);
    return (UDFTable) field.get(service);
  }

  private static void markAsDeprecated(UDFClassLoader classLoader) {
    try {
      classLoader.markAsDeprecated();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean isClosed(UDFClassLoader classLoader) {
    try {
      Field field = UDFClassLoader.class.getDeclaredField("closed");
      field.setAccessible(true);
      return field.getBoolean(classLoader);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
