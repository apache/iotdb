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
package org.apache.iotdb.tools.it;

import org.apache.iotdb.cli.it.AbstractScriptIT;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class ExportSchemaTestIT extends AbstractScriptIT {
  private static String ip;

  private static String port;

  private static String toolsPath;

  private static String libPath;

  private static String homePath;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    toolsPath = EnvFactory.getEnv().getToolsPath();
    libPath = EnvFactory.getEnv().getLibPath();
    homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
  }

  @Override
  protected void testOnWindows() throws IOException {
    prepareSchema();
    final String[] output = {Constants.EXPORT_COMPLETELY};
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            toolsPath
                + File.separator
                + "windows"
                + File.separator
                + "schema"
                + File.separator
                + "export-schema.bat",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-t",
            "target",
            "-path",
            "root.**",
            "&",
            "exit",
            "%^errorlevel%");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, output, 0);
  }

  @Override
  protected void testOnUnix() throws IOException {
    prepareSchema();
    final String[] output = {Constants.EXPORT_COMPLETELY};
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            toolsPath + File.separator + "schema" + File.separator + "export-schema.sh",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-t",
            "target",
            "-path",
            "root.**");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, output, 0);
  }

  public void prepareSchema() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.open();
      session.createTimeseries(
          "root.schema.t2.c1",
          TSDataType.DOUBLE,
          TSEncoding.GORILLA,
          CompressionType.LZ4,
          null,
          null,
          null,
          null);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
