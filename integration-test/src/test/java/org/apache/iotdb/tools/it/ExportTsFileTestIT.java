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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class ExportTsFileTestIT extends AbstractScriptIT {
  private static String ip;

  private static String port;

  private static String toolsPath;

  private static String libPath;

  @BeforeClass
  public static void setUp() throws Exception {
    // enable subscription
    EnvFactory.getEnv().getConfig().getCommonConfig().setSubscriptionEnabled(true);
    EnvFactory.getEnv().initClusterEnvironment();

    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    toolsPath = EnvFactory.getEnv().getToolsPath();
    libPath = EnvFactory.getEnv().getLibPath();
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
    // Test for empty export, temporary removal
    //    final String[] output = {"Export TsFile Count: 0"};
    //    ProcessBuilder builder =
    //        new ProcessBuilder(
    //            "cmd.exe",
    //            "/c",
    //            toolsPath + File.separator + "windows" + File.separator + "export-tsfile.bat",
    //            "-h",
    //            ip,
    //            "-p",
    //            port,
    //            "-u",
    //            "root",
    //            "-pw",
    //            "root",
    //            "-path",
    //            "root.test.t2.**",
    //            "&",
    //            "exit",
    //            "%^errorlevel%");
    //    builder.environment().put("CLASSPATH", libPath);
    //    testOutput(builder, output, 0);

    prepareData();

    final String[] output1 = {"Export TsFile Count: "};
    ProcessBuilder builder1 =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            toolsPath + File.separator + "windows" + File.separator + "export-tsfile.bat",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-path",
            "root.test.t2.**",
            "&",
            "exit",
            "%^errorlevel%");
    builder1.environment().put("CLASSPATH", libPath);
    testOutput(builder1, output1, 0);
  }

  @Override
  protected void testOnUnix() throws IOException {
    // Test for empty export, temporary removal
    //    final String[] output = {"Export TsFile Count: 0"};
    //    // -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
    //    ProcessBuilder builder =
    //        new ProcessBuilder(
    //            "bash",
    //            toolsPath + File.separator + "export-tsfile.sh",
    //            "-h",
    //            ip,
    //            "-p",
    //            port,
    //            "-u",
    //            "root",
    //            "-pw",
    //            "root",
    //            "-path",
    //            "root.**");
    //    builder.environment().put("CLASSPATH", libPath);
    //    testOutput(builder, output, 0);

    prepareData();

    final String[] output1 = {"Export TsFile Count: "};
    // -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
    ProcessBuilder builder1 =
        new ProcessBuilder(
            "bash",
            toolsPath + File.separator + "export-tsfile.sh",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-path",
            "root.**");
    builder1.environment().put("CLASSPATH", libPath);
    testOutput(builder1, output1, 0);
  }

  public void prepareData() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.open();

      String deviceId = "root.test.t2";
      List<String> measurements = new ArrayList<>();
      measurements.add("c1");
      measurements.add("c2");
      measurements.add("c3");

      List<String> values = new ArrayList<>();
      values.add("1.0");
      values.add("bbbbb");
      values.add("abbes");
      session.insertRecord(deviceId, 1L, measurements, values);
      session.executeNonQueryStatement("flush");
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
