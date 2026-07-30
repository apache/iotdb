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
package org.apache.iotdb.auth.it;

import org.apache.iotdb.cli.it.AbstractScriptIT;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoginLockManagerIT extends AbstractScriptIT {

  private static String ip;

  private static String port;

  private static String sbinPath;

  private static String libPath;

  private static String homePath;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPasswordLockTimeMinutes(1);
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    sbinPath = EnvFactory.getEnv().getSbinPath();
    libPath = EnvFactory.getEnv().getLibPath();
    homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Ignore
  @Test
  public void testExemptUser() throws Exception {
    // root login success
    String loginSuccessMsg =
        "Msg: org.apache.iotdb.jdbc.IoTDBSQLException: 700: Error occurred while parsing SQL to physical plan: line 1:8 no viable alternative at input 'SELECT 1'";
    login("root", "root", new String[] {loginSuccessMsg}, 1);

    /* root will not be locked */
    String authFailedMsg = "ErrorCan't execute sql because801: Authentication failed.";
    for (int i = 0; i < 5; i++) {
      login("root", "wrong", new String[] {authFailedMsg}, 1);
    }
    // account was not locked
    login("root", "root", new String[] {loginSuccessMsg}, 1);
  }

  @Ignore
  @Test
  public void testUnlockManual() throws Exception {
    ISession session = EnvFactory.getEnv().getSessionConnection();
    // create test account 'test'
    session.executeNonQueryStatement("CREATE USER test 'test'");
    // TEST login success
    String loginSuccessMsg =
        "Msg: org.apache.iotdb.jdbc.IoTDBSQLException: 700: Error occurred while parsing SQL to physical plan: line 1:8 no viable alternative at input 'SELECT 1'";
    login("test", "test", new String[] {loginSuccessMsg}, 1);

    /* test unlock user lock */
    String authFailedMsg = "ErrorCan't execute sql because801: Authentication failed.";
    for (int i = 0; i < 5; i++) {
      login("test", "wrong", new String[] {authFailedMsg}, 1);
    }
    // account was locked
    String lockedMsg =
        "ErrorCan't execute sql because822: Account is blocked due to consecutive failed logins.";
    login("test", "test", new String[] {lockedMsg}, 1);
    // unlock user-lock manual
    session.executeNonQueryStatement("ALTER USER test ACCOUNT UNLOCK");
    login("test", "test", new String[] {loginSuccessMsg}, 1);

    /* test unlock user-ip lock */
    for (int i = 0; i < 5; i++) {
      login("test", "wrong", new String[] {authFailedMsg}, 1);
    }
    // account was locked
    login("test", "test", new String[] {lockedMsg}, 1);
    // unlock user-lock manual
    session.executeNonQueryStatement("ALTER USER test @ '127.0.0.1' ACCOUNT UNLOCK");
    login("test", "test", new String[] {loginSuccessMsg}, 1);
  }

  protected void login(String username, String password, String[] expectOutput, int statusCode)
      throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      loginOnWindows(username, password, expectOutput, statusCode);
    } else {
      loginOnUnix(username, password, expectOutput, statusCode);
    }
  }

  protected void loginOnWindows(
      String username, String password, String[] expectOutput, int statusCode) throws IOException {
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            sbinPath + File.separator + "windows" + File.separator + "start-cli.bat",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            username,
            "-pw",
            password,
            "-e",
            "\"SELECT 1\"",
            "&",
            "exit",
            "%^errorlevel%");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, expectOutput, statusCode);
  }

  protected void loginOnUnix(
      String username, String password, String[] expectOutput, int statusCode) throws IOException {
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            username,
            "-pw",
            password,
            "-e",
            "\"SELECT 1\"");
    builder.environment().put("IOTDB_HOME", homePath);
    testOutput(builder, expectOutput, statusCode);
  }

  @Override
  protected void testOnWindows() {}

  @Override
  protected void testOnUnix() {}
}
