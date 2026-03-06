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
package com.timecho.iotdb.commons.tools;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.user.LocalFileUserManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class UserPasswordModifierTest {

  @After
  public void clearTestPidsOverride() {
    UserPasswordModifier.testPidsOverride = null;
    UserPasswordModifier.testAllowedCodesOverride = null;
  }

  @Test
  public void testRejectSameOldAndNewPassword() throws Exception {
    Path userDir = Files.createTempDirectory("upm-users");
    try {
      String username = "u_test";
      String oldPassword = "Abcdef1234!@";

      LocalFileUserManager userManager = new LocalFileUserManager(userDir.toString());
      userManager.createUser(username, oldPassword, true, true);
      userManager.getAccessor().saveEntity(userManager.getEntity(username));

      UserPasswordModifier modifier = new UserPasswordModifier(userDir.toString());

      AuthException exception =
          Assert.assertThrows(
              AuthException.class,
              () -> modifier.modifyPassword(username, oldPassword, oldPassword, true));
      Assert.assertEquals(TSStatusCode.ILLEGAL_PASSWORD, exception.getCode());
    } finally {
      deleteDir(userDir);
    }
  }

  @Test
  public void testReadPasswordFallbackScannerPath() throws Exception {
    Method readPasswordMethod =
        UserPasswordModifier.class.getDeclaredMethod("readPassword", String.class, Scanner.class);
    readPasswordMethod.setAccessible(true);

    Scanner scanner =
        new Scanner(
            new ByteArrayInputStream("Abcdef1234!@\n".getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8.name());
    String password = (String) readPasswordMethod.invoke(null, "Enter password: ", scanner);
    Assert.assertEquals("Abcdef1234!@", password);
  }

  @Test
  public void testJpsDetectionMatchesCurrentJvmView() throws Exception {
    Method runningCheckMethod =
        UserPasswordModifier.class.getDeclaredMethod("isIoTDBProcessRunning");
    runningCheckMethod.setAccessible(true);

    boolean actual = (boolean) runningCheckMethod.invoke(null);
    boolean expected = scanByJps();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetRunningIoTDBPidsConsistentWithIsRunning() throws Exception {
    Method getPidsMethod = UserPasswordModifier.class.getDeclaredMethod("getRunningIoTDBPids");
    getPidsMethod.setAccessible(true);
    Method isRunningMethod = UserPasswordModifier.class.getDeclaredMethod("isIoTDBProcessRunning");
    isRunningMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Long> pids = (List<Long>) getPidsMethod.invoke(null);
    boolean isRunning = (boolean) isRunningMethod.invoke(null);
    Assert.assertEquals("isEmpty should equal !isRunning", pids.isEmpty(), !isRunning);
  }

  @Test
  public void testGetRunningIoTDBPidsConsistentWithScanByJps() throws Exception {
    Method getPidsMethod = UserPasswordModifier.class.getDeclaredMethod("getRunningIoTDBPids");
    getPidsMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Long> pids = (List<Long>) getPidsMethod.invoke(null);
    boolean scanFound = scanByJps();
    Assert.assertEquals(
        "getRunningIoTDBPids non-empty should match scanByJps", !pids.isEmpty(), scanFound);
  }

  @Test
  public void testGetRunningIoTDBPidsReturnsValidPids() throws Exception {
    Method getPidsMethod = UserPasswordModifier.class.getDeclaredMethod("getRunningIoTDBPids");
    getPidsMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Long> pids = (List<Long>) getPidsMethod.invoke(null);
    for (Long pid : pids) {
      Assert.assertNotNull(pid);
      Assert.assertTrue("PID should be positive: " + pid, pid > 0);
    }
  }

  @Test
  public void testInteractiveModifyShowsPidsWhenProcessRunning() throws Exception {
    UserPasswordModifier.testPidsOverride = () -> Collections.singletonList(12345L);
    Path dataRoot = Files.createTempDirectory("upm-data-root");
    try {
      createUsersDir(dataRoot, "nodeA");
      String input = dataRoot + System.lineSeparator();
      String output =
          runWithStdIO(
              input,
              () -> {
                UserPasswordModifier.interactiveModify();
                return null;
              });
      Assert.assertTrue(
          "Error message should contain PIDs when process is running", output.contains("PIDs:"));
      Assert.assertTrue(
          "Error message should tell user to stop processes",
          output.contains("Please stop them first"));
    } finally {
      deleteDir(dataRoot);
    }
  }

  @Test
  public void testInteractiveModifySuccessAcrossMultipleDirs() throws Exception {
    UserPasswordModifier.testPidsOverride = Collections::emptyList;

    Path dataRoot = Files.createTempDirectory("upm-data-root");
    try {
      String username = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
      String oldPassword = CommonDescriptor.getInstance().getConfig().getAdminPassword();
      String newPassword = "Xyzabc1234!@";

      Path userDir1 = createUsersDir(dataRoot, "nodeA");
      Path userDir2 = createUsersDir(dataRoot, "nodeB");
      seedDefaultAdminUser(userDir1, oldPassword);
      seedDefaultAdminUser(userDir2, oldPassword);

      String input =
          dataRoot
              + System.lineSeparator()
              + username
              + System.lineSeparator()
              + oldPassword
              + System.lineSeparator()
              + newPassword
              + System.lineSeparator()
              + newPassword
              + System.lineSeparator();
      String output =
          runWithStdIO(
              input,
              () -> {
                UserPasswordModifier.interactiveModify();
                return null;
              });

      assertCurrentRootPassword(userDir1, newPassword);
      assertCurrentRootPassword(userDir2, newPassword);
      Assert.assertTrue(output.contains("Password updated successfully in all user directories."));
    } finally {
      deleteDir(dataRoot);
    }
  }

  @Test
  public void testInteractiveModifyRejectSameOldAndNewPassword() throws Exception {
    UserPasswordModifier.testPidsOverride = Collections::emptyList;

    Path dataRoot = Files.createTempDirectory("upm-data-root");
    try {
      String username = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
      String oldPassword = CommonDescriptor.getInstance().getConfig().getAdminPassword();
      Path userDir = createUsersDir(dataRoot, "nodeA");
      seedDefaultAdminUser(userDir, oldPassword);

      String input =
          dataRoot
              + System.lineSeparator()
              + username
              + System.lineSeparator()
              + oldPassword
              + System.lineSeparator()
              + oldPassword
              + System.lineSeparator()
              + oldPassword
              + System.lineSeparator();
      String output =
          runWithStdIO(
              input,
              () -> {
                UserPasswordModifier.interactiveModify();
                return null;
              });

      assertCurrentRootPassword(userDir, oldPassword);
      Assert.assertTrue(output.contains("Error: New password cannot be the same as old password"));
    } finally {
      deleteDir(dataRoot);
    }
  }

  @Test
  public void testMainCommandLineModeSuccess() throws Exception {
    UserPasswordModifier.testPidsOverride = Collections::emptyList;

    Path dataRoot = Files.createTempDirectory("upm-data-root");
    try {
      String username = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
      String oldPassword = CommonDescriptor.getInstance().getConfig().getAdminPassword();
      String newPassword = "Mainnew1234!@";
      Path userDir = createUsersDir(dataRoot, "nodeMain");
      seedDefaultAdminUser(userDir, oldPassword);
      String machineCode =
          com.timecho.iotdb.commons.utils.OSUtils.generateSystemInfoContentWithVersion();
      UserPasswordModifier.testAllowedCodesOverride =
          () -> {
            List<String> codes = new ArrayList<>();
            codes.add(machineCode);
            return codes;
          };

      String output =
          runWithStdIO(
              "",
              () -> {
                UserPasswordModifier.main(
                    new String[] {dataRoot.toString(), username, oldPassword, newPassword});
                return null;
              });

      assertCurrentRootPassword(userDir, newPassword);
      Assert.assertTrue(output.contains("Password updated successfully in all user directories."));
    } finally {
      deleteDir(dataRoot);
    }
  }

  private static boolean scanByJps() {
    Process process = null;
    try {
      process = new ProcessBuilder("jps", "-l").start();
      try (Scanner scanner = new Scanner(process.getInputStream(), StandardCharsets.UTF_8.name())) {
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          if (line.contains("UserPasswordModifier")) {
            continue;
          }
          if (line.contains("org.apache.iotdb.db.service.DataNode")
              || line.contains("org.apache.iotdb.confignode.service.ConfigNode")
              || line.contains("DataNode")
              || line.contains("ConfigNode")) {
            return true;
          }
        }
      }
      process.waitFor();
      return false;
    } catch (Exception e) {
      return false;
    } finally {
      if (process != null) {
        process.destroy();
      }
    }
  }

  private interface ThrowingRunnable {
    Void run() throws Exception;
  }

  private static String runWithStdIO(String input, ThrowingRunnable runnable) throws Exception {
    InputStream originalIn = System.in;
    PrintStream originalOut = System.out;
    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
    try {
      System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
      System.setOut(new PrintStream(outputBytes, true, StandardCharsets.UTF_8.name()));
      runnable.run();
      return outputBytes.toString(StandardCharsets.UTF_8.name());
    } finally {
      System.setIn(originalIn);
      System.setOut(originalOut);
    }
  }

  private static Path createUsersDir(Path dataRoot, String nodeName) throws IOException {
    Path usersDir = dataRoot.resolve(nodeName).resolve("system").resolve("users");
    Files.createDirectories(usersDir);
    return usersDir;
  }

  /** Seed default admin user in the given users dir so interactive/main flow can find it. */
  private static void seedDefaultAdminUser(Path usersDir, String password)
      throws AuthException, IOException {
    LocalFileUserManager mgr = new LocalFileUserManager(usersDir.toString());
    mgr.reset();
    String name = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
    mgr.createUser(name, password, true, true);
    mgr.getAccessor().saveEntity(mgr.getEntity(name));
  }

  private static void assertCurrentRootPassword(Path usersDir, String expectedCurrentPassword)
      throws Exception {
    String rootUser = CommonDescriptor.getInstance().getConfig().getDefaultAdminName();
    UserPasswordModifier modifier = new UserPasswordModifier(usersDir.toString());
    String verifyNextPassword = expectedCurrentPassword + "_A1!";
    boolean success =
        modifier.modifyPassword(rootUser, expectedCurrentPassword, verifyNextPassword, true);
    Assert.assertTrue(success);
  }

  private static void deleteDir(Path dir) throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    Files.walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(
            path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException ignored) {
                // Ignore cleanup failures in tests
              }
            });
  }
}
