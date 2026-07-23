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

package com.timecho.iotdb.db.it.audit;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This test class verifies that TTL object scan failures and settle compaction failures produce
 * FAILED_TTL_DELETION audit logs with the expected fields and format.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class TimechoDBAuditLogTTLDeletionIT {

  private static final long TTL_CHECK_INTERVAL_IN_MS = 3_000L;
  private static final long POLL_INTERVAL_IN_MS = 500L;
  private static final long INITIAL_TTL_IN_MS = TimeUnit.DAYS.toMillis(2);

  private static final boolean ENABLE_AUDIT_LOG = true;

  private static final String DATABASE = "test_ttl_audit";
  private static final String TABLE_NAME = "test_table";

  private final List<File> lockedDirs = new ArrayList<>();

  @BeforeClass
  public static void skipWhenDirectoryPermissionsCannotBlockAccess() {
    Assume.assumeTrue(
        "Skipping TTL deletion audit IT because directory permissions cannot block access",
        canRestrictDirectoryAccess());
  }

  private static boolean canRestrictDirectoryAccess() {
    File directory = null;
    try {
      directory = Files.createTempDirectory("ttl-deletion-audit-permission-").toFile();
      if (!new File(directory, "existing-file").createNewFile()) {
        return false;
      }

      boolean canPreventListing =
          directory.setReadable(false, false)
              & directory.setExecutable(false, false)
              & directory.listFiles() == null;
      restoreDirectoryPermissions(directory);

      boolean canPreventCreatingFile =
          directory.setWritable(false, false)
              & directory.setExecutable(false, false)
              & !createFile(new File(directory, "new-file"));
      return canPreventListing && canPreventCreatingFile;
    } catch (IOException e) {
      return false;
    } finally {
      if (directory != null) {
        restoreDirectoryPermissions(directory);
        File[] files = directory.listFiles();
        if (files != null) {
          for (File file : files) {
            file.delete();
          }
        }
        directory.delete();
      }
    }
  }

  private static boolean createFile(File file) {
    try {
      return file.createNewFile();
    } catch (IOException e) {
      return false;
    }
  }

  private static void restoreDirectoryPermissions(File directory) {
    directory.setReadable(true, false);
    directory.setWritable(true, false);
    directory.setExecutable(true, false);
  }

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAuditLog(ENABLE_AUDIT_LOG)
        .setAuditableOperationResult("FAIL")
        .setAuditableOperationType(AuditLogOperation.CONTROL.toString())
        .setAuditableControlEventType(AuditEventType.FAILED_TTL_DELETION.toString())
        .setTTLCheckInterval(TTL_CHECK_INTERVAL_IN_MS);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    for (File dir : lockedDirs) {
      setDirectoryPermissions(dir, true, true, true);
    }
    lockedDirs.clear();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTTLDeletionFailureAudit() throws Exception {
    // 1. Create database and table with OBJECT field, insert data, flush
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("USE " + DATABASE);
      session.executeNonQueryStatement(
          "CREATE TABLE "
              + TABLE_NAME
              + "(tag1 STRING TAG, o1 OBJECT, s1 STRING) WITH (TTL="
              + INITIAL_TTL_IN_MS
              + ")");
      long oldTime = System.currentTimeMillis() - 24L * 3600 * 1000; // 1 day ago
      for (int i = 0; i < 100; i++) {
        session.executeNonQueryStatement(
            "INSERT INTO "
                + TABLE_NAME
                + "(time, tag1, o1, s1) VALUES ("
                + (oldTime + i)
                + ", 'd1', to_object(true, 0, X'cafebabe"
                + String.format("%02x", i)
                + "'), 'value"
                + i
                + "')");
      }
      session.executeNonQueryStatement("FLUSH");
    }

    // Give TsFiles a brief window to finalize on disk
    TimeUnit.SECONDS.sleep(2);

    // 2. Phase A — lock the object storage region dir so the scan cannot even enter table dirs
    List<File> phaseALocks = new ArrayList<>();
    for (DataNodeWrapper dn : EnvFactory.getEnv().getDataNodeWrapperList()) {
      File objectRoot = new File(dn.getDataNodeObjectDir());
      if (objectRoot.isDirectory()) {
        // Lock each region-level directory under the object root
        File[] regionDirs = objectRoot.listFiles();
        if (regionDirs != null) {
          for (File regionDir : regionDirs) {
            if (regionDir.isDirectory()) {
              setDirectoryPermissions(regionDir, false, null, false);
              phaseALocks.add(regionDir);
            }
          }
        }
      }
    }
    Assert.assertFalse("Unable to lock any object region directories", phaseALocks.isEmpty());

    try {
      // 3. Poll for the first object TTL scan's "TTL check failed to start" audit log
      long deadline = System.currentTimeMillis() + 3 * TTL_CHECK_INTERVAL_IN_MS + 5_000L;
      boolean foundFailedToStart = false;
      while (System.currentTimeMillis() < deadline) {
        TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_IN_MS);
        foundFailedToStart = queryLogContains("TTL check failed to start:");
        if (foundFailedToStart) {
          break;
        }
      }
      Assert.assertTrue(
          "FAILED_TTL_DELETION audit log for TTL check failed to start was not found",
          foundFailedToStart);
    } finally {
      for (File dir : phaseALocks) {
        setDirectoryPermissions(dir, true, null, true);
      }
    }

    // 4. Phase B — Break per-file parent dirs so object scan and settle compaction both fail
    for (DataNodeWrapper dn : EnvFactory.getEnv().getDataNodeWrapperList()) {
      // Object storage directory — break .bin files
      File objectDir = new File(dn.getDataNodeObjectDir());
      if (objectDir.isDirectory()) {
        lockBinFileParents(objectDir, lockedDirs);
      }
      // TsFile data dir under the test database — lock .tsfile parent dirs
      // so settle compaction cannot delete them. Scoped to sequence/${DATABASE}
      // to avoid blocking root.__audit writes.
      File seqDbDir = new File(new File(dn.getDataNodeDir(), "data"), "sequence/" + DATABASE);
      if (seqDbDir.isDirectory()) {
        lockTsFileParents(seqDbDir, lockedDirs);
      }
    }
    Assert.assertFalse("Unable to lock any file directories", lockedDirs.isEmpty());

    // 5. Set TTL to expire all data only after Phase B locks are in place
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE);
      session.executeNonQueryStatement("ALTER TABLE " + TABLE_NAME + " SET PROPERTIES TTL=1");
    }

    // 6. Wait for TTL check cycles — poll until both types of audit log appear
    long deadline = System.currentTimeMillis() + 5 * TTL_CHECK_INTERVAL_IN_MS + 5_000L;
    boolean foundScan = false;
    boolean foundSettle = false;
    while (System.currentTimeMillis() < deadline) {
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_IN_MS);
      if (!foundScan) {
        foundScan = queryAuditLogContains("TTL object scan failed: errors=");
      }
      if (!foundSettle) {
        foundSettle = queryAuditLogContains("TTL settle compaction failed: error=");
      }
      if (foundScan && foundSettle) {
        break;
      }
    }
    Assert.assertTrue(
        "FAILED_TTL_DELETION audit log for TTL object scan failure was not found", foundScan);
    Assert.assertTrue(
        "FAILED_TTL_DELETION audit log for TTL settle compaction failure was not found",
        foundSettle);
  }

  private boolean queryLogContains(String logPrefix) throws Exception {
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Statement stmt = conn.createStatement();
      ResultSet rs =
          stmt.executeQuery(
              "SELECT log FROM __audit.audit_log"
                  + " WHERE user_id = 'u_5' AND audit_event_type = 'FAILED_TTL_DELETION'"
                  + " AND log LIKE '"
                  + logPrefix
                  + "%'");
      while (rs.next()) {
        String log = rs.getString("log");
        if (log != null && log.startsWith(logPrefix)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean queryAuditLogContains(String logPrefix) throws Exception {
    try (Connection conn = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Statement stmt = conn.createStatement();
      ResultSet rs =
          stmt.executeQuery(
              "SELECT log, sql_string, cli_hostname FROM __audit.audit_log"
                  + " WHERE user_id = 'u_5' AND audit_event_type = 'FAILED_TTL_DELETION'"
                  + " AND log LIKE '"
                  + logPrefix
                  + "%'");
      while (rs.next()) {
        String log = rs.getString("log");
        String sqlString = rs.getString("sql_string");
        String cliHostname = rs.getString("cli_hostname");
        if (log != null
            && log.startsWith(logPrefix)
            && TABLE_NAME.equals(sqlString)
            && cliHostname != null
            && !cliHostname.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  private static void lockBinFileParents(File dir, List<File> lockedDirs) {
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        lockBinFileParents(child, lockedDirs);
      } else if (child.getName().endsWith(".bin")) {
        File parentDir = child.getParentFile();
        if (setDirectoryPermissions(parentDir, true, false, false)) {
          lockedDirs.add(parentDir);
        }
      }
    }
  }

  /**
   * Locks each immediate child directory of {@code seqDbDir} that contains at least one .tsfile
   * file, preventing settle compaction from deleting them.
   */
  private static void lockTsFileParents(File dir, List<File> lockedDirs) {
    File[] children = dir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        lockTsFileParents(child, lockedDirs);
      } else if (child.getName().endsWith(".tsfile")) {
        File parentDir = child.getParentFile();
        if (setDirectoryPermissions(parentDir, true, false, false)) {
          lockedDirs.add(parentDir);
        }
      }
    }
  }

  private static boolean setDirectoryPermissions(
      File directory, Boolean readable, Boolean writable, Boolean executable) {
    boolean permissionsUpdated =
        (readable == null || directory.setReadable(readable, false))
            & (writable == null || directory.setWritable(writable, false))
            & (executable == null || directory.setExecutable(executable, false));
    Assert.assertTrue("Unable to update directory permissions: " + directory, permissionsUpdated);
    return permissionsUpdated;
  }
}
