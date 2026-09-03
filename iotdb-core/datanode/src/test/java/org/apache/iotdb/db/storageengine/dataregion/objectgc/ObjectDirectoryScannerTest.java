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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.NOP;

import org.apache.tsfile.read.common.TimeRange;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObjectDirectoryScannerTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void deletesLegacyAndOlderVersionsButKeepsProtected() throws Exception {
    File objectRoot = folder.newFolder("object");
    File tableDir = new File(objectRoot, "1" + File.separator + "tbl" + File.separator + "tag");
    File colDir = new File(tableDir, "col");
    assertTrue(colDir.mkdirs());
    File legacy = new File(colDir, "100.bin");
    File oldVer = new File(colDir, "100_2.bin");
    File protectedVer = new File(colDir, "100_4.bin");
    File newer = new File(colDir, "100_6.bin");
    File otherTime = new File(colDir, "200_2.bin");
    File tmp = new File(colDir, "100.bin.tmp");
    Files.write(legacy.toPath(), "l".getBytes());
    Files.write(oldVer.toPath(), "o".getBytes());
    Files.write(protectedVer.toPath(), "p".getBytes());
    Files.write(newer.toPath(), "n".getBytes());
    Files.write(otherTime.toPath(), "t".getBytes());
    Files.write(tmp.toPath(), "w".getBytes());

    TableDeletionEntry deletion =
        new TableDeletionEntry(new DeletionPredicate("tbl", new NOP()), new TimeRange(0, 150));
    Map<Long, Long> exclusive = new HashMap<>();
    exclusive.put(0L, 6L);
    Map<Long, Set<Long>> protectedVersions = new HashMap<>();
    protectedVersions.put(0L, new HashSet<>(Collections.singleton(4L)));
    ObjectGcRecord record = ObjectGcRecord.scan(1L, deletion, exclusive, protectedVersions);

    boolean original = CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit();
    CommonDescriptor.getInstance().getConfig().setRestrictObjectLimit(true);
    try {
      ObjectDirectoryScanner.scanTableDir(
          "db",
          1,
          new File(objectRoot, "1" + File.separator + "tbl"),
          record,
          deletion,
          LoggerFactory.getLogger(ObjectDirectoryScannerTest.class),
          "failed {}");

      assertFalse(legacy.exists());
      assertFalse(oldVer.exists());
      assertTrue(protectedVer.exists());
      assertTrue(newer.exists());
      assertTrue(otherTime.exists());
      assertTrue(tmp.exists());
    } finally {
      CommonDescriptor.getInstance().getConfig().setRestrictObjectLimit(original);
    }
  }

  @Test
  public void unlinkMatchingTempAndBackDeletesHitsAndLeavesBins() throws Exception {
    File objectRoot = folder.newFolder("object-tmp");
    File tableDir = new File(objectRoot, "1" + File.separator + "tbl" + File.separator + "tag");
    File colDir = new File(tableDir, "col");
    assertTrue(colDir.mkdirs());
    File tmp = new File(colDir, "100.bin.tmp");
    File back = new File(colDir, "100.bin.back");
    File otherTmp = new File(colDir, "200.bin.tmp");
    File sealed = new File(colDir, "100_2.bin");
    Files.write(tmp.toPath(), "w".getBytes());
    Files.write(back.toPath(), "b".getBytes());
    Files.write(otherTmp.toPath(), "x".getBytes());
    Files.write(sealed.toPath(), "s".getBytes());

    TableDeletionEntry deletion =
        new TableDeletionEntry(new DeletionPredicate("tbl", new NOP()), new TimeRange(0, 150));
    boolean original = CommonDescriptor.getInstance().getConfig().isRestrictObjectLimit();
    CommonDescriptor.getInstance().getConfig().setRestrictObjectLimit(true);
    try {
      ObjectDirectoryScanner.unlinkTempAndBackInTableDir(
          new File(objectRoot, "1" + File.separator + "tbl"),
          deletion,
          LoggerFactory.getLogger(ObjectDirectoryScannerTest.class),
          "failed {}");
      assertFalse(tmp.exists());
      assertFalse(back.exists());
      assertTrue(otherTmp.exists());
      assertTrue(sealed.exists());
    } finally {
      CommonDescriptor.getInstance().getConfig().setRestrictObjectLimit(original);
    }
  }

  @Test
  public void dropTableDirsDeletesLocalTree() throws Exception {
    File tableDir = folder.newFolder("object", "1", "tbl");
    File bin = new File(new File(tableDir, "col"), "100.bin");
    assertTrue(bin.getParentFile().mkdirs());
    Files.write(bin.toPath(), "x".getBytes());
    ObjectDirectoryScanner.dropTableDirs(
        Collections.singletonList(tableDir.getAbsolutePath()),
        LoggerFactory.getLogger(ObjectDirectoryScannerTest.class),
        "failed {}");
    assertFalse(tableDir.exists());
    assertFalse(bin.exists());
  }

  @Test
  public void renameTableDirForDropMovesOffLivePathThenDropCleansTombstone() throws Exception {
    File tableDir = folder.newFolder("object", "1", "tbl");
    File bin = new File(new File(tableDir, "col"), "100.bin");
    assertTrue(bin.getParentFile().mkdirs());
    Files.write(bin.toPath(), "x".getBytes());

    File tombstone = ObjectDirectoryScanner.renameTableDirForDrop(tableDir, 42L);
    assertTrue(tombstone != null);
    assertFalse(tableDir.exists());
    assertTrue(tombstone.exists());
    assertTrue(
        tombstone.getAbsolutePath().contains(ObjectDirectoryScanner.OBJECT_GC_TOMBSTONE_DIR));
    assertTrue(new File(new File(tombstone, "col"), "100.bin").exists());
    // Live path is free for a same-name recreate.
    assertTrue(tableDir.mkdirs());

    ObjectDirectoryScanner.dropTableDirs(
        Collections.singletonList(tombstone.getAbsolutePath()),
        LoggerFactory.getLogger(ObjectDirectoryScannerTest.class),
        "failed {}");
    assertFalse(tombstone.exists());
    assertTrue(tableDir.exists());
  }

  @Test
  public void renameTableDirForDropReturnsNullWhenMissing() throws Exception {
    File missing = new File(folder.getRoot(), "object/1/missing");
    assertTrue(ObjectDirectoryScanner.renameTableDirForDrop(missing, 1L) == null);
  }
}
