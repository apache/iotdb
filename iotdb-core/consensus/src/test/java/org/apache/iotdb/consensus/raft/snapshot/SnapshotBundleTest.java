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

package org.apache.iotdb.consensus.raft.snapshot;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.raft.core.Membership;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SnapshotBundleTest {
  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = Files.createTempDirectory("raft-file-bundle").toFile();
  }

  @After
  public void tearDown() {
    FileUtils.deleteFileOrDirectory(dir);
  }

  private File source() throws IOException {
    File root = new File(dir, "source");
    File data = new File(root, "sequence/root.db/1/0/1-1-0-0.tsfile");
    Files.createDirectories(data.getParentFile().toPath());
    byte[] bytes = new byte[32768];
    Arrays.fill(bytes, (byte) 42);
    Files.write(data.toPath(), bytes);
    File mods = new File(data + ".mods2");
    Files.write(mods.toPath(), "deleted-range".getBytes(StandardCharsets.UTF_8));
    new SnapshotMeta(10, 2, Membership.empty(), null, root).save();
    SnapshotBundle.create(root, Arrays.asList(data, mods, new File(root, SnapshotMeta.MANIFEST)));
    return root;
  }

  @Test
  public void compressedRoundTripRetainsAuxiliaryFilesAndIdentity() throws IOException {
    File source = source();
    File archive = SnapshotBundle.archive(source);
    assertTrue(archive.length() < SnapshotBundle.rawBytes(source));
    File target = new File(dir, "target");
    assertTrue(target.mkdirs());
    Files.copy(archive.toPath(), new File(target, SnapshotBundle.ARCHIVE).toPath());
    SnapshotBundle.unpack(target);
    SnapshotBundle.verify(target);
    assertEquals(10, SnapshotMeta.load(target).index);
    assertEquals(2, SnapshotMeta.load(target).term);
    assertEquals(3, SnapshotBundle.inventory(target).size());
    assertFalse(new File(target, SnapshotBundle.ARCHIVE).exists());
  }

  @Test
  public void missingOrChangedFileCannotBeInstalled() throws IOException {
    File source = source();
    Path data = new File(source, "sequence/root.db/1/0/1-1-0-0.tsfile").toPath();
    Files.write(data, new byte[] {1});
    try {
      SnapshotBundle.verify(source);
      fail("changed file accepted");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("corrupt"));
    }
    Files.delete(data);
    try {
      SnapshotBundle.verify(source);
      fail("missing file accepted");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("incomplete"));
    }
  }

  @Test
  public void externalFilesAndSiblingPrefixEscapesAreRejected() throws IOException {
    File source = source();
    File outside = new File(dir, "outside");
    Files.write(outside.toPath(), new byte[] {1});
    try {
      SnapshotBundle.create(source, Collections.singletonList(outside));
      fail("external snapshot accepted");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("portable root"));
    }
    for (String name : Arrays.asList("../source-escape/file", "/absolute/file", "../outside", "")) {
      try {
        SnapshotBundle.resolve(source, name);
        fail("escaped snapshot path accepted: " + name);
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("illegal"));
      }
    }
  }

  @Test
  public void unlistedDataFileIsRejected() throws IOException {
    File source = source();
    Files.write(new File(source, "unexpected.tsfile").toPath(), new byte[] {1, 2});
    try {
      SnapshotBundle.verify(source);
      fail("unlisted data file accepted");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("unexpected"));
    }
  }

  @Test
  public void truncatedArchiveIsRejected() throws IOException {
    File source = source();
    byte[] archive = Files.readAllBytes(SnapshotBundle.archive(source).toPath());
    File target = new File(dir, "target");
    assertTrue(target.mkdirs());
    Files.write(new File(target, SnapshotBundle.ARCHIVE).toPath(), Arrays.copyOf(archive, 80));
    try {
      SnapshotBundle.unpack(target);
      fail("truncated archive accepted");
    } catch (IOException expected) {
      // No state-machine loader runs until unpack + inventory verification both succeed.
    }
  }
}
