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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SnapshotFileUtilsTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testHardLink() throws Exception {
    Path source = folder.newFile("source").toPath();
    Files.write(source, new byte[] {1, 2, 3});
    Path target = source.resolveSibling("target");
    SnapshotFileUtils.createLinkOrCopy(target, source);
    assertTrue(Files.isSameFile(source, target));
    assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(target));
  }

  @Test
  public void testCopyWhenLinksUnsupported() throws Exception {
    Path archive = folder.getRoot().toPath().resolve("snapshot.zip");
    try (FileSystem fs = FileSystems.newFileSystem(archive, Map.of("create", "true"))) {
      Path source = fs.getPath("/source");
      Path target = fs.getPath("/target");
      Files.write(source, new byte[] {1, 2, 3});
      SnapshotFileUtils.createLinkOrCopy(target, source);
      Files.delete(source);
      assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(target));
    }
  }

  @Test
  public void testCopyFailureRetainsLinkFailure() throws Exception {
    Path source = folder.getRoot().toPath().resolve("missing");
    Path target = source.resolveSibling("target");
    try {
      SnapshotFileUtils.createLinkOrCopy(target, source);
      fail("Missing source must fail the snapshot");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("missing"));
      assertEquals(1, e.getSuppressed().length);
      assertTrue(e.getSuppressed()[0] instanceof IOException);
    }
    assertFalse(Files.exists(target));
  }

  @Test
  public void testExistingTargetIsNotOverwritten() throws Exception {
    Path source = folder.newFile("source").toPath();
    Path target = folder.newFile("target").toPath();
    Files.write(source, new byte[] {1});
    Files.write(target, new byte[] {2});
    try {
      SnapshotFileUtils.createLinkOrCopy(target, source);
      fail("Existing target must not be replaced");
    } catch (IOException e) {
      assertEquals(1, e.getSuppressed().length);
    }
    assertArrayEquals(new byte[] {2}, Files.readAllBytes(target));
  }
}
