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

package org.apache.iotdb.db.storageengine.load;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class LoadTsFileSnapshotMetaTest {

  @Test
  public void testSnapshotMetaRoundTrip() throws IOException {
    final File metaFile = File.createTempFile("load-snapshot-meta", ".meta");
    try {
      final List<LoadSnapshotManager.StagedFileSnapshot> snapshots = new ArrayList<>();
      snapshots.add(
          new LoadSnapshotManager.StagedFileSnapshot(
              "root.sg_1_0.tsfile", "root.sg", "1", 0L, false));
      snapshots.add(
          new LoadSnapshotManager.StagedFileSnapshot(
              "root.sg_1_1000.tsfile", "root.sg", "1", 1000L, true));

      final LoadSnapshotManager.TaskSnapshot taskSnapshot =
          new LoadSnapshotManager.TaskSnapshot(snapshots, "0=1,1=2,");
      LoadSnapshotManager.writeSnapshotMeta(metaFile, taskSnapshot);

      final LoadSnapshotManager.TaskSnapshot parsedTaskSnapshot =
          LoadSnapshotManager.parseSnapshotMeta(metaFile);
      Assert.assertEquals(2, parsedTaskSnapshot.getStagedFiles().size());
      Assert.assertEquals("0=1,1=2,", parsedTaskSnapshot.getAppliedPieces());

      final LoadSnapshotManager.StagedFileSnapshot first =
          parsedTaskSnapshot.getStagedFiles().get(0);
      Assert.assertEquals("root.sg_1_0.tsfile", first.getFileName());
      Assert.assertEquals("root.sg", first.getDatabase());
      Assert.assertEquals("1", first.getRegionId());
      Assert.assertEquals(0L, first.getTimePartitionStart());
      Assert.assertFalse(first.isFinalized());

      final LoadSnapshotManager.StagedFileSnapshot second =
          parsedTaskSnapshot.getStagedFiles().get(1);
      Assert.assertEquals("root.sg_1_1000.tsfile", second.getFileName());
      Assert.assertEquals(1000L, second.getTimePartitionStart());
      Assert.assertTrue(second.isFinalized());
    } finally {
      Files.deleteIfExists(metaFile.toPath());
    }
  }

  @Test
  public void testSnapshotMetaRejectsMalformedLine() throws IOException {
    final File metaFile = File.createTempFile("load-snapshot-meta", ".meta");
    try {
      Files.write(metaFile.toPath(), "only-one-field\n".getBytes(StandardCharsets.UTF_8));
      try {
        LoadSnapshotManager.parseSnapshotMeta(metaFile);
        Assert.fail("expected IOException for malformed snapshot meta");
      } catch (IOException expected) {
        // expected
      }
    } finally {
      Files.deleteIfExists(metaFile.toPath());
    }
  }
}
