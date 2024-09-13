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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PipeDataNodeSnapshotResourceManagerTest {
  private static final String ROOT_DIR =
      "target" + File.separator + "PipeSnapshotResourceManagerTest";
  private static final String CONSENSUS_SNAPSHOT_DIR =
      ROOT_DIR + File.separator + "consensus" + File.separator + "snapshot0";
  private static final String PIPE_CONSENSUS_SNAPSHOT_DIR =
      ROOT_DIR
          + File.separator
          + "consensus"
          + File.separator
          + "pipe_snapshot"
          + File.separator
          + "snapshot0";

  private static final String WRONG_SNAPSHOT_DIR =
      ROOT_DIR + File.separator + "wrong" + File.separator + "snapshot0";
  private static final String PIPE_WRONG_SNAPSHOT_DIR =
      ROOT_DIR
          + File.separator
          + "wrong"
          + File.separator
          + "pipe_snapshot"
          + File.separator
          + "snapshot0";
  private static final String FILE = "test_file";

  @Before
  public void setUp() throws Exception {
    final File testDir = new File(ROOT_DIR);
    if (testDir.exists()) {
      FileUtils.deleteFileOrDirectory(testDir);
    }

    new File(CONSENSUS_SNAPSHOT_DIR).mkdirs();
    new File(WRONG_SNAPSHOT_DIR).mkdirs();
    new File(CONSENSUS_SNAPSHOT_DIR, FILE).createNewFile();
    new File(WRONG_SNAPSHOT_DIR, FILE).createNewFile();
  }

  @After
  public void tearDown() throws Exception {
    final File testDir = new File(ROOT_DIR);
    if (testDir.exists()) {
      FileUtils.deleteFileOrDirectory(testDir);
    }
  }

  @Test
  public void test() {
    try {
      PipeDataNodeResourceManager.snapshot()
          .increaseSnapshotReference(CONSENSUS_SNAPSHOT_DIR + File.separator + FILE);
    } catch (final IOException e) {
      Assert.fail();
    }

    Assert.assertEquals(
        1,
        PipeDataNodeResourceManager.snapshot()
            .getSnapshotReferenceCount(PIPE_CONSENSUS_SNAPSHOT_DIR + File.separator + FILE));
    Assert.assertTrue(new File(PIPE_CONSENSUS_SNAPSHOT_DIR, FILE).exists());

    PipeDataNodeResourceManager.snapshot()
        .decreaseSnapshotReference(PIPE_CONSENSUS_SNAPSHOT_DIR + File.separator + FILE);

    Assert.assertEquals(
        0,
        PipeDataNodeResourceManager.snapshot()
            .getSnapshotReferenceCount(PIPE_CONSENSUS_SNAPSHOT_DIR + File.separator + FILE));
    Assert.assertFalse(new File(PIPE_CONSENSUS_SNAPSHOT_DIR, FILE).exists());

    try {
      PipeDataNodeResourceManager.snapshot()
          .increaseSnapshotReference(WRONG_SNAPSHOT_DIR + File.separator + FILE);
      Assert.fail();
    } catch (final IOException e) {
    }

    Assert.assertEquals(
        0,
        PipeDataNodeResourceManager.snapshot()
            .getSnapshotReferenceCount(PIPE_WRONG_SNAPSHOT_DIR + File.separator + FILE));
    Assert.assertFalse(new File(PIPE_WRONG_SNAPSHOT_DIR, FILE).exists());
  }
}
