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

package org.apache.iotdb.consensus.ratis;

import org.apache.ratis.protocol.RaftGroupId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class RaftLogSummaryTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testOpenSegmentSizeChanges() throws IOException {
    final DiskGuardian.RaftLogSummary summary =
        new DiskGuardian.RaftLogSummary(RaftGroupId.randomId(), temporaryFolder.getRoot());
    final File log = temporaryFolder.newFile("log_inprogress_0");
    summary.updateNow();
    assertEquals(0, summary.getTotalSize());

    setLength(log, 4 * 1024 * 1024);
    summary.updateNow();
    assertEquals(4 * 1024 * 1024, summary.getTotalSize());

    setLength(log, 8 * 1024 * 1024);
    summary.updateNow();
    assertEquals(8 * 1024 * 1024, summary.getTotalSize());

    setLength(log, 128);
    summary.updateNow();
    assertEquals(128, summary.getTotalSize());
  }

  @Test
  public void testRecreatedSegmentAtSamePath() throws IOException {
    final DiskGuardian.RaftLogSummary summary =
        new DiskGuardian.RaftLogSummary(RaftGroupId.randomId(), temporaryFolder.getRoot());
    final File log = temporaryFolder.newFile("log_inprogress_0");
    summary.updateNow();
    assertEquals(0, summary.getTotalSize());

    Files.delete(log.toPath());
    setLength(log, 256);
    summary.updateNow();
    assertEquals(256, summary.getTotalSize());
  }

  @Test
  public void testSegmentRotationAndDeletion() throws IOException {
    final DiskGuardian.RaftLogSummary summary =
        new DiskGuardian.RaftLogSummary(RaftGroupId.randomId(), temporaryFolder.getRoot());
    final File closed = temporaryFolder.newFile("log_0-9");
    final File open = temporaryFolder.newFile("log_inprogress_10");
    setLength(closed, 8);
    setLength(open, 32);
    setLength(temporaryFolder.newFile("raft-meta"), 1024);
    setLength(temporaryFolder.newFile("log_not_a_segment"), 1024);
    summary.updateNow();
    assertEquals(40, summary.getTotalSize());

    final File rotated = new File(temporaryFolder.getRoot(), "log_10-19");
    Files.move(open.toPath(), rotated.toPath());
    final File next = temporaryFolder.newFile("log_inprogress_20");
    setLength(next, 16);
    summary.updateNow();
    assertEquals(56, summary.getTotalSize());

    Files.delete(closed.toPath());
    summary.updateNow();
    assertEquals(48, summary.getTotalSize());

    Files.delete(rotated.toPath());
    Files.delete(next.toPath());
    summary.updateNow();
    assertEquals(0, summary.getTotalSize());
  }

  private static void setLength(File file, long length) throws IOException {
    try (RandomAccessFile writer = new RandomAccessFile(file, "rw")) {
      writer.setLength(length);
    }
  }
}
