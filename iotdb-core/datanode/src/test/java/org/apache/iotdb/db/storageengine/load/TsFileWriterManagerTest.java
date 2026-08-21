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

import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileDataType;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Focused unit tests for the per-task applied/cached/retained piece bookkeeping. */
public class TsFileWriterManagerTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAppliedContiguityFence() throws IOException {
    final TsFileWriterManager manager =
        new TsFileWriterManager(temporaryFolder.newFolder("task-1"));
    try {
      Assert.assertTrue(manager.hasAppliedAllUpTo(-1));
      Assert.assertFalse(manager.hasAppliedAllUpTo(0));

      manager.recordAppliedPiece(1, 111L);
      // Piece 1 without piece 0 is not a contiguous prefix yet.
      Assert.assertFalse(manager.hasAppliedAllUpTo(1));

      manager.recordAppliedPiece(0, 100L);
      Assert.assertTrue(manager.hasAppliedAllUpTo(0));
      Assert.assertTrue(manager.hasAppliedAllUpTo(1));
      Assert.assertFalse(manager.hasAppliedAllUpTo(2));

      manager.recordAppliedPiece(3, 333L);
      // A hole at 2 keeps the prefix at 2.
      Assert.assertFalse(manager.hasAppliedAllUpTo(3));
      manager.recordAppliedPiece(2, 222L);
      Assert.assertTrue(manager.hasAppliedAllUpTo(3));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRestoredAppliedPiecesRebuildsContiguity() throws IOException {
    final TsFileWriterManager manager =
        new TsFileWriterManager(temporaryFolder.newFolder("task-2"));
    try {
      manager.restoreAppliedPieces("0:100,1:111,2:222,");
      Assert.assertTrue(manager.hasAppliedAllUpTo(2));
      Assert.assertFalse(manager.hasAppliedAllUpTo(3));
      Assert.assertTrue(manager.isPieceAlreadyApplied(1, 111L));
      Assert.assertTrue(manager.isPieceConflicting(1, 999L));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testCachedPieceChecksumConflict() throws IOException {
    final TsFileWriterManager manager =
        new TsFileWriterManager(temporaryFolder.newFolder("task-3"));
    try {
      final byte[] data = "chunk-data".getBytes(StandardCharsets.UTF_8);
      Assert.assertTrue(manager.cachePiece(0, 1L, Arrays.asList(new TestTsFileData(data))));
      // Same index with the same checksum is idempotent.
      Assert.assertTrue(manager.cachePiece(0, 1L, Arrays.asList(new TestTsFileData(data))));
      Assert.assertTrue(manager.hasCachedPiece(0, 1L));
      // Same index with a different checksum is a divergent delivery and must be rejected.
      Assert.assertFalse(manager.cachePiece(0, 2L, Arrays.asList(new TestTsFileData(data))));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRetainedPiecesSurviveRestart() throws IOException {
    final File taskDir = temporaryFolder.newFolder("task-4");
    final byte[] pieceBytes = "serialized-piece".getBytes(StandardCharsets.UTF_8);
    final TsFileWriterManager first = new TsFileWriterManager(taskDir);
    // Simulate a crash: the first manager is abandoned without close() so the retained bytes stay
    // on disk, exactly like a process that dies mid-load.
    first.retainPiece(0, pieceBytes);

    // A new manager over the same (restored) task dir must reload the retained bytes from disk.
    final TsFileWriterManager second = new TsFileWriterManager(taskDir, false);
    try {
      Assert.assertTrue(Arrays.equals(pieceBytes, second.getRetainedPiece(0).orElse(null)));
    } finally {
      second.close();
    }
  }

  /** Minimal TsFileData placeholder for cache bookkeeping tests (never applied to a writer). */
  private static final class TestTsFileData implements TsFileData {

    private final byte[] data;

    private TestTsFileData(byte[] data) {
      this.data = data;
    }

    @Override
    public long getDataSize() {
      return data.length;
    }

    @Override
    public TsFileDataType getType() {
      return TsFileDataType.CHUNK;
    }

    @Override
    public void serialize(DataOutputStream stream) {
      throw new UnsupportedOperationException("not used in this test");
    }
  }
}
