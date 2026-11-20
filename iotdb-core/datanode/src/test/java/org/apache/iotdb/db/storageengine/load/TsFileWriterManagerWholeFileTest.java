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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileConsensusNode;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.load.splitter.WholeFileData;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32C;

public class TsFileWriterManagerWholeFileTest {

  private File taskDir;
  private TsFileWriterManager writerManager;

  @Before
  public void setUp() throws Exception {
    taskDir = Files.createTempDirectory("whole-file-staging-test").toFile();
    final DataRegion dataRegion = Mockito.mock(DataRegion.class);
    writerManager = new TsFileWriterManager(dataRegion, taskDir);
  }

  @After
  public void tearDown() throws Exception {
    if (writerManager != null) {
      writerManager.close();
    }
    deleteRecursively(taskDir);
  }

  @Test
  public void testSlicesAreAppendedInOrderAndDuplicatesAreSkipped() throws Exception {
    final byte[] source = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};
    final List<LoadTsFileConsensusNode.PieceRef> firstRefs =
        writerManager.writePiece(Collections.singletonList(slice(0, source, 0, 3)), 1L);
    Assert.assertTrue(firstRefs.isEmpty());
    final List<LoadTsFileConsensusNode.PieceRef> secondRefs =
        writerManager.writePiece(Collections.singletonList(slice(3, source, 3, 5)), 2L);
    Assert.assertTrue(secondRefs.isEmpty());

    // A retried first slice must not append its bytes a second time.
    writerManager.writePiece(Collections.singletonList(slice(0, source, 0, 3)), 3L);

    final File staged = new File(taskDir, "whole-file.tsfile");
    Assert.assertTrue(staged.isFile());
    Assert.assertArrayEquals(source, Files.readAllBytes(staged.toPath()));
    Assert.assertTrue(writerManager.hasStagedData());
  }

  @Test
  public void testHoleIsRejected() throws Exception {
    final byte[] source = new byte[] {0, 1, 2, 3};
    try {
      writerManager.writePiece(Collections.singletonList(slice(2, source, 2, 2)), 1L);
      Assert.fail("a slice that leaves a hole must be rejected");
    } catch (final IOException expected) {
      // expected: the receiver cannot append a slice whose predecessor never arrived
    }
  }

  private static WholeFileData slice(
      final int offset, final byte[] source, final int from, final int length) {
    final byte[] payload = new byte[length];
    System.arraycopy(source, from, payload, 0, length);
    final CRC32C crc32c = new CRC32C();
    crc32c.update(payload, 0, payload.length);
    return new WholeFileData(offset, source.length, 0L, false, crc32c.getValue(), payload);
  }

  private static void deleteRecursively(final File file) {
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    file.delete();
  }
}
