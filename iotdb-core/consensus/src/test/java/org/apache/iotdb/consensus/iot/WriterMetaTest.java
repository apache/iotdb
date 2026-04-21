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

package org.apache.iotdb.consensus.iot;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WriterMetaTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPersistAndLoadRoundTrip() throws IOException {
    final Path metaPath = temporaryFolder.newFile("writer.meta").toPath();
    Files.deleteIfExists(metaPath);

    final WriterMeta expected = new WriterMeta(7L, 123L, 456L);
    expected.persist(metaPath);

    final Optional<WriterMeta> loaded = WriterMeta.load(metaPath);

    assertTrue(loaded.isPresent());
    assertEquals(7L, loaded.get().getWriterEpoch());
    assertEquals(123L, loaded.get().getLastAllocatedLocalSeq());
    assertEquals(456L, loaded.get().getLastAssignedPhysicalTimeMs());
    assertFalse(Files.exists(tempPath(metaPath)));
    assertTrue(Files.size(metaPath) > 0);
  }

  @Test
  public void testPersistOverwritesExistingFileWithoutLeavingTempFile() throws IOException {
    final File dir = temporaryFolder.newFolder("writer-meta");
    final Path metaPath = dir.toPath().resolve("writer.meta");

    new WriterMeta(1L, 10L, 100L).persist(metaPath);
    new WriterMeta(2L, 20L, 200L).persist(metaPath);

    final Optional<WriterMeta> loaded = WriterMeta.load(metaPath);

    assertTrue(loaded.isPresent());
    assertEquals(2L, loaded.get().getWriterEpoch());
    assertEquals(20L, loaded.get().getLastAllocatedLocalSeq());
    assertEquals(200L, loaded.get().getLastAssignedPhysicalTimeMs());
    assertFalse(Files.exists(tempPath(metaPath)));
  }

  private static Path tempPath(final Path metaPath) {
    return metaPath.getParent().resolve(metaPath.getFileName().toString() + ".tmp");
  }
}
