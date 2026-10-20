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

package org.apache.iotdb.db.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class ObjectWriterTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRejectMissingSegments() throws Exception {
    for (boolean isGeneratedByConsensus : new boolean[] {false, true}) {
      File file = temporaryFolder.newFile();
      try (ObjectWriter writer = new ObjectWriter(file)) {
        // The first segment is missing.
        assertThrows(
            IOException.class, () -> writer.write(isGeneratedByConsensus, 2, new byte[] {3, 4}));
        assertArrayEquals(new byte[0], Files.readAllBytes(file.toPath()));

        writer.write(isGeneratedByConsensus, 0, new byte[] {1, 2});
        // The middle segment is missing.
        assertThrows(
            IOException.class, () -> writer.write(isGeneratedByConsensus, 4, new byte[] {5, 6}));
        assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(file.toPath()));

        writer.write(isGeneratedByConsensus, 2, new byte[] {3, 4});
        writer.write(isGeneratedByConsensus, 4, new byte[] {5, 6});
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, Files.readAllBytes(file.toPath()));
      }
    }
  }

  @Test
  public void testRewriteAfterReopen() throws Exception {
    File file = temporaryFolder.newFile();
    try (ObjectWriter writer = new ObjectWriter(file)) {
      writer.write(false, 0, new byte[] {1, 2, 3, 4, 5, 6});
    }
    try (ObjectWriter writer = new ObjectWriter(file)) {
      assertThrows(IOException.class, () -> writer.write(false, 2, new byte[] {7}));
      assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, Files.readAllBytes(file.toPath()));

      writer.write(true, 2, new byte[] {7});
      assertArrayEquals(new byte[] {1, 2, 7}, Files.readAllBytes(file.toPath()));
      writer.write(true, 3, new byte[] {8});
      assertArrayEquals(new byte[] {1, 2, 7, 8}, Files.readAllBytes(file.toPath()));
    }
    for (boolean isGeneratedByConsensus : new boolean[] {false, true}) {
      try (ObjectWriter writer = new ObjectWriter(file)) {
        writer.write(isGeneratedByConsensus, 0, new byte[] {9});
        assertArrayEquals(new byte[] {9}, Files.readAllBytes(file.toPath()));
        writer.write(isGeneratedByConsensus, 1, new byte[] {10, 11});
        assertArrayEquals(new byte[] {9, 10, 11}, Files.readAllBytes(file.toPath()));
      }
    }
  }
}
