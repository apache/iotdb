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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferIntegrityUtils;

import org.apache.tsfile.exception.TsFileRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TsFileStructureCorruptionUtilsTest {

  @Test
  public void testDiagnosticContainsOffsetAndSha256() throws IOException {
    final File tsFile = Files.createTempFile("tsfile-structure-corruption", ".tsfile").toFile();
    try {
      Files.writeString(tsFile.toPath(), "malformed-tsfile");
      final long corruptionOffset = 37;
      final String expectedSha256 = PipeTransferIntegrityUtils.calculateSha256(tsFile);

      final String message =
          TsFileStructureCorruptionUtils.createDiagnosticMessage(
              tsFile, corruptionOffset, new IOException("malformed metadata"));

      Assert.assertTrue(message.contains(String.valueOf(corruptionOffset)));
      Assert.assertTrue(message.contains(expectedSha256));
      Assert.assertTrue(message.contains("[TSFILE_STRUCTURE_CORRUPTION]"));
    } finally {
      Files.deleteIfExists(tsFile.toPath());
    }
  }

  @Test
  public void testOnlyTsFileReadFailuresAreClassifiedAsStructureCorruption() {
    Assert.assertTrue(TsFileStructureCorruptionUtils.isStructureCorruption(new EOFException()));
    Assert.assertTrue(
        TsFileStructureCorruptionUtils.isStructureCorruption(
            new TsFileRuntimeException("wrapped parse failure", new EOFException())));
    Assert.assertFalse(
        TsFileStructureCorruptionUtils.isStructureCorruption(
            new TsFileRuntimeException("unrelated runtime failure")));
    Assert.assertFalse(
        TsFileStructureCorruptionUtils.isStructureCorruption(new IOException("disk I/O failure")));
    Assert.assertFalse(
        TsFileStructureCorruptionUtils.isStructureCorruption(
            new IllegalStateException(
                "LOAD business error", new IOException("permission denied"))));
  }

  @Test
  public void testDiagnosticMessageIsDetectedInCauseChain() {
    final RuntimeException failure =
        new IllegalStateException(
            "wrapped failure",
            new EOFException(PipeTransferIntegrityUtils.STRUCTURE_CORRUPTION_MARKER));

    Assert.assertTrue(TsFileStructureCorruptionUtils.hasDiagnosticMessage(failure));
    Assert.assertFalse(
        TsFileStructureCorruptionUtils.hasDiagnosticMessage(new EOFException("truncated")));
  }
}
