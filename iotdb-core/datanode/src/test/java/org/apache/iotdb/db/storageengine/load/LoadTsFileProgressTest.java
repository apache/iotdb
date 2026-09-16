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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class LoadTsFileProgressTest {

  @Test
  public void testTotalLengthKeepsMaximumWhenFillingEarlierHole() throws Exception {
    final File tsFile = Files.createTempFile("load-progress", ".tsfile").toFile();
    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);

    progress.activate(100L);
    progress.activate(50L);

    Assert.assertEquals(100L, progress.getTotalLength());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
  }

  @Test
  public void testRecordAndReadChunkRange() throws Exception {
    final File tsFile = Files.createTempFile("load-progress-record", ".tsfile").toFile();
    final ChunkHeader chunkHeader =
        new ChunkHeader(
            "s1", 4, TSDataType.INT32, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 1);
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1L, 1);
    statistics.update(2L, 2);
    final Chunk chunk =
        new Chunk(chunkHeader, ByteBuffer.wrap(new byte[] {0, 0, 0, 1}), null, statistics);

    final LoadTsFileProgress progress = new LoadTsFileProgress(tsFile);
    progress.recordChunk("root.sg.d1", false, 7L, 30L, true, chunk, 40L);

    Assert.assertEquals(40L, progress.getTotalLength());
    Assert.assertTrue(progress.isReady(40L));

    final LoadTsFileProgress recovered = new LoadTsFileProgress(tsFile);
    final LoadTsFileProgress.ChunkRangeRecord record = recovered.readAllRecords().get(0);
    Assert.assertEquals("root.sg.d1", record.device());
    Assert.assertEquals(7L, record.physicalStart());
    Assert.assertEquals(40L, record.physicalEnd());
    Assert.assertEquals(30L, record.chunkOffset());
    Assert.assertTrue(record.firstChunkOfGroup());

    Files.deleteIfExists(tsFile.toPath());
    Files.deleteIfExists(LoadTsFileProgress.progressFileFor(tsFile).toPath());
  }
}
