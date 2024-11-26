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
package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tsfile.tableview.TableViewTest.genTableSchema;
import static org.apache.tsfile.tableview.TableViewTest.testDir;
import static org.apache.tsfile.tableview.TableViewTest.writeTsFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TableChunkReaderTest {

  @Before
  public void setUp() throws Exception {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  @Test
  public void testWithTimeDeletion() throws IOException, WriteProcessException {
    TableSchema tableSchema = genTableSchema(0);
    final File testFile = new File(testDir, "testFile");
    writeTsFile(tableSchema, testFile);

    CachedChunkLoaderImpl chunkLoader;
    List<IChunkMetadata> chunkMetadataList;
    try (TsFileSequenceReader sequenceReader =
        new TsFileSequenceReader(testFile.getAbsolutePath())) {
      chunkLoader = new CachedChunkLoaderImpl(sequenceReader);

      chunkMetadataList =
          sequenceReader.getIChunkMetadataList(
              Factory.DEFAULT_FACTORY.create(
                  new String[] {tableSchema.getTableName(), "0", "0", "0", "0", "0"}),
              "");
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetadataList.get(0);

      Chunk timeChunk =
          chunkLoader.loadChunk(((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata()));
      List<Chunk> valueChunks =
          alignedChunkMetadata.getValueChunkMetadataList().stream()
              .map(c -> (ChunkMetadata) c)
              .map(
                  c -> {
                    try {
                      return chunkLoader.loadChunk(c);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toList());

      // [0, 9] -> [5, 9]
      timeChunk.setDeleteIntervalList(Collections.singletonList(new TimeRange(0, 4)));

      TableChunkReader tableChunkReader = new TableChunkReader(timeChunk, valueChunks, null);
      BatchData batchData = tableChunkReader.nextPageData();
      for (int i = 5; i < 10; i++) {
        assertEquals(i, batchData.currentTime());
        batchData.next();
      }
      assertFalse(batchData.hasCurrent());
    }
  }
}
