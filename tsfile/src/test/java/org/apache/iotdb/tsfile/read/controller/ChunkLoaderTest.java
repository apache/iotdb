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
package org.apache.iotdb.tsfile.read.controller;

import java.io.IOException;
import java.util.List;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.read.reader.ReaderTest;
import org.apache.iotdb.tsfile.utils.BaseTsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;

public class ChunkLoaderTest extends BaseTsFileGeneratorForTest {

  private static final String FILE_PATH = TestConstant.BASE_OUTPUT_PATH.concat("testChunkLoaderTest.tsfile");
  private TsFileSequenceReader fileReader;

  @Override
  public void initParameter() {
    chunkGroupSize = 1024 * 1024;
    pageSize = 10000;
    inputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perChunkLoaderTest");
    outputDataFile = ChunkLoaderTest.FILE_PATH;
    errorOutputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perChunkLoaderTest.tsfile");
  }

  @Before
  public void before() throws IOException {
    generateFile(1000000, 1000000, 1000000);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    closeAndDelete();
  }

  @Test
  public void test() throws IOException {
    fileReader = new TsFileSequenceReader(FILE_PATH);
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    List<ChunkMetadata> chunkMetadataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d2", "s1"));

    CachedChunkLoaderImpl seriesChunkLoader = new CachedChunkLoaderImpl(fileReader);
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = seriesChunkLoader.loadChunk(chunkMetaData);
      ChunkHeader chunkHeader = chunk.getHeader();
      Assert.assertEquals(chunkHeader.getDataSize(), chunk.getData().remaining());
    }
  }
}
