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

package org.apache.tsfile.read.reader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReaderWithoutStatistics;
import org.apache.tsfile.utils.FilePathUtils;
import org.apache.tsfile.utils.TsFileGeneratorUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class AlignedChunkReaderWithoutStatisticsTest {
  private final String testStorageGroup = TsFileGeneratorUtils.testStorageGroup;
  private final File SEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "sequence"
              + File.separator
              + testStorageGroup
              + File.separator
              + "0"
              + File.separator
              + "0");

  private File file;
  private final int oldMaxPointNumInPage =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
  private final int deviceNum = 5;
  private final int measurementNum = 10;

  @Before
  public void setUp() throws IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(500);
    if (!SEQ_DIRS.exists()) {
      Assert.assertTrue(SEQ_DIRS.mkdirs());
    }
    String fileName =
        System.currentTimeMillis() + FilePathUtils.FILE_NAME_SEPARATOR + "0-0-0.tsfile";
    String filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    file =
        TsFileGeneratorUtils.generateAlignedTsFile(
            filePath, deviceNum, measurementNum, 500, 0, 0, 0, 0);
  }

  @After
  public void tearDown() {
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldMaxPointNumInPage);
    if (file.exists()) {
      file.delete();
    }
    if (SEQ_DIRS.exists()) {
      SEQ_DIRS.delete();
    }
  }

  @Test
  public void testChunkReaderWithoutStatistics() throws IOException {
    try (final TsFileSequenceReader tsFileSequenceReader =
        new TsFileSequenceReader(file.getPath())) {
      for (int i = 0; i < deviceNum; i++) {
        final List<AlignedChunkMetadata> chunkMetadataList =
            tsFileSequenceReader.getAlignedChunkMetadata(
                Factory.DEFAULT_FACTORY.create(testStorageGroup + PATH_SEPARATOR + "d1000" + i),
                true);
        for (final AlignedChunkMetadata chunkMetadata : chunkMetadataList) {
          Chunk timeChunk =
              tsFileSequenceReader.readMemChunk(
                  (ChunkMetadata) chunkMetadata.getTimeChunkMetadata());
          timeChunk = new Chunk(timeChunk.getHeader(), timeChunk.getData());
          final List<Chunk> valueChunkList = new ArrayList<>();
          for (final IChunkMetadata valueChunkMetadata :
              chunkMetadata.getValueChunkMetadataList()) {
            final Chunk valueChunk =
                tsFileSequenceReader.readMemChunk((ChunkMetadata) valueChunkMetadata);
            valueChunkList.add(new Chunk(valueChunk.getHeader(), valueChunk.getData()));
          }
          final AlignedChunkReader chunkReader =
              new AlignedChunkReaderWithoutStatistics(timeChunk, valueChunkList);
          Assert.assertEquals(1, chunkReader.loadPageReaderList().size());
        }
      }
    }
  }
}
