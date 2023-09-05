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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class ChunkReaderTest {
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
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(100);
    if (!SEQ_DIRS.exists()) {
      Assert.assertTrue(SEQ_DIRS.mkdirs());
    }
    String fileName =
        System.currentTimeMillis() + FilePathUtils.FILE_NAME_SEPARATOR + "0-0-0.tsfile";
    String filePath = SEQ_DIRS.getPath() + File.separator + fileName;
    file =
        TsFileGeneratorUtils.generateNonAlignedTsFile(
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
  public void testAccelerateQueryByTimestamp() throws IOException {
    try (TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader(file.getPath())) {
      for (int i = 0; i < deviceNum; i++) {
        for (int j = 0; j < measurementNum; j++) {
          List<ChunkMetadata> chunkMetadataList =
              tsFileSequenceReader.getChunkMetadataList(
                  new Path(testStorageGroup + PATH_SEPARATOR + "d" + i, "s" + j, true));
          for (ChunkMetadata chunkMetadata : chunkMetadataList) {
            Chunk chunk = tsFileSequenceReader.readMemChunk(chunkMetadata);
            ChunkReader chunkReader = new ChunkReader(chunk, null);
            chunk = tsFileSequenceReader.readMemChunk(chunkMetadata);
            ChunkReader chunkReaderByTimestamp = new ChunkReader(chunk, null, 301);
            Assert.assertEquals(5, chunkReader.loadPageReaderList().size());
            Assert.assertEquals(2, chunkReaderByTimestamp.loadPageReaderList().size());
          }
        }
      }
    }
  }
}
