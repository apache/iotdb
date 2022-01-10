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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GetAlignedChunkMetadataTest {
  @Test
  public void getAlignedChunkMetadataTest() throws IOException {
    // generate aligned timeseries "d1.s1","d1.s2","d1.s3","d1.s4" and nonAligned timeseries
    // "d2.s1","d2.s2","d2.s3"
    TsFileGeneratorForTest.generateAlignedTsFile(10, 100, 30);
    String filePath = TsFileGeneratorForTest.alignedOutputDataFile;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      // query for non-exist device
      try {
        reader.getAlignedChunkMetadata("d3");
      } catch (IOException e) {
        Assert.assertEquals("Device {d3} is not in tsFileMetaData", e.getMessage());
      }

      // query for non-aligned device
      try {
        reader.getAlignedChunkMetadata("d2");
      } catch (IOException e) {
        Assert.assertEquals("Timeseries of device {d2} are not aligned", e.getMessage());
      }

      String[] expected = new String[] {"s1", "s2", "s3", "s4"};

      List<AlignedChunkMetadata> chunkMetadataList = reader.getAlignedChunkMetadata("d1");
      AlignedChunkMetadata alignedChunkMetadata = chunkMetadataList.get(0);
      Assert.assertEquals("", alignedChunkMetadata.getTimeChunkMetadata().getMeasurementUid());
      int i = 0;
      for (IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        Assert.assertEquals(expected[i], chunkMetadata.getMeasurementUid());
        i++;
      }

      Assert.assertEquals(expected.length, i);
    }
    TsFileGeneratorForTest.closeAlignedTsFile();
  }
}
