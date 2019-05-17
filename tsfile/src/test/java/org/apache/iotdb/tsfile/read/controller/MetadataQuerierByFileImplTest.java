/**
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
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataQuerierByFileImplTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader reader;
  private ArrayList<TimeRange> d1s6timeRangeList = new ArrayList<>();
  private ArrayList<TimeRange> d2s1timeRangeList = new ArrayList<>();
  private ArrayList<long[]> d1chunkGroupMetaDataOffsetList = new ArrayList<>();
  private ArrayList<long[]> d2chunkGroupMetaDataOffsetList = new ArrayList<>();

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    reader = new TsFileSequenceReader(FILE_PATH);

    // Because the size of the generated chunkGroupMetaData may differ under different test environments,
    // we get metadata from the real-time generated TsFile instead of using a fixed parameter setting.
    TsFileMetaData metaData = reader.readFileMetadata();
    TsDeviceMetadataIndex d1MetadataIndex = metaData.getDeviceMap().get("d1");
    TsDeviceMetadataIndex d2MetadataIndex = metaData.getDeviceMap().get("d2");

    TsDeviceMetadata d1Metadata = reader.readTsDeviceMetaData(d1MetadataIndex);
    List<ChunkGroupMetaData> d1chunkGroupMetaDataList = d1Metadata.getChunkGroupMetaDataList();
    for (ChunkGroupMetaData chunkGroupMetaData : d1chunkGroupMetaDataList) {
      // get a series of [startOffsetOfChunkGroup, endOffsetOfChunkGroup] from the chunkGroupMetaData of d1
      long[] chunkGroupMetaDataOffset = new long[2];
      chunkGroupMetaDataOffset[0] = chunkGroupMetaData.getStartOffsetOfChunkGroup();
      chunkGroupMetaDataOffset[1] = chunkGroupMetaData.getEndOffsetOfChunkGroup();
      d1chunkGroupMetaDataOffsetList.add(chunkGroupMetaDataOffset);

      List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData.getChunkMetaDataList();
      for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
        if (chunkMetaData.getMeasurementUid().equals("s6")) {
          // get a series of [startTime, endTime] of d1.s6 from the chunkGroupMetaData of d1
          d1s6timeRangeList
              .add(new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
        }
      }
    }

    TsDeviceMetadata d2Metadata = reader.readTsDeviceMetaData(d2MetadataIndex);
    List<ChunkGroupMetaData> d2chunkGroupMetaDataList = d2Metadata.getChunkGroupMetaDataList();
    for (ChunkGroupMetaData chunkGroupMetaData : d2chunkGroupMetaDataList) {
      // get a series of [startOffsetOfChunkGroup, endOffsetOfChunkGroup] from the chunkGroupMetaData of d2
      long[] chunkGroupMetaDataOffset = new long[2];
      chunkGroupMetaDataOffset[0] = chunkGroupMetaData.getStartOffsetOfChunkGroup();
      chunkGroupMetaDataOffset[1] = chunkGroupMetaData.getEndOffsetOfChunkGroup();
      d2chunkGroupMetaDataOffsetList.add(chunkGroupMetaDataOffset);

      List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData.getChunkMetaDataList();
      for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
        if (chunkMetaData.getMeasurementUid().equals("s1")) {
          // get a series of [startTime, endTime] of d2.s1 from the chunkGroupMetaData of d1
          d2s1timeRangeList
              .add(new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
        }
      }
    }
  }

  @After
  public void after() throws IOException {
    reader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testEmpty() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));

    ArrayList<TimeRange> resTimeRanges = new ArrayList<>(metadataQuerierByFile
        .convertSpace2TimePartition(paths, 0L, 0L));

    Assert.assertEquals(0, resTimeRanges.size());
  }

  @Test
  public void testConvert1() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));

    long spacePartitionStartPos = d1chunkGroupMetaDataOffsetList.get(0)[0];
    long spacePartitionEndPos = d1chunkGroupMetaDataOffsetList.get(1)[1];
    ArrayList<TimeRange> resTimeRanges = new ArrayList<>(metadataQuerierByFile
        .convertSpace2TimePartition(paths, spacePartitionStartPos, spacePartitionEndPos));

    ArrayList<TimeRange> unionCandidates = new ArrayList<>();
    unionCandidates.add(d1s6timeRangeList.get(0));
    unionCandidates.add(d2s1timeRangeList.get(0));
    unionCandidates.add(d1s6timeRangeList.get(1));
    ArrayList<TimeRange> expectedRanges = new ArrayList<>(TimeRange.sortAndMerge(unionCandidates));

    Assert.assertEquals(expectedRanges.toString(), resTimeRanges.toString());
  }

  @Test
  public void testConvert2() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));

    long spacePartitionStartPos = d2chunkGroupMetaDataOffsetList.get(0)[0];
    long spacePartitionEndPos = d2chunkGroupMetaDataOffsetList.get(0)[1];
    ArrayList<TimeRange> resTimeRanges = new ArrayList<>(metadataQuerierByFile
        .convertSpace2TimePartition(paths, spacePartitionStartPos, spacePartitionEndPos));

    ArrayList<TimeRange> inCandidates = new ArrayList<>();
    ArrayList<TimeRange> beforeCandidates = new ArrayList<>();
    inCandidates.add(d2s1timeRangeList.get(0));
    beforeCandidates.add(d1s6timeRangeList.get(0));
    ArrayList<TimeRange> expectedRanges = new ArrayList<>();
    for (TimeRange in : inCandidates) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(beforeCandidates));
      expectedRanges.addAll(remains);
    }

    Assert.assertEquals(expectedRanges.toString(), resTimeRanges.toString());
  }
}
