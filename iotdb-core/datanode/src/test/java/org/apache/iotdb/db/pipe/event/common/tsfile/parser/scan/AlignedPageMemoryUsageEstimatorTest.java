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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan;

import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AlignedPageMemoryUsageEstimatorTest {

  @Test
  public void testIncrementalEstimatesMatchFullEstimates() throws Exception {
    final int originalMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    final File tsFile = new File("aligned-page-memory-estimator.tsfile");

    try {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2);
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }

      final List<IMeasurementSchema> schemas = new ArrayList<>();
      for (int i = 0; i < 4; ++i) {
        schemas.add(
            new MeasurementSchema(
                "s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4));
      }
      final Tablet tablet = new Tablet("root.sg.d", schemas, 8);
      for (int row = 0; row < 8; ++row) {
        tablet.addTimestamp(row, row);
        for (int measurement = 0; measurement < schemas.size(); ++measurement) {
          tablet.addValue("s" + measurement, row, (long) row + measurement);
        }
      }

      try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
        writer.registerAlignedTimeseries(new PartialPath("root.sg.d"), schemas);
        writer.writeAligned(tablet);
      }

      try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
        final IDeviceID deviceID = reader.getDeviceMeasurementsMap().keySet().iterator().next();
        final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, true);
        Assert.assertEquals(1, alignedChunkMetadataList.size());
        final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
        final Chunk timeChunk =
            reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
        Assert.assertEquals(MetaMarker.CHUNK_HEADER, timeChunk.getHeader().getChunkType() & 0x3F);

        final List<Chunk> prefix = new ArrayList<>();
        final AlignedSinglePageWholeChunkReader.PageMemoryUsageEstimator estimator =
            new AlignedSinglePageWholeChunkReader.PageMemoryUsageEstimator(timeChunk);
        for (final IChunkMetadata valueChunkMetadata :
            alignedChunkMetadata.getValueChunkMetadataList()) {
          final Chunk valueChunk = reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
          Assert.assertEquals(
              MetaMarker.CHUNK_HEADER, valueChunk.getHeader().getChunkType() & 0x3F);

          final long expectedWithCandidate =
              AlignedSinglePageWholeChunkReader
                  .calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(
                      timeChunk, appendAndCopy(prefix, valueChunk));
          Assert.assertEquals(
              expectedWithCandidate,
              estimator.calculateMaxPageEstimatedMemoryUsageInBytesWithValueChunk(valueChunk));

          estimator.addValueChunk(valueChunk);
          prefix.add(valueChunk);
          final List<Long> expectedPageMemoryUsage =
              AlignedSinglePageWholeChunkReader
                  .calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(timeChunk, prefix);
          Assert.assertEquals(
              expectedPageMemoryUsage, estimator.getPageEstimatedMemoryUsageInBytesList());
        }
      }
    } finally {
      TSFileDescriptor.getInstance()
          .getConfig()
          .setMaxNumberOfPointsInPage(originalMaxNumberOfPointsInPage);
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }
  }

  private List<Chunk> appendAndCopy(final List<Chunk> chunks, final Chunk chunk) {
    final List<Chunk> copy = new ArrayList<>(chunks);
    copy.add(chunk);
    return copy;
  }
}
