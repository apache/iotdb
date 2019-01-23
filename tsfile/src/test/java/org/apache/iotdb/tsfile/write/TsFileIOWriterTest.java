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
package org.apache.iotdb.tsfile.write;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TsFileIOWriterTest {

  private static String tsfile = "target/tsfileIOWriterTest.tsfile";
  private static String deviceId = "device1";

  @Before
  public void before() throws IOException {
    TsFileIOWriter writer = new TsFileIOWriter(new File(tsfile));

    // file schema
    MeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema();
    FileSchema fileSchema = new FileSchema();
    fileSchema.registerMeasurement(measurementSchema);

    // chunk statistics
    Statistics statistics = Statistics.getStatsByType(measurementSchema.getType());
    statistics.updateStats(0L);

    // chunk group 1
    writer.startFlushChunkGroup(deviceId);
    writer.startFlushChunk(measurementSchema, measurementSchema.getCompressor().getType(),
        measurementSchema.getType(), measurementSchema.getEncodingType(), statistics, 0, 0, 0, 0);
    writer.endChunk(0);
    ChunkGroupFooter footer = new ChunkGroupFooter(deviceId, 0, 1);
    writer.endChunkGroup(footer);

    // end file
    writer.endFile(fileSchema);
  }

  @After
  public void after() {
    File file = new File(tsfile);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void endFileTest() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(tsfile);

    // magic_string
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readHeadMagic());
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());

    // chunk header
    Assert.assertEquals(MetaMarker.CHUNK_HEADER, reader.readMarker());
    ChunkHeader header = reader.readChunkHeader();
    Assert.assertEquals(TimeSeriesMetadataTest.measurementUID, header.getMeasurementID());

    // chunk group footer
    Assert.assertEquals(MetaMarker.CHUNK_GROUP_FOOTER, reader.readMarker());
    ChunkGroupFooter footer = reader.readChunkGroupFooter();
    Assert.assertEquals(deviceId, footer.getDeviceID());

    // separator
    Assert.assertEquals(MetaMarker.SEPARATOR, reader.readMarker());

    // FileMetaData
    TsFileMetaData metaData = reader.readFileMetadata();
    MeasurementSchema actual = metaData.getMeasurementSchema()
        .get(TimeSeriesMetadataTest.measurementUID);
    Assert.assertEquals(TimeSeriesMetadataTest.measurementUID, actual.getMeasurementId());
    Assert.assertEquals(1, metaData.getDeviceMap().size());
  }
}
