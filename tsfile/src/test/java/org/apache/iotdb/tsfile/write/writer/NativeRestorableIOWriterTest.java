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

package org.apache.iotdb.tsfile.write.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

@SuppressWarnings("squid:S4042") // Suppress use java.nio.Files#delete warning
public class NativeRestorableIOWriterTest {

  private static final String FILE_NAME = "test.ts";

  @Test(expected = IOException.class)
  public void testBadHeadMagic() throws Exception {
    File file = new File(FILE_NAME);
    FileWriter fWriter = new FileWriter(file);
    fWriter.write("Tsfile");
    fWriter.close();
    try {
      NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testOnlyHeadMagic() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter().forceClose();

    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();

    rWriter = new NativeRestorableIOWriter(file);
    assertEquals(TsFileCheckStatus.COMPLETE_FILE, rWriter.getTruncatedPosition());
    assertFalse(rWriter.canWrite());
    rWriter = new NativeRestorableIOWriter(file, true);
    assertEquals(TSFileConfig.MAGIC_STRING.length(), rWriter.getTruncatedPosition());
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertTrue(file.delete());
  }


  @Test
  public void testOnlyFirstMask() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    //we have to flush using inner API.
    writer.getIOWriter().out.write(new byte[] {MetaMarker.CHUNK_HEADER});
    writer.getIOWriter().forceClose();
    assertEquals(TsFileIOWriter.magicStringBytes.length + 1, file.length());
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneIncompleteChunkHeader() throws Exception {
    File file = new File(FILE_NAME);

    IncompleteFileTestUtil.writeFileWithOneIncompleteChunkHeader(file);

    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    TsFileWriter writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkHeader() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter()
        .startFlushChunk(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.PLAIN),
            CompressionType.SNAPPY, TSDataType.FLOAT,
            TSEncoding.PLAIN, new FloatStatistics(), 100, 50, 100, 10);
    writer.getIOWriter().forceClose();

    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkHeaderAndSomePage() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    long pos = writer.getIOWriter().getPos();
    //let's delete one byte.
    writer.getIOWriter().out.truncate(pos - 1);
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    assertTrue(file.delete());
  }


  @Test
  public void testOnlyOneChunkGroup() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkGroupAndOneMask() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    writer.getIOWriter().writeChunkMaskForTest();
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertNotEquals(TsFileIOWriter.magicStringBytes.length, rWriter.getTruncatedPosition());
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsDeviceMetadataIndex index = reader.readFileMetadata().getDeviceMap().get("d1");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    reader.close();
    assertTrue(file.delete());
  }


  @Test
  public void testTwoChunkGroupAndMore() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(new TSRecord(1, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsDeviceMetadataIndex index = reader.readFileMetadata().getDeviceMap().get("d1");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testNoSeperatorMask() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(new TSRecord(1, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsDeviceMetadataIndex index = reader.readFileMetadata().getDeviceMap().get("d1");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    index = reader.readFileMetadata().getDeviceMap().get("d2");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    reader.close();
    assertTrue(file.delete());
  }


  @Test
  public void testHavingSomeFileMetadata() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(new TSRecord(1, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d2").addTuple(new FloatDataPoint("s1", 6))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().forceClose();
    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsDeviceMetadataIndex index = reader.readFileMetadata().getDeviceMap().get("d1");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    index = reader.readFileMetadata().getDeviceMap().get("d2");
    assertEquals(1, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testOpenCompleteFile() throws Exception {
    File file = new File(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.addMeasurement(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.addMeasurement(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.close();

    NativeRestorableIOWriter rWriter = new NativeRestorableIOWriter(file);
    assertFalse(rWriter.canWrite());
    rWriter.forceClose();

    rWriter = new NativeRestorableIOWriter(file, true);
    assertTrue(rWriter.canWrite());
    writer = new TsFileWriter(rWriter);
    writer.write(new TSRecord(3, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(4, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_NAME);
    TsDeviceMetadataIndex index = reader.readFileMetadata().getDeviceMap().get("d1");
    assertEquals(2, reader.readTsDeviceMetaData(index).getChunkGroupMetaDataList().size());
    reader.close();
    assertTrue(file.delete());
  }
}