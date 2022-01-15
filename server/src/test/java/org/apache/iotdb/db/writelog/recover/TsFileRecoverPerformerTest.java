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

package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsFileRecoverPerformerTest {
  private final Logger LOGGER = LoggerFactory.getLogger(TsFileRecoverPerformerTest.class);

  @Test
  @Ignore("no need to test")
  public void testUncompletedTsFileRecoverWithoutRedoWAL() throws IOException {
    // generate a completed tsfile
    File tempTsFile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                TestConstant.BASE_OUTPUT_PATH.concat(System.currentTimeMillis() + "-1-0-0.tsfile"));
    if (!tempTsFile.getParentFile().exists()) {
      Assert.assertTrue(tempTsFile.getParentFile().mkdirs());
    }
    if (tempTsFile.exists() && !tempTsFile.delete()) {
      throw new RuntimeException("cannot delete exists file " + tempTsFile.getAbsolutePath());
    }
    Schema schema = new Schema();
    String device = "d1";
    String sensorPrefix = "s";
    int rowNum = 1_000_000;
    int sensorNum = 20;
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    for (int i = 0; i < sensorNum; ++i) {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
      measurementSchemas.add(measurementSchema);
      schema.registerTimeseries(
          new Path(device, sensorPrefix + (i + 1)),
          new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
    }

    List<Long> flushNum = new ArrayList<>();
    long lastChunkGroupPosition = -1;
    try (TsFileWriter tsFileWriter = new TsFileWriter(tempTsFile, schema)) {
      Tablet tablet = new Tablet(device, measurementSchemas);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      long timestamp = 1;
      long value = 100000L;
      for (int r = 0; r < rowNum; ++r) {
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; ++i) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
          tsFileWriter.flushAllChunkGroups();
          flushNum.add((long) (r + 1));
          lastChunkGroupPosition = tsFileWriter.getIOWriter().getPos();
        }
      }
      if (tablet.rowSize != 0) {
        tsFileWriter.write(tablet);
        tablet.reset();
        tsFileWriter.flushAllChunkGroups();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("fail to write tsfile " + tempTsFile.getAbsolutePath());
    }

    // make the tsfile uncompleted
    TsFileOutput output =
        FSFactoryProducer.getFileOutputFactory().getTsFileOutput(tempTsFile.getPath(), true);
    output.truncate(lastChunkGroupPosition);
    output.close();

    TsFileRecoverPerformer performer =
        new TsFileRecoverPerformer(null, new TsFileResource(tempTsFile), true, false);
    try {
      performer.recover(false, null, null);
    } catch (Exception e) {
    }

    try {
      Assert.assertEquals(
          (long) flushNum.get(flushNum.size() - 1), getValueCount(tempTsFile, "s1"));
    } finally {
      tempTsFile.delete();
    }
  }

  @Test
  @Ignore("no need to test")
  public void testUncompletedTsFileRecoverWithRedoWAL() throws IOException, IllegalPathException {
    // generate a completed tsfile
    File tempTsFile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                TestConstant.BASE_OUTPUT_PATH.concat(System.currentTimeMillis() + "-2-0-0.tsfile"));
    if (!tempTsFile.getParentFile().exists()) {
      Assert.assertTrue(tempTsFile.getParentFile().mkdirs());
    }
    if (tempTsFile.exists() && !tempTsFile.delete()) {
      throw new RuntimeException("cannot delete exists file " + tempTsFile.getAbsolutePath());
    }
    Schema schema = new Schema();
    String device = "d1";
    String sensorPrefix = "s";
    int rowNum = 1_000_000;
    int sensorNum = 20;
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    for (int i = 0; i < sensorNum; ++i) {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
      measurementSchemas.add(measurementSchema);
      schema.registerTimeseries(
          new Path(device, sensorPrefix + (i + 1)),
          new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
    }

    List<Long> flushNum = new ArrayList<>();
    long lastChunkGroupPosition = -1;
    try (TsFileWriter tsFileWriter = new TsFileWriter(tempTsFile, schema)) {
      Tablet tablet = new Tablet(device, measurementSchemas);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      long timestamp = 1;
      long value = 100000L;
      for (int r = 0; r < rowNum; ++r) {
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; ++i) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
          tsFileWriter.flushAllChunkGroups();
          flushNum.add((long) (r + 1));
          lastChunkGroupPosition = tsFileWriter.getIOWriter().getPos();
        }
      }
      if (tablet.rowSize != 0) {
        tsFileWriter.write(tablet);
        tablet.reset();
        tsFileWriter.flushAllChunkGroups();
      }
    } catch (Exception e) {
      throw new RuntimeException("fail to write tsfile " + tempTsFile.getAbsolutePath());
    }

    // make the tsfile uncompleted
    TsFileOutput output =
        FSFactoryProducer.getFileOutputFactory().getTsFileOutput(tempTsFile.getPath(), true);
    output.truncate(lastChunkGroupPosition);
    output.close();

    TsFileRecoverPerformer performer =
        new TsFileRecoverPerformer(null, new TsFileResource(tempTsFile), true, false);

    try {
      performer.recover(true, null, null);
    } catch (Exception e) {
    }

    try {
      Assert.assertEquals(
          (long) flushNum.get(flushNum.size() - 2), getValueCount(tempTsFile, "s1"));
    } finally {
      tempTsFile.delete();
    }
  }

  private long getValueCount(File file, String sensor) {
    long count = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              while (batchData.hasCurrent()) {
                batchData.next();
                if (header.getMeasurementID().equals(sensor)) {
                  count++;
                }
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
    } catch (Exception e) {
    }
    return count;
  }
}
