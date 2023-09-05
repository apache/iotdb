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

package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TsFileWriterTest {
  TsFileWriter writer = null;
  String fileName = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
  boolean closed = false;

  @Before
  public void setUp() {
    try {
      File f = new File(fileName);
      if (!f.getParentFile().exists()) {
        Assert.assertTrue(f.getParentFile().mkdirs());
      }
      writer = new TsFileWriter(f);
      registerTimeseries();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    if (!closed) {
      closeFile();
    }
    try {
      Files.deleteIfExists(new File(fileName).toPath());
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void registerTimeseries() {
    // register nonAligned timeseries "d1.s1","d1.s2","d1.s3"
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals("given nonAligned timeseries d1.s1 has been registered.", e.getMessage());
    }
    try {
      List<MeasurementSchema> schemas = new ArrayList<>();
      schemas.add(
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      writer.registerAlignedTimeseries(new Path("d1"), schemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d1 has been registered for nonAligned timeseries.", e.getMessage());
    }
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(
        new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    schemas.add(
        new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    writer.registerTimeseries(new Path("d1"), schemas);

    // Register aligned timeseries "d2.s1" , "d2.s2", "d2.s3"
    try {
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new MeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries and should not be expanded.",
          e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d2"),
          new MeasurementSchema("s5", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries.", e.getMessage());
    }

    /*try {
      for (int i = 2; i < 3; i++) {
        writer.registerTimeseries(
            new Path("d" + i, "s1"),
            new MeasurementSchema(
                "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }*/
  }

  @Test
  public void registerTimeseriesTest() {
    closeFile();
    readNothing();
  }

  private void writeTSRecord() throws IOException, WriteProcessException {
    // normal
    TSRecord record = new TSRecord(10000, "d1");
    record.addTuple(new FloatDataPoint("s1", 5.0f));
    record.addTuple(new IntDataPoint("s2", 5));
    writer.write(record);

    // not existed time series
    record = new TSRecord(10001, "d1");
    record.addTuple(new FloatDataPoint("s4", 5));
    try {
      writer.write(record);
    } catch (WriteProcessException e) {
      assertTrue(e instanceof NoMeasurementException);
    }

    // not existed time series
    record = new TSRecord(10001, "d1");
    record.addTuple(new FloatDataPoint("s3", 5));
    try {
      writer.write(record);
    } catch (TsFileEncodingException e) {
      // do nothing
    }
  }

  @Test
  public void writeTSRecordTest() throws IOException, WriteProcessException {
    writeTSRecord();
    closeFile();
    readOneRow();
  }

  @Test
  public void writeIncorrectTSRecord0() throws IOException {
    // incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.write(record);
    } catch (WriteProcessException e) {
      Assert.assertEquals("no nonAligned timeseries is registered in the group.", e.getMessage());
    }
    closeFile();
    readNothing();
  }

  @Test
  public void writeIncorrectTSRecords() throws IOException, WriteProcessException {
    // incorrect data type
    for (int i = 2; i < 3; i++) {
      TSRecord record = new TSRecord(10000 + i, "d" + i);
      record.addTuple(new IntDataPoint("s1", 5));
      try {
        writer.writeAligned(record);
      } catch (UnsupportedOperationException e) {
        // do nothing
      }
    }
    closeFile();
    readNothing();
  }

  @Test
  public void writeIncorrectTSRecord() throws IOException, WriteProcessException {
    writeTSRecord();
    // incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.writeAligned(record);
    } catch (UnsupportedOperationException e) {
      // do nothing
    }
    closeFile();
    readOneRow();
  }

  @Test
  public void writeTablet() throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            "d1",
            Arrays.asList(
                new MeasurementSchema(
                    "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
                new MeasurementSchema(
                    "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)));
    tablet.timestamps[0] = 10000;
    ((float[]) tablet.values[0])[0] = 5.0f;
    ((int[]) tablet.values[1])[0] = 5;
    tablet.rowSize = 1;
    writer.write(tablet);
    closeFile();
    readOneRow();
  }

  @Test
  public void writeTablet2() throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            "d1",
            Arrays.asList(
                new MeasurementSchema(
                    "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
                new MeasurementSchema(
                    "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)));
    tablet.timestamps[0] = 10000;
    ((float[]) tablet.values[0])[0] = 5.0f;
    tablet.rowSize = 1;
    writer.write(tablet);
    closeFile();
    // in this case, the value of s2 = 0 at time 10000.
    readOneRow(0);
  }

  @Test
  public void getIOWriter() {
    // The interface is just for test
    writer.getIOWriter();
    closeFile();
    readNothing();
  }

  @Test
  public void flushForTest() throws IOException {
    // The interface is just for test
    writer.flushAllChunkGroups();
    closeFile();
    readNothing();
  }

  @Test
  public void flushForTestWithVersion() throws IOException {
    // The interface is just for test
    writer.flushAllChunkGroups();
    closeFile();
    readNothing();
  }

  private void closeFile() {
    try {
      closed = true;
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readNothing() {
    // using TsFileReader for test
    try {
      TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(fileName));
      QueryDataSet dataSet =
          tsFileReader.query(
              QueryExpression.create()
                  .addSelectedPath(new Path("d1", "s1", true))
                  .addSelectedPath(new Path("d1", "s2", true)));
      assertFalse(dataSet.hasNext());
      tsFileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void readOneRow() {
    readOneRow(5);
  }

  private void readOneRow(int s2Value) {
    try {
      TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(fileName));
      QueryDataSet dataSet =
          tsFileReader.query(
              QueryExpression.create()
                  .addSelectedPath(new Path("d1", "s1", true))
                  .addSelectedPath(new Path("d1", "s2", true))
                  .addSelectedPath(new Path("d1", "s3", true)));
      while (dataSet.hasNext()) {
        RowRecord result = dataSet.next();
        assertEquals(2, result.getFields().size());
        assertEquals(10000, result.getTimestamp());
        assertEquals(5.0f, result.getFields().get(0).getFloatV(), 0.00001);
        assertEquals(s2Value, result.getFields().get(1).getIntV());
      }
      tsFileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
