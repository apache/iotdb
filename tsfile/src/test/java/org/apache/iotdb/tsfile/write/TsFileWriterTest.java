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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TsFileWriterTest {
  TsFileWriter writer = null;
  long fileName = System.nanoTime();
  boolean closed = false;
  @Before
  public void setUp() {
    try {
      writer = new TsFileWriter(new File("target/tsfileWriter-" + fileName));
      addMeasurement();
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
      Files.deleteIfExists(new File("target/tsfileWriter-" + fileName).toPath());
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void addMeasurement() {
    try {
      //String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.addTimeseries(new Path("d1.s1"),
          new TimeseriesSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      //String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.addTimeseries(new Path("d1.s1"),
          new TimeseriesSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals("given timeseries has exists! d1.s1", e.getMessage());
    }
    try {
      //String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.addTimeseries(new Path("d1.s2"),
          new TimeseriesSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      for(int i = 2; i < 3; i++) {
        writer.addTimeseries(new Path("d"+ i + ".s1"),
            new TimeseriesSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  @Test
  public void addMeasurementTest() {
    closeFile();
    readNothing();
  }


  private void writeTSRecord() throws IOException, WriteProcessException {
    //normal
    TSRecord record = new TSRecord( 10000, "d1");
    record.addTuple(new FloatDataPoint("s1", 5.0f));
    record.addTuple(new IntDataPoint("s2", 5));
    writer.write(record);

    //not existed time series
    record = new TSRecord( 10001, "d1");
    record.addTuple(new FloatDataPoint("s3", 5.0f));
    try {
      writer.write(record);
    } catch (WriteProcessException e) {
      assertTrue(e instanceof NoMeasurementException);
    }
  }

  @Test
  public void writeTSRecordTest() throws IOException, WriteProcessException {
    writeTSRecord();
    closeFile();
    readOneRow();
  }

  @Test
  public void writeIncorrectTSRecord0() throws IOException, WriteProcessException {
    //incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.write(record);
    } catch (TsFileEncodingException e) {
      //do nothing
    }
    closeFile();
    readNothing();
  }
  @Test
  public void writeIncorrectTSRecords() throws IOException, WriteProcessException {
    //incorrect data type
    for(int i = 2; i < 3; i++) {
      TSRecord record = new TSRecord(10000 + i, "d"+i);
      record.addTuple(new IntDataPoint("s1", 5));
      try {
        writer.write(record);
      } catch (TsFileEncodingException e) {
        //do nothing
      }
    }
    closeFile();
    readNothing();
  }

  @Test
  public void writeIncorrectTSRecord() throws IOException, WriteProcessException {
    writeTSRecord();
    //incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.write(record);
    } catch (TsFileEncodingException e) {
      //do nothing
    }
    closeFile();
    readOneRow();
  }

  @Test
  public void writeRowBatch() throws IOException, WriteProcessException {
    RowBatch rowBatch = new RowBatch("d1", Arrays.asList(new TimeseriesSchema[]{
        new TimeseriesSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
        new TimeseriesSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)
    }));
    rowBatch.timestamps[0] = 10000;
    ((float[])rowBatch.values[0])[0] = 5.0f;
    ((int[])rowBatch.values[1])[0] = 5;
    rowBatch.batchSize = 1;
    writer.write(rowBatch);
    closeFile();
    readOneRow();
  }

  @Test
  public void writeRowBatch2() throws IOException, WriteProcessException {
    RowBatch rowBatch = new RowBatch("d1", Arrays.asList(new TimeseriesSchema[]{
        new TimeseriesSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
        new TimeseriesSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)
    }));
    rowBatch.timestamps[0] = 10000;
    ((float[])rowBatch.values[0])[0] = 5.0f;
    rowBatch.batchSize = 1;
    writer.write(rowBatch);
    closeFile();
    //in this case, the value of s2 = 0 at time 10000.
    readOneRow(0);
  }

  @Test
  public void getIOWriter() throws IOException {
    //The interface is just for test
    writer.getIOWriter();
    closeFile();
    readNothing();
  }

  @Test
  public void flushForTest() throws IOException {
    //The interface is just for test
    writer.flushForTest();
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
    //using TsFileReader for test
    try {
      ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(
          new TsFileSequenceReader("target/tsfileWriter-" + fileName));
      QueryDataSet dataSet = readOnlyTsFile.query(QueryExpression.create()
          .addSelectedPath(new Path("d1.s1"))
          .addSelectedPath(new Path("d1.s2")));
      assertFalse(dataSet.hasNext());
      readOnlyTsFile.close();
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
      ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(
          new TsFileSequenceReader("target/tsfileWriter-" + fileName));
      QueryDataSet dataSet = readOnlyTsFile.query(QueryExpression.create()
          .addSelectedPath(new Path("d1.s1"))
          .addSelectedPath(new Path("d1.s2"))
          .addSelectedPath(new Path("d1.s3")));
      while(dataSet.hasNext()) {
        RowRecord result = dataSet.next();
        assertEquals(2, result.getFields().size());
        assertEquals(10000, result.getTimestamp());
        assertEquals(5.0f, result.getFields().get(0).getFloatV(), 0.00001);
        assertEquals(s2Value, result.getFields().get(1).getIntV());
      }
      readOnlyTsFile.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
