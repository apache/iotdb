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
package org.apache.tsfile.encrypt;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AES128TsFileReadWriteTest {
  private final double delta = 0.0000001;
  private final String path = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
  private File f;
  private final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("device_1");

  private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

  @Before
  public void setUp() {
    conf.setEncryptFlag("true");
    conf.setEncryptType("org.apache.tsfile.encrypt.AES128");
    conf.setEncryptKey("thisisourtestkey");
    f = new File(path);
    if (f.exists()) {
      assertTrue(f.delete());
    }
    if (!f.getParentFile().exists()) {
      assertTrue(f.getParentFile().mkdirs());
    }
  }

  @After
  public void tearDown() {
    conf.setEncryptKey("abcdefghijklmnop");
    conf.setEncryptType("org.apache.tsfile.encrypt.UNENCRYPTED");
    conf.setEncryptFlag("false");
    f = new File(path);
    if (f.exists()) {
      assertTrue(f.delete());
    }
  }

  @Test
  public void intTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.REGULAR,
            TSEncoding.GORILLA,
            TSEncoding.ZIGZAG);
    for (TSEncoding encoding : encodings) {
      intTest(encoding);
    }
  }

  private void intTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(TSDataType.INT32, (i) -> new IntDataPoint("sensor_1", (int) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getIntV()));
  }

  @Test
  public void longTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.REGULAR,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      longTest(encoding);
    }
  }

  public void longTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(TSDataType.INT64, (i) -> new LongDataPoint("sensor_1", i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getLongV()));
  }

  @Test
  public void floatTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.GORILLA_V1,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      floatTest(encoding);
    }
  }

  public void floatTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(
        TSDataType.FLOAT, (i) -> new FloatDataPoint("sensor_1", (float) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getFloatV(), delta));
  }

  @Test
  public void doubleTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.GORILLA_V1,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      doubleTest(encoding);
    }
  }

  public void doubleTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(
        TSDataType.DOUBLE, (i) -> new DoubleDataPoint("sensor_1", (double) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getDoubleV(), delta));
  }

  // If no dataPoint in "device_1.sensor_2", it will throws a nomeasurement
  // exception,
  // cause no schema in tsfilemetadata anymore.
  @Test
  public void readEmptyMeasurementTest() throws IOException, WriteProcessException {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f, new Schema(), conf)) {
      // add measurements into file schema
      tsFileWriter.registerTimeseries(
          new Path(deviceID), new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
      tsFileWriter.registerTimeseries(
          new Path(deviceID),
          new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
      // construct TSRecord
      TSRecord tsRecord = new TSRecord(deviceID, 1);
      DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
      tsRecord.addTuple(dPoint1);
      // write a TSRecord to TsFile
      tsFileWriter.writeRecord(tsRecord);
    }

    // read example : no filter
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    TsFileReader readTsFile = new TsFileReader(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path(deviceID, "sensor_2", true));
    QueryExpression queryExpression = QueryExpression.create(paths, null);
    try {
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    } catch (IOException e) {
      // Assert.fail();
    } finally {
      reader.close();
    }

    assertTrue(f.delete());
  }

  @Test
  public void readMeasurementWithRegularEncodingTest() throws IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("REGULAR");
    writeDataByTSRecord(
        TSDataType.INT64, (i) -> new LongDataPoint("sensor_1", i), TSEncoding.REGULAR);
    readData((i, field, delta) -> assertEquals(i, field.getLongV()));
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
  }

  private void writeDataByTSRecord(
      TSDataType dataType, AES128TsFileReadWriteTest.DataPointProxy proxy, TSEncoding encodingType)
      throws IOException, WriteProcessException {
    int floatCount = 1024 * 1024 * 13 + 1023;
    // add measurements into file schema
    try (TsFileWriter tsFileWriter = new TsFileWriter(f, new Schema(), conf)) {
      tsFileWriter.registerTimeseries(
          new Path(deviceID), new MeasurementSchema("sensor_1", dataType, encodingType));
      for (long i = 1; i < floatCount; i++) {
        // construct TSRecord
        TSRecord tsRecord = new TSRecord(deviceID, i);
        DataPoint dPoint1 = proxy.generateOne(i);
        tsRecord.addTuple(dPoint1);
        // write a TSRecord to TsFile
        tsFileWriter.writeRecord(tsRecord);
      }
    }
  }

  private void readData(AES128TsFileReadWriteTest.ReadDataPointProxy proxy) throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    TsFileReader readTsFile = new TsFileReader(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path(deviceID, "sensor_1", true));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    for (int j = 0; j < paths.size(); j++) {
      assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
    }
    int i = 1;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      assertEquals(i, r.getTimestamp());
      proxy.assertEqualProxy(i, r.getFields().get(0), delta);
      i++;
    }
    reader.close();
  }

  private interface DataPointProxy {

    DataPoint generateOne(long value);
  }

  private interface ReadDataPointProxy {

    void assertEqualProxy(long i, Field field, double delta);
  }
}
