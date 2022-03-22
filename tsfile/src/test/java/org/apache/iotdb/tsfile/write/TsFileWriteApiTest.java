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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileWriteApiTest {
  private final File f = FSFactoryProducer.getFSFactory().getFile("TsFileWriteTest.tsfile");
  private final String deviceId = "root.sg.d1";
  private final List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
  private final List<MeasurementSchema> measurementSchemas = new ArrayList<>();
  private int oldChunkGroupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
  private int oldMaxNumOfPointsInPage =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  @Before
  public void setUp() {
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
  }

  @After
  public void end() {
    if (f.exists()) f.delete();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldMaxNumOfPointsInPage);
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(oldChunkGroupSize);
  }

  private void setEnv(int chunkGroupSize, int pageSize) {
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
  }

  public void registerAlignedTimeseries(TsFileWriter tsFileWriter) throws WriteProcessException {
    alignedMeasurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));
    alignedMeasurementSchemas.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.RLE));

    // register align timeseries
    tsFileWriter.registerAlignedTimeseries(new Path(deviceId), alignedMeasurementSchemas);
  }

  public void registerTimeseries(TsFileWriter tsFileWriter) {
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.PLAIN));

    // register nonAlign timeseries
    tsFileWriter.registerTimeseries(new Path(deviceId), measurementSchemas);
  }

  @Test
  public void writeWithTsRecord() throws IOException, WriteProcessException {
    setEnv(100 * 1024 * 1024, 10 * 1024);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10000, 0, 0, false);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeMeasurementScheams.add(measurementSchemas.get(0));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10000, 10000, 100, false);

      // example 3 : late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 20000, 200000, false);
    }
  }

  @Test
  public void writeAlignedWithTsRecord() throws IOException, WriteProcessException {
    setEnv(100 * 1024 * 1024, 10 * 1024);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 8, 0, 0, true);

      // example2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 20, 1000, 500, true);

      // example3 : late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      TsFileGeneratorUtils.writeWithTsRecord(
          tsFileWriter, deviceId, writeMeasurementScheams, 20, 300000, 50, true);
    }
  }

  @Test
  public void writeWithTablet() throws IOException, WriteProcessException {
    setEnv(100 * 1024 * 1024, 10 * 1024);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 0, 0, false);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 2000, 0, false);

      // example 3: late data
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 3111, 0, false);
    }
  }

  @Test
  public void writeAlignedWithTablet() throws IOException, WriteProcessException {
    setEnv(100 * 1024 * 1024, 10 * 1024);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 0, 0, true);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 200000, 10, 0, true);

      // example 3
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 10, 210000, 0, true);
    }
  }

  @Test
  public void writeNewAlignedMeasurementAfterFlushChunkGroup1() {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 100000, 0, 0, true);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(2));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(3));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 20, 1000000, 0, true);

    } catch (IOException | WriteProcessException e) {
      Assert.assertEquals(
          "TsFile has flushed chunk group and should not add new measurement s3 in device root.sg.d1",
          e.getMessage());
    }
  }

  @Test
  public void writeNewAlignedMeasurementAfterFlushChunkGroup2() {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 100000, 0, 0, true);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(3));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 20, 1000000, 0, true);
    } catch (IOException | WriteProcessException e) {
      Assert.assertEquals(
          "TsFile has flushed chunk group and should not add new measurement s4 in device root.sg.d1",
          e.getMessage());
    }
  }

  @Test
  public void writeOutOfOrderAlignedData() throws IOException, WriteProcessException {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerAlignedTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 0, 0, true);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(0));
      try {
        TsFileGeneratorUtils.writeWithTablet(
            tsFileWriter, deviceId, writeMeasurementScheams, 20, 100, 0, true);
        Assert.fail("Expected to throw writeProcessException due to write out-of-order data.");
      } catch (WriteProcessException e) {
        Assert.assertEquals(
            "Not allowed to write out-of-order data in timeseries root.sg.d1., time should later than 999",
            e.getMessage());
      }

      // example 3
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(alignedMeasurementSchemas.get(1));
      try {
        TsFileGeneratorUtils.writeWithTsRecord(
            tsFileWriter, deviceId, writeMeasurementScheams, 20, 100, 0, true);
        Assert.fail("Expected to throw writeProcessException due to write out-of-order data.");
      } catch (WriteProcessException e) {
        Assert.assertEquals(
            "Not allowed to write out-of-order data in timeseries root.sg.d1., time should later than 999",
            e.getMessage());
      }
    }
  }

  @Test
  public void writeOutOfOrderData() throws IOException, WriteProcessException {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      registerTimeseries(tsFileWriter);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      TsFileGeneratorUtils.writeWithTablet(
          tsFileWriter, deviceId, writeMeasurementScheams, 1000, 0, 0, false);

      // example 2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(0));
      try {
        TsFileGeneratorUtils.writeWithTablet(
            tsFileWriter, deviceId, writeMeasurementScheams, 20, 100, 0, false);
        Assert.fail("Expected to throw writeProcessException due to write out-of-order data.");
      } catch (WriteProcessException e) {
        Assert.assertEquals(
            "Not allowed to write out-of-order data in timeseries root.sg.d1.s1, time should later than 999",
            e.getMessage());
      }

      // example 3
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(1));
      try {
        TsFileGeneratorUtils.writeWithTsRecord(
            tsFileWriter, deviceId, writeMeasurementScheams, 20, 100, 0, false);
        Assert.fail("Expected to throw writeProcessException due to write out-of-order data.");
      } catch (WriteProcessException e) {
        Assert.assertEquals(
            "Not allowed to write out-of-order data in timeseries root.sg.d1.s2, time should later than 999",
            e.getMessage());
      }
    }
  }

  @Test
  public void writeNonAlignedWithTabletWithNullValue() {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      measurementSchemas.add(new MeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));

      // register nonAligned timeseries
      tsFileWriter.registerTimeseries(new Path(deviceId), measurementSchemas);

      Tablet tablet = new Tablet(deviceId, measurementSchemas);
      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;
      tablet.initBitMaps();
      long sensorNum = measurementSchemas.size();
      long startTime = 0;
      for (long r = 0; r < 10000; r++) {
        int row = tablet.rowSize++;
        timestamps[row] = startTime++;
        for (int i = 0; i < sensorNum; i++) {
          if (i == 1 && r > 1000) {
            tablet.bitMaps[i].mark((int) r % tablet.getMaxRowNumber());
            continue;
          }
          Binary[] textSensor = (Binary[]) values[i];
          textSensor[row] = new Binary("testString.........");
        }
        // write
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      }
      // write
      if (tablet.rowSize != 0) {
        tsFileWriter.write(tablet);
        tablet.reset();
      }

    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Meet errors in test: " + e.getMessage());
    }
  }

  @Test
  public void writeAlignedWithTabletWithNullValue() {
    setEnv(100, 30);
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      measurementSchemas.add(new MeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));

      // register aligned timeseries
      tsFileWriter.registerAlignedTimeseries(new Path(deviceId), measurementSchemas);

      Tablet tablet = new Tablet(deviceId, measurementSchemas);
      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;
      tablet.initBitMaps();
      long sensorNum = measurementSchemas.size();
      long startTime = 0;
      for (long r = 0; r < 10000; r++) {
        int row = tablet.rowSize++;
        timestamps[row] = startTime++;
        for (int i = 0; i < sensorNum; i++) {
          if (i == 1 && r > 1000) {
            tablet.bitMaps[i].mark((int) r % tablet.getMaxRowNumber());
            continue;
          }
          Binary[] textSensor = (Binary[]) values[i];
          textSensor[row] = new Binary("testString.........");
        }
        // write
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.writeAligned(tablet);
          tablet.reset();
        }
      }
      // write
      if (tablet.rowSize != 0) {
        tsFileWriter.writeAligned(tablet);
        tablet.reset();
      }

    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Meet errors in test: " + e.getMessage());
    }
  }
}
