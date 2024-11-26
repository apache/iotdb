/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompactionValidationTest {
  static final String SENSOR_1 = "sensor_1";
  static final String SENSOR_2 = "sensor_2";
  static final String SENSOR_3 = "sensor_3";

  static final String DEVICE_1 = "root.sg.device_1";
  final String dir = TestConstant.OUTPUT_DATA_DIR + "test-validation";

  @Before
  public void setUp() throws IOException {
    FileUtils.forceMkdir(new File(dir));
  }

  private void writeOneFile(String path) {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
        measurementSchemas.add(new MeasurementSchema(SENSOR_1, TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new MeasurementSchema(SENSOR_2, TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new MeasurementSchema(SENSOR_3, TSDataType.TEXT, TSEncoding.PLAIN));

        // register nonAligned timeseries
        tsFileWriter.registerTimeseries(new Path(DEVICE_1), measurementSchemas);

        List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
        // example 1
        writeMeasurementScheams.add(measurementSchemas.get(0));
        writeMeasurementScheams.add(measurementSchemas.get(1));
        writeMeasurementScheams.add(measurementSchemas.get(2));
        writeWithTablet(tsFileWriter, DEVICE_1, writeMeasurementScheams, 10000, 0, 0);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private void writeWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, schemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    long sensorNum = schemas.size();

    for (long r = 0; r < rowNum; r++, startValue++) {
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, startTime++);
      for (int i = 0; i < sensorNum; i++) {
        Binary[] textSensor = (Binary[]) values[i];
        textSensor[row] = new Binary("testString.........", TSFileConfig.STRING_CHARSET);
      }
      // write
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        tsFileWriter.writeTree(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.getRowSize() != 0) {
      tsFileWriter.writeTree(tablet);
      tablet.reset();
    }
  }

  @After
  public void tearDown() throws IOException {
    File[] files = new File(dir).listFiles();
    if (files != null) {
      for (File f : files) {
        FileUtils.delete(f);
      }
    }

    FileUtils.forceDelete(new File(dir));
  }

  @Test
  public void testSingleCompleteFile() {
    String path = dir + File.separator + "test.tsfile";
    writeOneFile(path);
    TsFileResource mockTsFile = new TsFileResource(new File(path));
    Assert.assertTrue(TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile));
  }

  @Test
  public void testMultiCompleteFile() {
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      TsFileResource mockTsFile = new TsFileResource(new File(path));
      Assert.assertTrue(TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile));
    }
  }

  @Test
  public void testOneUncompletedFile() throws IOException {
    String path = dir + File.separator + "test.tsfile";
    writeOneFile(path);
    RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
    randomAccessFile.seek(1024);
    randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    randomAccessFile.close();
    TsFileResource mockTsFile = new TsFileResource(new File(path));
    Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile));
  }

  @Test
  public void testMultiUncompletedFiles() throws IOException {
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
      randomAccessFile.seek(1024);
      randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
      randomAccessFile.close();
      TsFileResource mockTsFile = new TsFileResource(new File(path));
      Assert.assertFalse(TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile));
    }
  }

  @Test // broken in chunk
  public void testOneUncompletedInMultiCompletedFiles1() throws IOException {
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      if (i == 5) {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        randomAccessFile.seek(1024);
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        randomAccessFile.close();
      }
      TsFileResource mockTsFile = new TsFileResource(new File(path));
      TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile);
    }
  }

  @Test // broken in metadata
  public void testOneUncompletedInMultiCompletedFiles2() throws IOException {
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      if (i == 5) {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        randomAccessFile.seek(randomAccessFile.length() - 130);
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        randomAccessFile.close();
      }
      TsFileResource mockTsFile = new TsFileResource(new File(path));
      try {
        boolean dataCorrect = TsFileResourceUtils.validateTsFileDataCorrectness(mockTsFile);
        Assert.assertEquals(i != 5, dataCorrect);
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testTsFileResourceIsDeletedByOtherCompactionTaskWhenValidateOverlap1() {
    TsFileResource resource1 = new TsFileResource();
    resource1.setFile(new File(dir + File.separator + "1-1-0-0.tsfile"));
    resource1.setTimeIndex(new ArrayDeviceTimeIndex());
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 2);

    TsFileResource resource2 = new TsFileResource();
    resource2.setFile(new File(dir + File.separator + "2-2-0-0.tsfile"));
    resource2.setTimeIndex(new FileTimeIndex());

    TsFileResource resource3 = new TsFileResource();
    resource3.setFile(new File(dir + File.separator + "3-3-0-0.tsfile"));
    resource3.setTimeIndex(new ArrayDeviceTimeIndex());
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 4);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 5);

    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            Arrays.asList(resource1, resource2, resource3)));
  }

  @Test
  public void testTsFileResourceIsDeletedByOtherCompactionTaskWhenValidateOverlap2() {
    TsFileResource resource1 = new TsFileResource();
    resource1.setTimeIndex(new ArrayDeviceTimeIndex());
    resource1.setFile(new File(dir + File.separator + "1-1-0-0.tsfile"));
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 2);

    TsFileResource resource2 = new TsFileResource();
    resource2.setFile(new File(dir + File.separator + "2-2-0-0.tsfile"));
    resource2.setTimeIndex(new FileTimeIndex());

    TsFileResource resource3 = new TsFileResource();
    resource3.setFile(new File(dir + File.separator + "3-3-0-0.tsfile"));
    resource3.setTimeIndex(new ArrayDeviceTimeIndex());
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 5);

    Assert.assertFalse(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            Arrays.asList(resource1, resource2, resource3)));
  }

  @Test
  public void testTsFileResourceIsBrokenWhenValidateOverlap1() throws IOException {
    TsFileResource resource1 = new TsFileResource();
    resource1.setTimeIndex(new ArrayDeviceTimeIndex());
    resource1.setFile(new File(dir + File.separator + "1-1-0-0.tsfile"));
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 1);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 2);

    TsFileResource resource2 = new TsFileResource();
    File tsFile2 = new File(dir + File.separator + "2-2-0-0.tsfile");
    Assert.assertTrue(tsFile2.createNewFile());
    File resourceFile2 = new File(dir + File.separator + "2-2-0-0.tsfile.resource");
    Assert.assertTrue(resourceFile2.createNewFile());
    resource2.setFile(tsFile2);
    resource2.setTimeIndex(new FileTimeIndex());

    TsFileResource resource3 = new TsFileResource();
    resource3.setFile(new File(dir + File.separator + "3-3-0-0.tsfile"));
    resource3.setTimeIndex(new ArrayDeviceTimeIndex());
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 4);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"), 5);

    Assert.assertTrue(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            Arrays.asList(resource1, resource2, resource3)));
  }

  @Test
  public void testTsFileResourceIsBrokenWhenValidateOverlap2() throws IOException {
    TsFileResource resource1 = new TsFileResource();
    resource1.setTimeIndex(new ArrayDeviceTimeIndex());
    resource1.setFile(new File(dir + File.separator + "1-1-0-0.tsfile"));
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("db1.d1"), 1);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("db1.d1"), 2);

    TsFileResource resource2 = new TsFileResource();
    File tsFile2 = new File(dir + File.separator + "2-2-0-0.tsfile");
    Assert.assertTrue(tsFile2.createNewFile());
    File resourceFile2 = new File(dir + File.separator + "2-2-0-0.tsfile.resource");
    Assert.assertTrue(resourceFile2.createNewFile());
    resource2.setFile(tsFile2);
    resource2.setTimeIndex(new FileTimeIndex());

    TsFileResource resource3 = new TsFileResource();
    resource3.setFile(new File(dir + File.separator + "3-3-0-0.tsfile"));
    resource3.setTimeIndex(new ArrayDeviceTimeIndex());
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("db1.d1"), 1);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create("db1.d1"), 5);

    Assert.assertFalse(
        TsFileResourceUtils.validateTsFileResourcesHasNoOverlap(
            Arrays.asList(resource1, resource2, resource3)));
  }
}
