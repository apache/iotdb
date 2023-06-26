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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
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
        List<MeasurementSchema> measurementSchemas = new ArrayList<>();
        measurementSchemas.add(new MeasurementSchema(SENSOR_1, TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new MeasurementSchema(SENSOR_2, TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new MeasurementSchema(SENSOR_3, TSDataType.TEXT, TSEncoding.PLAIN));

        // register nonAligned timeseries
        tsFileWriter.registerTimeseries(new Path(DEVICE_1), measurementSchemas);

        List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
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
      List<MeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, schemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    long sensorNum = schemas.size();

    for (long r = 0; r < rowNum; r++, startValue++) {
      int row = tablet.rowSize++;
      timestamps[row] = startTime++;
      for (int i = 0; i < sensorNum; i++) {
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
    TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
    Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
    Assert.assertTrue(CompactionUtils.validateTsFiles(Collections.singletonList(mockTsFile)));
  }

  @Test
  public void testMultiCompleteFile() {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
      Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
      resources.add(mockTsFile);
    }
    Assert.assertTrue(CompactionUtils.validateTsFiles(resources));
  }

  @Test
  public void testOneUncompletedFile() throws IOException {
    String path = dir + File.separator + "test.tsfile";
    writeOneFile(path);
    RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
    randomAccessFile.seek(1024);
    randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    randomAccessFile.close();
    TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
    Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
    Assert.assertFalse(CompactionUtils.validateTsFiles(Collections.singletonList(mockTsFile)));
  }

  @Test
  public void testMultiUncompletedFiles() throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
      randomAccessFile.seek(1024);
      randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
      randomAccessFile.close();
      TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
      Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
      resources.add(mockTsFile);
    }
    Assert.assertFalse(CompactionUtils.validateTsFiles(resources));
  }

  @Test // broken in chunk
  public void testOneUncompletedInMultiCompletedFiles1() throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      if (i == 5) {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        randomAccessFile.seek(1024);
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        randomAccessFile.close();
      }
      TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
      Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
      resources.add(mockTsFile);
    }
    Assert.assertFalse(CompactionUtils.validateTsFiles(resources));
  }

  @Test // broken in metadata
  public void testOneUncompletedInMultiCompletedFiles2() throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String path = dir + File.separator + "test" + i + ".tsfile";
      writeOneFile(path);
      if (i == 5) {
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        randomAccessFile.seek(randomAccessFile.length() - 100);
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        randomAccessFile.close();
      }
      TsFileResource mockTsFile = Mockito.mock(TsFileResource.class);
      Mockito.when(mockTsFile.getTsFilePath()).thenReturn(path);
      resources.add(mockTsFile);
    }
    Assert.assertFalse(CompactionUtils.validateTsFiles(resources));
  }
}
