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

package org.apache.iotdb.db.storageengine.dataregion.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.impl.RewriteCrossSpaceCompactionSelector;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeUpgradeTest {

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();

  private int seqFileNum = 2;
  private TSEncoding encoding = TSEncoding.RLE;
  private MeasurementSchema[] measurementSchemas;
  private int timeseriesNum = 5;
  private long ptNum = 10;
  private boolean changeVersion = true;
  private String deviceName = "root.MergeUpgrade.device0";

  @Before
  public void setUp() throws IOException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setMinCrossCompactionUnseqFileLevel(0);
    prepareSeries();
    prepareFiles();
  }

  @After
  public void tearDown() {
    new CompactionConfigRestorer().restoreCompactionConfig();
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
  }

  @Test
  public void testMergeUpgradeSelect() throws MergeException {
    TsFileManager tsFileManager = new TsFileManager("", "", "");
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, true);
    RewriteCrossSpaceCompactionSelector selector =
        new RewriteCrossSpaceCompactionSelector(
            "", "", 0, tsFileManager, new CompactionScheduleContext());
    List<CrossCompactionTaskResource> selected =
        selector.selectCrossSpaceTask(seqResources, unseqResources);
    assertEquals(0, selected.size());
  }

  private void prepareFiles() throws IOException, WriteProcessException {
    // prepare seqFiles
    for (int i = 0; i < seqFileNum; i++) {
      File seqfile =
          FSFactoryProducer.getFSFactory()
              .getFile(
                  TestConstant.BASE_OUTPUT_PATH.concat(
                      "seq"
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + i
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile"));
      TsFileResource seqTsFileResource = new TsFileResource(seqfile);
      seqResources.add(seqTsFileResource);
      prepareOldFile(seqTsFileResource, i * ptNum, ptNum, 0);
    }
    // prepare unseqFile
    File unseqfile =
        FSFactoryProducer.getFSFactory()
            .getFile(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    "unseq"
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile"));
    TsFileResource unseqTsFileResource = new TsFileResource(unseqfile);
    unseqResources.add(unseqTsFileResource);
    prepareFile(unseqTsFileResource, 0, 2 * ptNum, 10);
  }

  private void prepareSeries() {
    measurementSchemas = new MeasurementSchema[timeseriesNum];
    for (int i = 0; i < timeseriesNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
  }

  private void prepareOldFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    prepareData(tsFileResource, fileWriter, timeOffset, ptNum, valueOffset);
    fileWriter.close();
    if (changeVersion) {
      try (RandomAccessFile oldTsfile = new RandomAccessFile(tsFileResource.getTsFile(), "rw")) {
        oldTsfile.seek(TSFileConfig.MAGIC_STRING.length());
        oldTsfile.write(TSFileConfig.VERSION_NUMBER_V1.getBytes());
      }
      changeVersion = false;
    }
  }

  private void prepareFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    prepareData(tsFileResource, fileWriter, timeOffset, ptNum, valueOffset);
    fileWriter.close();
  }

  private void removeFiles() {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }
  }

  private void prepareData(
      TsFileResource tsFileResource,
      TsFileWriter fileWriter,
      long timeOffset,
      long ptNum,
      long valueOffset)
      throws WriteProcessException, IOException {
    for (org.apache.tsfile.write.schema.MeasurementSchema MeasurementSchema : measurementSchemas) {
      fileWriter.registerTimeseries(new Path(deviceName), MeasurementSchema);
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      TSRecord record = new TSRecord(deviceName, i);
      for (int k = 0; k < timeseriesNum; k++) {
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchemas[k].getType(),
                measurementSchemas[k].getMeasurementName(),
                String.valueOf(i + valueOffset)));
      }
      fileWriter.writeRecord(record);
      tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceName), i);
      tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceName), i);
    }
  }
}
