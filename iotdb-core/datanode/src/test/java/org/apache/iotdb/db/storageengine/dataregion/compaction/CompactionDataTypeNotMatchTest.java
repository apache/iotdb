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

package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompactionDataTypeNotMatchTest extends AbstractCompactionTest {
  private final String oldThreadName = Thread.currentThread().getName();
  private final IDeviceID device =
      IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + ".d1");

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  @Test
  public void testCompactNonAlignedSeriesWithReadChunkCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  @Test
  public void testCompactNonAlignedSeriesWithFastCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new FastCompactionPerformer(false), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  @Test
  public void testCompactNonAlignedSeriesWithReadPointCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadPointCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  @Test
  public void testCompactAlignedSeriesWithReadChunkCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  @Test
  public void testCompactAlignedSeriesWithFastCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new FastCompactionPerformer(false), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  @Test
  public void testCompactAlignedSeriesWithReadPointCompactionPerformer()
      throws IOException, WriteProcessException {
    generateDataTypeNotMatchFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadPointCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(2, tsFileManager.getTsFileList(true).get(0).getStartTime(device));
  }

  private void generateDataTypeNotMatchFilesWithNonAlignedSeries()
      throws IOException, WriteProcessException {
    MeasurementSchema measurementSchema1 = new MeasurementSchema("s1", TSDataType.BOOLEAN);
    TsFileResource resource1 = createEmptyFileAndResource(true);
    resource1.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource1.getTsFile())) {
      writer.registerTimeseries(new Path(device), measurementSchema1);
      TSRecord record = new TSRecord(device, 1);
      record.addTuple(new BooleanDataPoint("s1", true));
      writer.writeRecord(record);
      writer.flush();
    }
    resource1.updateStartTime(device, 1);
    resource1.updateEndTime(device, 1);
    resource1.serialize();
    seqResources.add(resource1);

    MeasurementSchema measurementSchema2 = new MeasurementSchema("s1", TSDataType.INT32);
    TsFileResource resource2 = createEmptyFileAndResource(true);
    resource2.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource2.getTsFile())) {
      writer.registerTimeseries(new Path(device), measurementSchema2);
      TSRecord record = new TSRecord(device, 2);
      record.addTuple(new IntDataPoint("s1", 10));
      writer.writeRecord(record);
      writer.flush();
    }
    resource2.updateStartTime(device, 2);
    resource2.updateEndTime(device, 2);
    resource2.serialize();
    seqResources.add(resource2);
  }

  private void generateDataTypeNotMatchFilesWithAlignedSeries()
      throws IOException, WriteProcessException {
    List<IMeasurementSchema> measurementSchemas1 = new ArrayList<>();
    measurementSchemas1.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemas1.add(new MeasurementSchema("s2", TSDataType.INT32));

    TsFileResource resource1 = createEmptyFileAndResource(true);
    resource1.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource1.getTsFile())) {
      writer.registerAlignedTimeseries(new Path(device), measurementSchemas1);
      TSRecord record = new TSRecord(device, 1);
      record.addTuple(new IntDataPoint("s1", 0));
      record.addTuple(new IntDataPoint("s2", 1));
      writer.writeRecord(record);
      writer.flush();
    }
    resource1.updateStartTime(device, 1);
    resource1.updateEndTime(device, 1);
    resource1.serialize();
    seqResources.add(resource1);

    List<IMeasurementSchema> measurementSchemas2 = new ArrayList<>();
    measurementSchemas2.add(new MeasurementSchema("s1", TSDataType.BOOLEAN));
    measurementSchemas2.add(new MeasurementSchema("s2", TSDataType.BOOLEAN));
    TsFileResource resource2 = createEmptyFileAndResource(true);
    resource2.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource2.getTsFile())) {
      writer.registerAlignedTimeseries(new Path(device), measurementSchemas2);
      TSRecord record = new TSRecord(device, 2);
      record.addTuple(new BooleanDataPoint("s1", true));
      record.addTuple(new BooleanDataPoint("s2", true));
      writer.writeRecord(record);
      writer.flush();
    }
    resource2.updateStartTime(device, 2);
    resource2.updateEndTime(device, 2);
    resource2.serialize();
    seqResources.add(resource2);
  }
}
