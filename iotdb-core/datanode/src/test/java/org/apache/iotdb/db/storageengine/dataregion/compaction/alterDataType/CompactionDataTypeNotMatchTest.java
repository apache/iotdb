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

package org.apache.iotdb.db.storageengine.dataregion.compaction.alterDataType;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(Parameterized.class)
public class CompactionDataTypeNotMatchTest extends AbstractCompactionAlterDataTypeTest {
  private final IDeviceID device =
      IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + ".d1");
  private String performerType;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Parameterized.Parameters(name = "type={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"read_chunk"}, {"fast"}, {"read_point"},
        });
  }

  public CompactionDataTypeNotMatchTest(String type) {
    this.performerType = type;
  }

  @Test
  public void testCompactNonAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    generateDataTypeNotMatchFilesWithNonAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
  }

  @Test
  public void testCompactNonAlignedSeries2()
      throws IOException, WriteProcessException, IllegalPathException {
    MeasurementSchema measurementSchema1 = new MeasurementSchema("s1", TSDataType.INT32);
    TsFileResource resource1 = generateInt32NonAlignedSeriesFile(new TimeRange(1, 1), true);
    seqResources.add(resource1);
    TsFileResource resource2 = generateDoubleNonAlignedSeriesFile(new TimeRange(2, 2), true);
    seqResources.add(resource2);
    schemaFetcher
        .getSchemaTree()
        .appendSingleMeasurementPath(new MeasurementPath(device, "s1", measurementSchema1));

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(
        1, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        1, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
  }

  @Test
  public void testCompactAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    generateDataTypeNotMatchFilesWithAlignedSeries();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
  }

  @Test
  public void testCompactAlignedSeries2()
      throws IOException, WriteProcessException, IllegalPathException {
    TsFileResource resource1 = generateInt32AlignedSeriesFile(new TimeRange(1, 1), true);
    seqResources.add(resource1);
    TsFileResource resource2 = generateDoubleAlignedSeriesFile(new TimeRange(2, 2), true);
    seqResources.add(resource2);
    MeasurementPath s1Path =
        new MeasurementPath(device, "s1", new MeasurementSchema("s1", TSDataType.INT32));
    s1Path.setUnderAlignedEntity(true);
    MeasurementPath s2Path =
        new MeasurementPath(device, "s2", new MeasurementSchema("s2", TSDataType.INT32));
    s2Path.setUnderAlignedEntity(true);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s1Path);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s2Path);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResourceUtils.validateTsFileDataCorrectness(tsFileManager.getTsFileList(true).get(0));
    Assert.assertEquals(
        1, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        1, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
  }

  private void generateDataTypeNotMatchFilesWithNonAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
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
    TsFileResource resource2 = generateInt32NonAlignedSeriesFile(new TimeRange(2, 2), true);
    seqResources.add(resource2);

    schemaFetcher
        .getSchemaTree()
        .appendSingleMeasurementPath(new MeasurementPath(device, "s1", measurementSchema2));
  }

  private void generateDataTypeNotMatchFilesWithAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    List<IMeasurementSchema> measurementSchemas1 = new ArrayList<>();
    measurementSchemas1.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemas1.add(new MeasurementSchema("s2", TSDataType.INT32));

    TsFileResource resource1 = generateInt32AlignedSeriesFile(new TimeRange(1, 1), true);
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

    MeasurementPath s1Path = new MeasurementPath(device, "s1", measurementSchemas2.get(0));
    s1Path.setUnderAlignedEntity(true);
    MeasurementPath s2Path = new MeasurementPath(device, "s2", measurementSchemas2.get(1));
    s2Path.setUnderAlignedEntity(true);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s1Path);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s2Path);
  }
}
