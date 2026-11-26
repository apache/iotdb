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
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
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

@RunWith(Parameterized.class)
public class CompactionDataTypeAlterTest extends AbstractCompactionAlterDataTypeTest {
  private boolean reverse;
  private String performerType;

  public CompactionDataTypeAlterTest(boolean reverse, String performerType) {
    this.reverse = reverse;
    this.performerType = performerType;
  }

  @Parameterized.Parameters(name = "reverse={0} performerType={1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {true, "read_chunk"},
          {false, "read_chunk"},
          {true, "fast"},
          {false, "fast"},
          {true, "read_point"},
          {false, "read_point"},
        });
  }

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
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
        1, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
    TsFileResource tsFileResource = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
            new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath());
        TsFileReader readTsFile = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(device, "s1", true));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      Assert.assertTrue(queryDataSet.hasNext());
      Assert.assertEquals("1\t1.0", queryDataSet.next().toString());
      Assert.assertTrue(queryDataSet.hasNext());
      Assert.assertEquals("2\t2.0", queryDataSet.next().toString());
      Assert.assertFalse(queryDataSet.hasNext());
    }
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
        1, ((long) tsFileManager.getTsFileList(true).get(0).getStartTime(device).get()));
    Assert.assertEquals(
        2, ((long) tsFileManager.getTsFileList(true).get(0).getEndTime(device).get()));
    TsFileResource tsFileResource = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
            new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath());
        TsFileReader readTsFile = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(device, "s1", true));
      paths.add(new Path(device, "s2", true));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      Assert.assertTrue(queryDataSet.hasNext());
      Assert.assertEquals("1\t1.0\t1.0", queryDataSet.next().toString());
      Assert.assertTrue(queryDataSet.hasNext());
      Assert.assertEquals("2\t2.0\t2.0", queryDataSet.next().toString());
      Assert.assertFalse(queryDataSet.hasNext());
    }
  }

  private void generateDataTypeNotMatchFilesWithNonAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    if (!reverse) {
      TsFileResource resource1 = generateInt32NonAlignedSeriesFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);

      TsFileResource resource2 = generateDoubleNonAlignedSeriesFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    } else {
      TsFileResource resource1 = generateDoubleNonAlignedSeriesFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);

      TsFileResource resource2 = generateInt32NonAlignedSeriesFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    }

    schemaFetcher
        .getSchemaTree()
        .appendSingleMeasurementPath(
            new MeasurementPath(device, "s1", new MeasurementSchema("s1", TSDataType.DOUBLE)));
  }

  private void generateDataTypeNotMatchFilesWithAlignedSeries()
      throws IOException, WriteProcessException, IllegalPathException {
    if (!reverse) {
      TsFileResource resource1 = generateInt32AlignedSeriesFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);
      TsFileResource resource2 = generateDoubleAlignedSeriesFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    } else {
      TsFileResource resource1 = generateDoubleAlignedSeriesFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);
      TsFileResource resource2 = generateInt32AlignedSeriesFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    }

    List<IMeasurementSchema> measurementSchemas2 = new ArrayList<>();
    measurementSchemas2.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
    measurementSchemas2.add(new MeasurementSchema("s2", TSDataType.DOUBLE));

    MeasurementPath s1Path = new MeasurementPath(device, "s1", measurementSchemas2.get(0));
    s1Path.setUnderAlignedEntity(true);
    MeasurementPath s2Path = new MeasurementPath(device, "s2", measurementSchemas2.get(1));
    s2Path.setUnderAlignedEntity(true);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s1Path);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s2Path);
  }

  @Test
  public void testAlterDataTypeWithAlignedSeriesWithTimeDeletion()
      throws IOException, WriteProcessException, IllegalPathException {
    List<IMeasurementSchema> measurementSchemas1 = new ArrayList<>();
    measurementSchemas1.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemas1.add(new MeasurementSchema("s2", TSDataType.INT32));

    TsFileResource resource1 = generateInt32AlignedSeriesFile(new TimeRange(0, 100), true);
    ModificationFile modFile = resource1.getExclusiveModFile();
    modFile.write(new TreeDeletionEntry(new MeasurementPath(device + ".*"), 0, 40));
    modFile.close();
    seqResources.add(resource1);

    List<IMeasurementSchema> measurementSchemas2 = new ArrayList<>();
    measurementSchemas2.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
    measurementSchemas2.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
    TsFileResource resource2 = generateDoubleAlignedSeriesFile(new TimeRange(200, 200), true);
    seqResources.add(resource2);

    MeasurementPath s1Path = new MeasurementPath(device, "s1", measurementSchemas2.get(0));
    s1Path.setUnderAlignedEntity(true);
    MeasurementPath s2Path = new MeasurementPath(device, "s2", measurementSchemas2.get(1));
    s2Path.setUnderAlignedEntity(true);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s1Path);
    schemaFetcher.getSchemaTree().appendSingleMeasurementPath(s2Path);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertEquals(41L, (long) target.getStartTime(device).get());
    Assert.assertEquals(200L, (long) target.getEndTime(device).get());
  }
}
