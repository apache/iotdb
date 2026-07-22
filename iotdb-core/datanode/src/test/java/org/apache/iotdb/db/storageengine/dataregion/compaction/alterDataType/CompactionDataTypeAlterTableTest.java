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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CompactionDataTypeAlterTableTest extends AbstractCompactionAlterDataTypeTest {

  private IDeviceID tableDevice = new StringArrayDeviceID("table1.d1");

  private boolean reverse;
  private String performerType;

  public CompactionDataTypeAlterTableTest(boolean reverse, String performerType) {
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
  @Override
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    DataNodeTableCache.getInstance().invalid(COMPACTION_TEST_SG);
  }

  @After
  @Override
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTableCache.getInstance().invalid(COMPACTION_TEST_SG);
  }

  @Test
  public void testAlter() throws IOException, WriteProcessException {
    generateDataTypeNotMatchedFiles(true);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);

    TimeseriesMetadata timeseriesMetadata = getTimeseriesMetadata(target, tableDevice, "s1");
    Assert.assertEquals(1L, timeseriesMetadata.getStatistics().getStartTime());
    Assert.assertEquals(2L, timeseriesMetadata.getStatistics().getEndTime());
  }

  @Test
  public void testCannotAlter() throws IOException, WriteProcessException {
    generateDataTypeNotMatchedFiles(false);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, getPerformer(performerType), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    TimeseriesMetadata timeseriesMetadata = getTimeseriesMetadata(target, tableDevice, "s1");
    if (!reverse) {
      Assert.assertEquals(1L, timeseriesMetadata.getStatistics().getStartTime());
      Assert.assertEquals(1L, timeseriesMetadata.getStatistics().getEndTime());
    } else {
      Assert.assertEquals(2L, timeseriesMetadata.getStatistics().getStartTime());
      Assert.assertEquals(2L, timeseriesMetadata.getStatistics().getEndTime());
    }
  }

  private TimeseriesMetadata getTimeseriesMetadata(
      TsFileResource resource, IDeviceID deviceID, String measurement) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      return reader.readTimeseriesMetadata(deviceID, measurement, true);
    }
  }

  private void generateDataTypeNotMatchedFiles(boolean canAlter)
      throws IOException, WriteProcessException {
    if (!reverse) {
      TsFileResource resource1 = generateInt32TableFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);
      TsFileResource resource2 = generateDoubleTableFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    } else {
      TsFileResource resource1 = generateDoubleTableFile(new TimeRange(1, 1), true);
      seqResources.add(resource1);
      TsFileResource resource2 = generateInt32TableFile(new TimeRange(2, 2), true);
      seqResources.add(resource2);
    }
    if (canAlter) {
      createTable(tableDevice.getTableName(), TSDataType.DOUBLE);
    } else {
      createTable(tableDevice.getTableName(), TSDataType.INT32);
    }
  }

  private void createTable(String tableName, TSDataType dataType) {
    TsTable tsTable = new TsTable(tableName);
    tsTable.addColumnSchema(new TagColumnSchema("id_column", TSDataType.STRING));
    tsTable.addColumnSchema(
        new FieldColumnSchema("s1", dataType, TSEncoding.PLAIN, CompressionType.LZ4));
    DataNodeTableCache.getInstance().preUpdateTable(this.COMPACTION_TEST_SG, tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable(this.COMPACTION_TEST_SG, tableName, null);
  }

  private TsFileResource generateInt32TableFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    TableSchema tableSchema =
        new TableSchema(
            tableDevice.getTableName(),
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(resource.getTsFile()).tableSchema(tableSchema).build()) {
      Tablet tablet =
          new Tablet(
              "table1",
              Arrays.asList("tag1", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.write(tablet);
          tablet.reset();
        }
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, i);
        tablet.addValue(row, 0, "d1");
        tablet.addValue(row, 1, 1);
      }
      if (tablet.getRowSize() > 0) {
        writer.write(tablet);
      }
    }
    resource.updateStartTime(tableDevice, timeRange.getMin());
    resource.updateEndTime(tableDevice, timeRange.getMax());
    resource.serialize();
    return resource;
  }

  private TsFileResource generateDoubleTableFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    TableSchema tableSchema =
        new TableSchema(
            tableDevice.getTableName(),
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(resource.getTsFile()).tableSchema(tableSchema).build()) {
      Tablet tablet =
          new Tablet(
              "table1",
              Arrays.asList("tag1", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.write(tablet);
          tablet.reset();
        }
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, i);
        tablet.addValue(row, 0, "d1");
        tablet.addValue(row, 1, (double) 1);
      }
      if (tablet.getRowSize() > 0) {
        writer.write(tablet);
      }
    }
    resource.updateStartTime(tableDevice, timeRange.getMin());
    resource.updateEndTime(tableDevice, timeRange.getMax());
    resource.serialize();
    return resource;
  }
}
