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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerSeqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant.InnerUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class TableModelCompactionWithTTLTest extends AbstractCompactionTest {

  private final String performerType;
  private String threadName;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    this.threadName = Thread.currentThread().getName();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    DataNodeTableCache.getInstance().invalid(this.COMPACTION_TEST_SG);
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(threadName);
    DataNodeTableCache.getInstance().invalid(this.COMPACTION_TEST_SG);
  }

  public TableModelCompactionWithTTLTest(String performerType) {
    this.performerType = performerType;
  }

  @Parameterized.Parameters(name = "type={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"read_chunk"}, {"fast"}, {"read_point"},
        });
  }

  public ICompactionPerformer getPerformer() {
    if (performerType.equalsIgnoreCase(InnerSeqCompactionPerformer.READ_CHUNK.toString())) {
      return new ReadChunkCompactionPerformer();
    } else if (performerType.equalsIgnoreCase(InnerUnseqCompactionPerformer.FAST.toString())) {
      return new FastCompactionPerformer(false);
    } else {
      return new ReadPointCompactionPerformer();
    }
  }

  @Test
  public void testAllDataExpired() throws IOException {
    createTable("t1", 1);
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id_column"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1"),
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertEquals(0, tsFileManager.getTsFileList(true).size());
  }

  @Test
  public void testPartialDataExpired() throws IOException {
    createTable("t1", 1);
    TsFileResource resource1 = createEmptyFileAndResource(true);
    long startTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(200);
    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(200);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id_column"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1"),
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(startTime, endTime)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertTrue(target.getFileStartTime() > startTime && target.getFileEndTime() == endTime);
  }

  @Test
  public void testTableNotExist() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    long startTime = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(200);
    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(200);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id_column"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1"));
      writer.generateSimpleAlignedSeriesToCurrentDeviceWithNullValue(
          Arrays.asList("s0", "s1"),
          new TimeRange[][][] {
            new TimeRange[][] {new TimeRange[] {new TimeRange(startTime, endTime)}}
          },
          TSEncoding.PLAIN,
          CompressionType.LZ4,
          Arrays.asList(false, false));
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(0, tsFileManager, seqResources, true, getPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource target = tsFileManager.getTsFileList(true).get(0);
    Assert.assertTrue(target.getFileStartTime() == startTime && target.getFileEndTime() == endTime);
  }

  public void createTable(String tableName, long ttl) {
    TsTable tsTable = new TsTable(tableName);
    tsTable.addColumnSchema(new IdColumnSchema("id_column", TSDataType.STRING));
    tsTable.addColumnSchema(
        new MeasurementColumnSchema(
            "s1", TSDataType.STRING, TSEncoding.PLAIN, CompressionType.LZ4));
    tsTable.addProp(TsTable.TTL_PROPERTY, ttl + "");
    DataNodeTableCache.getInstance().preUpdateTable(this.COMPACTION_TEST_SG, tsTable);
    DataNodeTableCache.getInstance().commitUpdateTable(this.COMPACTION_TEST_SG, tableName);
  }
}
