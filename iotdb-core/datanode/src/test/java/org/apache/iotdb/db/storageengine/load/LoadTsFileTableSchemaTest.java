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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.schema.table.TsFileTableSchemaUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.table.TsFileInsertionEventTableParser;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.ChunkOffsetCalculator;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class LoadTsFileTableSchemaTest {

  private static final String DATABASE = "load_table_schema_test";
  private static final String TABLE = "measurements";
  private static final int ROW_COUNT = 20;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private String[] originalLoadDirs;
  private DataRegion dataRegion;
  private TsTable table;

  @Before
  public void setUp() throws Exception {
    originalLoadDirs = IoTDBDescriptor.getInstance().getConfig().getLoadTsFileDirs();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setLoadTsFileDirs(new String[] {temporaryFolder.getRoot().getAbsolutePath()});
    dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getDatabaseName()).thenReturn(DATABASE);
    Mockito.when(dataRegion.getDataRegionIdString()).thenReturn("0");
    Mockito.when(dataRegion.getNonSystemDatabaseName()).thenReturn(Optional.of(DATABASE));

    table = new TsTable(TABLE);
    table.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    table.addColumnSchema(
        new FieldColumnSchema("value", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.LZ4));
    DataNodeTableCache.getInstance().preUpdateTable(DATABASE, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(DATABASE, TABLE, null, null);
  }

  @After
  public void tearDown() {
    DataNodeTableCache.getInstance().invalid(DATABASE);
    IoTDBDescriptor.getInstance().getConfig().setLoadTsFileDirs(originalLoadDirs);
  }

  @Test
  public void testLoadedTableRemainsReadableByPipe() throws Exception {
    assertLoadedTableReadable(false, false);
  }

  @Test
  public void testRecoveredTableRemainsReadableByPipe() throws Exception {
    assertLoadedTableReadable(true, false);
  }

  @Test
  public void testLoadedTableWithNullTagsRemainsReadableByPipe() throws Exception {
    assertLoadedTableReadable(false, true);
  }

  private void assertLoadedTableReadable(final boolean recover, final boolean nullTags)
      throws Exception {
    final File source = new File(temporaryFolder.getRoot(), "source.tsfile");
    final TableSchema schema = TsFileTableSchemaUtil.toTsFileTableSchemaNoAttribute(table);
    try (final TsFileWriter writer = new TsFileWriter(source)) {
      writer.registerTableSchema(schema);
      final Tablet tablet =
          new Tablet(
              TABLE,
              Arrays.asList("device", "value"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
              ROW_COUNT);
      for (int i = 0; i < ROW_COUNT; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue("device", i, nullTags ? null : "d" + i % 2);
        tablet.addValue("value", i, i);
      }
      writer.writeTable(tablet);
    }

    final File taskDir = temporaryFolder.newFolder("task");
    TsFileWriterManager manager = new TsFileWriterManager(dataRegion, taskDir);
    try {
      final ChunkOffsetCalculator calculator = new ChunkOffsetCalculator();
      final List<TsFileData> pieces = new ArrayList<>();
      new TsFileSplitter(
              source,
              data -> {
                calculator.assign((ChunkData) data);
                pieces.add(data);
                return true;
              })
          .splitTsFileByDataPartition();
      manager.writePiece(pieces);
      if (recover) {
        manager.close(true);
        manager = new TsFileWriterManager(dataRegion, taskDir).recoverFromDisk();
      }
      manager.prepare(false, Collections.emptyMap(), Collections.emptyMap());

      final File staged = new File(taskDir, DATABASE + "-0-0.tsfile");
      int rowCount = 0;
      try (final TsFileInsertionEventTableParser parser =
          new TsFileInsertionEventTableParser(
              staged,
              new TablePattern(true, null, null),
              Long.MIN_VALUE,
              Long.MAX_VALUE,
              null,
              null,
              null,
              false)) {
        for (final TabletInsertionEvent event : parser.toTabletInsertionEvents()) {
          final PipeRawTabletInsertionEvent tabletEvent = (PipeRawTabletInsertionEvent) event;
          rowCount += tabletEvent.convertToTablet().getRowSize();
          tabletEvent.clearReferenceCount(getClass().getName());
        }
      }
      assertEquals(ROW_COUNT, rowCount);
      try (final TsFileSequenceReader reader = new TsFileSequenceReader(staged.getPath())) {
        assertEquals(schema, reader.getTableSchemaMap().get(TABLE));
      }
    } finally {
      manager.close();
    }
  }
}
