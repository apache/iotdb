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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LoadTsFileAnalyzerTest {

  private int dataNodeId;

  @Before
  public void setUp() {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
  }

  @Test
  public void testAnalyzeSingleTableFileShouldNotCountTimestampInPointCount() throws Exception {
    final File tsFile = new File("load-table-mixed-null-device.tsfile");
    writeTableTsFileWithMixedDevices(tsFile);

    final LoadTsFile statement =
        new LoadTsFile(null, tsFile.getAbsolutePath(), Collections.emptyMap()).setDatabase("db");
    final TrackingLoadTsFileTableSchemaCache schemaCache = new TrackingLoadTsFileTableSchemaCache();
    try (final LoadTsFileAnalyzer analyzer =
            new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")));
        final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      injectTableSchemaCache(analyzer, schemaCache);

      final Method method =
          LoadTsFileAnalyzer.class.getDeclaredMethod(
              "doAnalyzeSingleTableFile",
              File.class,
              TsFileSequenceReader.class,
              TsFileSequenceReaderTimeseriesMetadataIterator.class,
              java.util.Map.class);
      method.setAccessible(true);

      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      method.invoke(
          analyzer, tsFile, reader, timeseriesMetadataIterator, reader.getTableSchemaMap());
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }

    Assert.assertEquals(1, statement.getResources().size());
    final TsFileResource resource = statement.getResources().get(0);
    Assert.assertTrue(containsDevice(resource.getDevices(), "table1", "tagA"));
    Assert.assertTrue(containsDevice(resource.getDevices(), "table1", "tagB"));
    Assert.assertEquals(6L, statement.getWritePointCount(0));
    Assert.assertTrue(schemaCache.containsDevice("table1", "tagA"));
    Assert.assertTrue(schemaCache.containsDevice("table1", "tagB"));
    Assert.assertEquals(2, schemaCache.getVerifiedDeviceCount());
  }

  private void writeTableTsFileWithMixedDevices(final File tsFile) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.DOUBLE));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(schema);

      writeDevice(writer, tableSchemaList, new String[] {"table1", "tagA"}, false);
      writeDevice(writer, tableSchemaList, new String[] {"table1", "tagB"}, true);

      writer.endFile();
    }
  }

  private void writeDevice(
      final TsFileIOWriter writer,
      final List<IMeasurementSchema> tableSchemaList,
      final String[] deviceSegments,
      final boolean areAllFieldsNull)
      throws Exception {
    writer.startChunkGroup(new StringArrayDeviceID(deviceSegments));

    final AlignedChunkWriterImpl chunkWriter =
        new AlignedChunkWriterImpl(tableSchemaList.subList(1, tableSchemaList.size()));
    for (int i = 0; i < 3; i++) {
      final long time = 100 + i;
      chunkWriter.getTimeChunkWriter().write(time);
      chunkWriter.getValueChunkWriterByIndex(0).write(time, (long) i, areAllFieldsNull);
      chunkWriter.getValueChunkWriterByIndex(1).write(time, 0.5 + i, areAllFieldsNull);
    }
    chunkWriter.writeToFileWriter(writer);
    writer.endChunkGroup();
  }

  private void injectTableSchemaCache(
      final LoadTsFileAnalyzer analyzer, final TrackingLoadTsFileTableSchemaCache schemaCache)
      throws Exception {
    final Field tableSchemaCacheField =
        LoadTsFileAnalyzer.class.getDeclaredField("tableSchemaCache");
    tableSchemaCacheField.setAccessible(true);
    tableSchemaCacheField.set(analyzer, schemaCache);
  }

  private boolean containsDevice(final Set<IDeviceID> devices, final String... expectedSegments) {
    return devices.stream()
        .anyMatch(device -> Arrays.equals(device.getSegments(), expectedSegments));
  }

  private static class TrackingLoadTsFileTableSchemaCache extends LoadTsFileTableSchemaCache {

    private final Set<List<Object>> verifiedDevices = new HashSet<>();

    private TrackingLoadTsFileTableSchemaCache() throws LoadRuntimeOutOfMemoryException {
      super(null, new MPPQueryContext(new QueryId("load_test")), false);
    }

    @Override
    public void autoCreateAndVerify(final IDeviceID device) {
      verifiedDevices.add(Arrays.asList(device.getSegments()));
    }

    @Override
    public boolean isDeviceDeletedByMods(final IDeviceID device) {
      return false;
    }

    private boolean containsDevice(final String... expectedSegments) {
      return verifiedDevices.contains(Arrays.asList((Object[]) expectedSegments));
    }

    private int getVerifiedDeviceCount() {
      return verifiedDevices.size();
    }
  }
}
