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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeMissingSchemaException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.type.TypeFactory;
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
import java.lang.reflect.InvocationTargetException;
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

  @Test
  public void testTableSchemaCacheShouldThrowMismatchWhenVerifyingDataType() throws Exception {
    final LoadTsFileTableSchemaCache schemaCache = createTableSchemaCache(true);
    try {
      final InvocationTargetException exception =
          Assert.assertThrows(
              InvocationTargetException.class,
              () ->
                  getVerifyTableDataTypeMethod()
                      .invoke(
                          schemaCache,
                          createTableSchema(TSDataType.INT64),
                          createTableSchema(TSDataType.DOUBLE)));

      Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeTypeMismatchException);
    } finally {
      schemaCache.close();
    }
  }

  @Test
  public void testTableSchemaCacheShouldNotThrowMismatchWhenSkippingDataTypeVerification()
      throws Exception {
    final LoadTsFileTableSchemaCache schemaCache = createTableSchemaCache(false);
    try {
      getVerifyTableDataTypeMethod()
          .invoke(
              schemaCache,
              createTableSchema(TSDataType.INT64),
              createTableSchema(TSDataType.DOUBLE));
    } finally {
      schemaCache.close();
    }
  }

  @Test
  public void testTreeSchemaVerifierShouldThrowMismatchWhenVerifyingDataType() throws Exception {
    final File tsFile = new File("load-tree-type-mismatch.tsfile");
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }
    Assert.assertTrue(tsFile.createNewFile());

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            false,
            new MPPQueryContext(new QueryId("load_tree_test")))) {
      final TreeSchemaAutoCreatorAndVerifier verifier =
          new TreeSchemaAutoCreatorAndVerifier(analyzer);
      try {
        final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
        final LoadTsFileTreeSchemaCache schemaCache = getTreeSchemaCache(verifier);
        schemaCache.addTimeSeries(device, new MeasurementSchema("s1", TSDataType.BOOLEAN));
        schemaCache.addIsAlignedCache(device, true, true);

        final ClusterSchemaTree schemaTree = new ClusterSchemaTree();
        schemaTree.appendSingleMeasurement(
            new PartialPath("root.sg.d1.s1"),
            new MeasurementSchema("s1", TSDataType.INT32),
            null,
            null,
            null,
            true);

        final InvocationTargetException exception =
            Assert.assertThrows(
                InvocationTargetException.class,
                () -> getVerifyTreeSchemaMethod().invoke(verifier, schemaTree));
        Assert.assertTrue(exception.getCause() instanceof LoadAnalyzeTypeMismatchException);
      } finally {
        verifier.close();
      }
    } finally {
      if (tsFile.exists()) {
        Assert.assertTrue(tsFile.delete());
      }
    }
  }

  @Test
  public void testPipeGeneratedLoadMissingSchemaShouldBeTemporaryWhenAutoCreateDisabled()
      throws Exception {
    final boolean originalAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    final File tsFile = File.createTempFile("missing-schema", ".tsfile");
    tsFile.deleteOnExit();

    try (final LoadTsFileAnalyzer analyzer =
        new LoadTsFileAnalyzer(
            LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
            true,
            new MPPQueryContext(new QueryId("load_pipe_test")))) {
      Assert.assertTrue(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new LoadAnalyzeMissingSchemaException("missing device schema")));
      Assert.assertTrue(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new RuntimeException(
                  "wrapped", new LoadAnalyzeMissingSchemaException("missing measurement schema"))));
      Assert.assertFalse(
          analyzer.isTemporaryUnavailableDueToPipeSchemaNotReady(
              new LoadAnalyzeException("Data type mismatch for measurement root.sg.d1.s1")));
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAutoCreateSchemaEnabled(originalAutoCreateSchemaEnabled);
    }
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

  private LoadTsFileTreeSchemaCache getTreeSchemaCache(
      final TreeSchemaAutoCreatorAndVerifier verifier) throws Exception {
    final Field schemaCacheField =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredField("schemaCache");
    schemaCacheField.setAccessible(true);
    return (LoadTsFileTreeSchemaCache) schemaCacheField.get(verifier);
  }

  private LoadTsFileTableSchemaCache createTableSchemaCache(final boolean shouldVerifyDataType)
      throws LoadRuntimeOutOfMemoryException {
    return new LoadTsFileTableSchemaCache(
        null, new MPPQueryContext(new QueryId("load_test")), false, shouldVerifyDataType);
  }

  private Method getVerifyTableDataTypeMethod() throws NoSuchMethodException {
    final Method method =
        LoadTsFileTableSchemaCache.class.getDeclaredMethod(
            "verifyTableDataTypeAndGenerateTagColumnMapper",
            org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema.class,
            org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema.class);
    method.setAccessible(true);
    return method;
  }

  private Method getVerifyTreeSchemaMethod() throws NoSuchMethodException {
    final Method method =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod("verifySchema", ISchemaTree.class);
    method.setAccessible(true);
    return method;
  }

  private org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema
      createTableSchema(final TSDataType fieldType) {
    return new org.apache.iotdb.commons.queryengine.plan.relational.metadata.TableSchema(
        "table1",
        Arrays.asList(
            new ColumnSchema(
                "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG),
            new ColumnSchema(
                "s1", TypeFactory.getType(fieldType), false, TsTableColumnCategory.FIELD)));
  }

  private boolean containsDevice(final Set<IDeviceID> devices, final String... expectedSegments) {
    return devices.stream()
        .anyMatch(device -> Arrays.equals(device.getSegments(), expectedSegments));
  }

  private static class TrackingLoadTsFileTableSchemaCache extends LoadTsFileTableSchemaCache {

    private final Set<List<Object>> verifiedDevices = new HashSet<>();

    private TrackingLoadTsFileTableSchemaCache() throws LoadRuntimeOutOfMemoryException {
      super(null, new MPPQueryContext(new QueryId("load_test")), false, true);
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
