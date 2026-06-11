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

package org.timecho.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeInvalidTimeSeriesException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.common.schematree.MeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileTreeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.analyze.load.TreeSchemaAutoCreatorAndVerifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;

public class TimechoTreeSchemaAutoCreatorAndVerifierTest {

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
  public void testVerifySchemaShouldFallbackToTabletConversionForAliasSeries() throws Exception {
    final TreeSchemaAutoCreatorAndVerifier verifier =
        newVerifier(Mockito.mock(LoadTsFileAnalyzer.class));

    try {
      final LoadTsFileTreeSchemaCache schemaCache = getSchemaCache(verifier);
      final IDeviceID deviceID = new StringArrayDeviceID("root.view.d1");

      schemaCache.addTimeSeries(deviceID, new MeasurementSchema("temperature", TSDataType.FLOAT));
      schemaCache.addIsAlignedCache(deviceID, false, true);

      final MeasurementSchema aliasSchema = new MeasurementSchema("temperature", TSDataType.FLOAT);
      aliasSchema.setProps(
          MeasurementPropsUtils.buildAliasSeriesProps(
              Collections.emptyMap(), new PartialPath("root.sg1.d1.temperature")));

      final DeviceSchemaInfo deviceSchemaInfo =
          new DeviceSchemaInfo(
              new PartialPath("root.view.d1"),
              false,
              -1,
              Collections.singletonList(
                  new MeasurementSchemaInfo("temperature", aliasSchema, null, null, null)));

      final ISchemaTree schemaTree = Mockito.mock(ISchemaTree.class);
      Mockito.when(
              schemaTree.searchDeviceSchemaInfo(
                  Mockito.any(PartialPath.class),
                  Mockito.eq(Collections.singletonList("temperature"))))
          .thenReturn(deviceSchemaInfo);

      final Method verifySchemaMethod =
          TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod(
              "verifySchema", ISchemaTree.class);
      verifySchemaMethod.setAccessible(true);

      try {
        verifySchemaMethod.invoke(verifier, schemaTree);
        Assert.fail("Expected alias series to trigger tablet-conversion fallback.");
      } catch (final InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof LoadAnalyzeException);
        Assert.assertTrue(
            e.getCause()
                .getMessage()
                .contains(
                    "Load TsFile will fall back to tablet conversion for alias series support."));
      }
    } finally {
      verifier.close();
    }
  }

  @Test
  public void testVerifySchemaShouldFailFastForInvalidSeries() throws Exception {
    final TreeSchemaAutoCreatorAndVerifier verifier =
        newVerifier(Mockito.mock(LoadTsFileAnalyzer.class));

    try {
      final LoadTsFileTreeSchemaCache schemaCache = getSchemaCache(verifier);
      final IDeviceID deviceID = new StringArrayDeviceID("root.sg1.d1");

      schemaCache.addTimeSeries(deviceID, new MeasurementSchema("temperature", TSDataType.FLOAT));
      schemaCache.addIsAlignedCache(deviceID, false, true);

      final MeasurementSchema invalidSchema =
          new MeasurementSchema("temperature", TSDataType.FLOAT);
      final HashMap<String, String> props = new HashMap<>();
      MeasurementPropsUtils.setInvalid(props, true);
      invalidSchema.setProps(props);

      final DeviceSchemaInfo deviceSchemaInfo =
          new DeviceSchemaInfo(
              new PartialPath("root.sg1.d1"),
              false,
              -1,
              Collections.singletonList(
                  new MeasurementSchemaInfo("temperature", invalidSchema, null, null, null)));

      final ISchemaTree schemaTree = Mockito.mock(ISchemaTree.class);
      Mockito.when(
              schemaTree.searchDeviceSchemaInfo(
                  Mockito.any(PartialPath.class),
                  Mockito.eq(Collections.singletonList("temperature"))))
          .thenReturn(deviceSchemaInfo);

      final Method verifySchemaMethod =
          TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod(
              "verifySchema", ISchemaTree.class);
      verifySchemaMethod.setAccessible(true);

      try {
        verifySchemaMethod.invoke(verifier, schemaTree);
        Assert.fail("Expected invalid series to fail fast.");
      } catch (final InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof LoadAnalyzeInvalidTimeSeriesException);
        Assert.assertTrue(e.getCause().getMessage().contains("cannot accept load data"));
      }
    } finally {
      verifier.close();
    }
  }

  @Test
  public void testVerifySchemaShouldAllowInvalidPhysicalPathSeries() throws Exception {
    try (final AnalyzerFixture fixture = new AnalyzerFixture(true)) {
      final TreeSchemaAutoCreatorAndVerifier verifier = newVerifier(fixture.analyzer);
      final LoadTsFileTreeSchemaCache schemaCache = getSchemaCache(verifier);
      final IDeviceID deviceID = new StringArrayDeviceID("root.sg1.d1");

      schemaCache.addTimeSeries(deviceID, new MeasurementSchema("temperature", TSDataType.FLOAT));
      schemaCache.addIsAlignedCache(deviceID, false, true);

      final MeasurementSchema invalidSchema =
          new MeasurementSchema("temperature", TSDataType.FLOAT);
      final HashMap<String, String> props = new HashMap<>();
      MeasurementPropsUtils.setInvalid(props, true);
      invalidSchema.setProps(props);

      final DeviceSchemaInfo deviceSchemaInfo =
          new DeviceSchemaInfo(
              new PartialPath("root.sg1.d1"),
              false,
              -1,
              Collections.singletonList(
                  new MeasurementSchemaInfo("temperature", invalidSchema, null, null, null)));

      final ISchemaTree schemaTree = Mockito.mock(ISchemaTree.class);
      Mockito.when(
              schemaTree.searchDeviceSchemaInfo(
                  Mockito.any(PartialPath.class),
                  Mockito.eq(Collections.singletonList("temperature"))))
          .thenReturn(deviceSchemaInfo);

      final Method verifySchemaMethod =
          TreeSchemaAutoCreatorAndVerifier.class.getDeclaredMethod(
              "verifySchema", ISchemaTree.class);
      verifySchemaMethod.setAccessible(true);

      try {
        verifySchemaMethod.invoke(verifier, schemaTree);
      } finally {
        verifier.close();
      }
    }
  }

  private static TreeSchemaAutoCreatorAndVerifier newVerifier(final LoadTsFileAnalyzer analyzer)
      throws Exception {
    final Constructor<TreeSchemaAutoCreatorAndVerifier> constructor =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredConstructor(LoadTsFileAnalyzer.class);
    constructor.setAccessible(true);
    return constructor.newInstance(analyzer);
  }

  private static LoadTsFileTreeSchemaCache getSchemaCache(
      final TreeSchemaAutoCreatorAndVerifier verifier) throws Exception {
    final Field schemaCacheField =
        TreeSchemaAutoCreatorAndVerifier.class.getDeclaredField("schemaCache");
    schemaCacheField.setAccessible(true);
    return (LoadTsFileTreeSchemaCache) schemaCacheField.get(verifier);
  }

  private static class AnalyzerFixture implements AutoCloseable {

    private final File tsFile;
    private final LoadTsFileAnalyzer analyzer;

    private AnalyzerFixture(final boolean isTsFilePhysicalPath) throws Exception {
      tsFile = File.createTempFile("timecho-tree-schema-load", ".tsfile");
      final HashMap<String, String> loadAttributes = new HashMap<>();
      if (isTsFilePhysicalPath) {
        loadAttributes.put("tsfile-is-physical-path", Boolean.TRUE.toString());
      }
      final LoadTsFile statement =
          LoadTsFile.createUnchecked(null, tsFile.getAbsolutePath(), loadAttributes)
              .setDatabase("db");
      analyzer = new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")));
    }

    @Override
    public void close() throws Exception {
      analyzer.close();
      Assert.assertTrue(tsFile.delete());
    }
  }
}
