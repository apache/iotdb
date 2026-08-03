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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeInvalidTimeSeriesException;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTypeMismatchException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.load.LoadTsFileAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TimechoLoadTsFileAnalyzerTest {

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
  public void testShouldSkipConversionForInvalidSeries() throws Exception {
    final File tsFile = File.createTempFile("load-skip-conversion", ".tsfile");
    try {
      final LoadTsFile statement =
          LoadTsFile.createUnchecked(null, tsFile.getAbsolutePath(), Collections.emptyMap())
              .setDatabase("db");
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final Method method =
            LoadTsFileAnalyzer.class.getDeclaredMethod(
                "shouldSkipConversion", LoadAnalyzeException.class);
        method.setAccessible(true);

        Assert.assertTrue(
            (Boolean)
                method.invoke(
                    analyzer, new LoadAnalyzeInvalidTimeSeriesException("invalid timeseries")));
        Assert.assertFalse(
            (Boolean) method.invoke(analyzer, new LoadAnalyzeTypeMismatchException("mismatch")));
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testCreateTableModelConversionStatementKeepsPhysicalPathFromTreeLoad()
      throws Exception {
    final File tsFile = File.createTempFile("load-table-conversion-tree", ".tsfile");
    try {
      final Map<String, String> loadAttributes = new HashMap<>();
      loadAttributes.put(
          LoadTsFileConfigurator.TSFILE_IS_PHYSICAL_PATH_KEY, Boolean.TRUE.toString());
      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      statement.setLoadAttributes(loadAttributes);

      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFile conversionStatement =
            invokeCreateTableModelConversionStatement(analyzer, tsFile);
        Assert.assertTrue(conversionStatement.isTsFilePhysicalPath());
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testCreateTableModelConversionStatementWithoutPhysicalPathFromTreeLoad()
      throws Exception {
    final File tsFile = File.createTempFile("load-table-conversion-tree-default", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFile conversionStatement =
            invokeCreateTableModelConversionStatement(analyzer, tsFile);
        Assert.assertFalse(conversionStatement.isTsFilePhysicalPath());
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testCreateTableModelConversionStatementKeepsPhysicalPathFromTableLoad()
      throws Exception {
    final File tsFile = File.createTempFile("load-table-conversion-table", ".tsfile");
    try {
      final Map<String, String> loadAttributes = new HashMap<>();
      loadAttributes.put(
          LoadTsFileConfigurator.TSFILE_IS_PHYSICAL_PATH_KEY, Boolean.TRUE.toString());
      final LoadTsFile statement =
          LoadTsFile.createUnchecked(null, tsFile.getAbsolutePath(), loadAttributes)
              .setDatabase("db");
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFile conversionStatement =
            invokeCreateTableModelConversionStatement(analyzer, tsFile);
        Assert.assertTrue(conversionStatement.isTsFilePhysicalPath());
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testCreateTreeConversionStatementKeepsPhysicalPathFromTreeLoad() throws Exception {
    final File tsFile = File.createTempFile("load-tree-conversion", ".tsfile");
    try {
      final Map<String, String> loadAttributes = new HashMap<>();
      loadAttributes.put(
          LoadTsFileConfigurator.TSFILE_IS_PHYSICAL_PATH_KEY, Boolean.TRUE.toString());
      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      statement.setLoadAttributes(loadAttributes);

      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFileStatement conversionStatement =
            invokeCreateTreeConversionStatement(analyzer, tsFile);
        Assert.assertTrue(conversionStatement.isTsFilePhysicalPath());
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testCreateTreeConversionStatementWithoutPhysicalPathFromTreeLoad() throws Exception {
    final File tsFile = File.createTempFile("load-tree-conversion-default", ".tsfile");
    try {
      final LoadTsFileStatement statement =
          LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath());
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(statement, false, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFileStatement conversionStatement =
            invokeCreateTreeConversionStatement(analyzer, tsFile);
        Assert.assertFalse(conversionStatement.isTsFilePhysicalPath());
      }
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testPipeConversionAllowsTsFileUnderInternalDataDirectory() throws Exception {
    final String[][] originalTierDataDirs =
        IoTDBDescriptor.getInstance().getConfig().getTierDataDirs();
    final Path dataDir = Files.createTempDirectory("load-pipe-internal-data");
    final Path tsFile = Files.createFile(dataDir.resolve("pipe-receiver.tsfile"));
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setTierDataDirs(new String[][] {{dataDir.toString()}});

      final LoadTsFileStatement treeStatement =
          LoadTsFileStatement.createForPipe(tsFile.toString());
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(treeStatement, true, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFileStatement conversionStatement =
            invokeCreateTreeConversionStatement(analyzer, tsFile.toFile());
        Assert.assertTrue(conversionStatement.isGeneratedByPipe());
      }

      final LoadTsFile tableStatement =
          LoadTsFile.createForPipe(null, tsFile.toString(), Collections.emptyMap());
      try (final LoadTsFileAnalyzer analyzer =
          new LoadTsFileAnalyzer(tableStatement, true, new MPPQueryContext(new QueryId("test")))) {
        final LoadTsFile conversionStatement =
            invokeCreateTableModelConversionStatement(analyzer, tsFile.toFile());
        Assert.assertTrue(conversionStatement.isGeneratedByPipe());
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setTierDataDirs(originalTierDataDirs);
      Assert.assertTrue(tsFile.toFile().delete());
      Assert.assertTrue(dataDir.toFile().delete());
    }
  }

  private static LoadTsFile invokeCreateTableModelConversionStatement(
      final LoadTsFileAnalyzer analyzer, final File tsFile) throws Exception {
    final Method method =
        LoadTsFileAnalyzer.class.getDeclaredMethod(
            "createTableModelConversionStatement", File.class);
    method.setAccessible(true);
    return (LoadTsFile) method.invoke(analyzer, tsFile);
  }

  private static LoadTsFileStatement invokeCreateTreeConversionStatement(
      final LoadTsFileAnalyzer analyzer, final File tsFile) throws Exception {
    final Method method =
        LoadTsFileAnalyzer.class.getDeclaredMethod("createTreeConversionStatement", File.class);
    method.setAccessible(true);
    return (LoadTsFileStatement) method.invoke(analyzer, tsFile);
  }
}
