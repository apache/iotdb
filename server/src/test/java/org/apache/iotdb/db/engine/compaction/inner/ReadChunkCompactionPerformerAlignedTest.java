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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.TestUtilsForAlignedSeries;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ReadChunkCompactionPerformerAlignedTest {
  private static final String storageGroup = "root.testAlignedCompaction";

  private final ICompactionPerformer performer = new ReadChunkCompactionPerformer();
  private static File dataDirectory =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data".concat(File.separator)
              + "sequence".concat(File.separator)
              + storageGroup.concat(File.separator)
              + "0".concat(File.separator)
              + "0".concat(File.separator));

  @Before
  public void setUp() throws Exception {
    if (!dataDirectory.exists()) {
      Assert.assertTrue(dataDirectory.mkdirs());
    }
    IoTDB.configManager.init();
  }

  @After
  public void tearDown() throws Exception {
    new CompactionConfigRestorer().restoreCompactionConfig();
    FileUtils.forceDelete(dataDirectory);
    IoTDB.configManager.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSimpleAlignedTsFileCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {true, true, true, true, true};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {false, false, false, false, false};
    int timeInterval = 500;
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas.toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithModificationCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {true, true, true, true, true};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {false, false, false, false, false};
    int timeInterval = 500;
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas.toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      Pair<Long, Long> deleteInterval = new Pair<>(timeInterval * i + 10L, timeInterval * i + 20L);
      Map<String, Pair<Long, Long>> deletionMap = new HashMap<>();
      for (String device : devices) {
        deletionMap.put(device + ".s0", deleteInterval);
      }
      CompactionFileGeneratorUtils.generateMods(deletionMap, resource, false);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithNullValueCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {true, true, true, true, true};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas.toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithDifferentSchemaInDifferentTsFileCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {true, true, true, true, true};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    Random random = new Random(1);
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas
              .subList(0, random.nextInt(schemas.size() - 1) + 1)
              .toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithDifferentDataTypeCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {false, true, false, true, false};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    Random random = new Random(1);
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[] {}),
          schemas.toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithDifferentDataTypeInDifferentTsFileCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      devices.add(storageGroup + ".d" + i);
    }
    boolean[] aligned = new boolean[] {false, true, false, true, false};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    Random random = new Random(5);
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.subList(0, random.nextInt(devices.size() - 1) + 1).toArray(new String[0]),
          schemas
              .subList(0, random.nextInt(schemas.size() - 1) + 1)
              .toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithBadSchemaCompaction() throws Exception {
    List<String> devices = new ArrayList<>();
    devices.add(storageGroup + ".d" + 0);
    for (int i = 1; i < 5; ++i) {
      devices.add(devices.get(i - 1) + ".d" + i);
    }
    boolean[] aligned = new boolean[] {false, true, false, true, false};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    Random random = new Random(5);
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 31; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.subList(0, random.nextInt(devices.size() - 1) + 1).toArray(new String[0]),
          schemas
              .subList(0, random.nextInt(schemas.size() - 1) + 1)
              .toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testAlignedTsFileWithEmptyChunkGroup() throws Exception {
    List<String> devices = new ArrayList<>();
    devices.add(storageGroup + ".d" + 0);
    for (int i = 1; i < 5; ++i) {
      devices.add(devices.get(i - 1) + ".d" + i);
    }
    boolean[] aligned = new boolean[] {false, true, false, true, false};
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));

    TestUtilsForAlignedSeries.registerTimeSeries(
        storageGroup,
        devices.toArray(new String[] {}),
        schemas.toArray(new IMeasurementSchema[] {}),
        aligned);

    boolean[] randomNull = new boolean[] {true, false, true, false, true};
    int timeInterval = 500;
    Random random = new Random(5);
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i < 30; i++) {
      TsFileResource resource =
          new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      TestUtilsForAlignedSeries.writeTsFile(
          devices.toArray(new String[0]),
          schemas.toArray(new IMeasurementSchema[0]),
          resource,
          aligned,
          timeInterval * i,
          timeInterval * (i + 1),
          randomNull);
      resources.add(resource);
    }
    TsFileResource resource =
        new TsFileResource(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", 30, 30)));
    // the start time and end time is the same
    // it will write tsfile with empty chunk group
    TestUtilsForAlignedSeries.writeTsFile(
        devices.toArray(new String[0]),
        schemas.toArray(new IMeasurementSchema[0]),
        resource,
        aligned,
        timeInterval * (30 + 1),
        timeInterval * (30 + 1),
        randomNull);
    resources.add(resource);
    TsFileResource targetResource = new TsFileResource(new File(dataDirectory, "1-1-1-0.tsfile"));
    List<PartialPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementId());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(new AlignedPath(device, measurementIds, schemas));
    }
    Map<PartialPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, iMeasurementSchemas, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(Collections.singletonList(targetResource), true, storageGroup);
    Map<PartialPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths,
            iMeasurementSchemas,
            Collections.singletonList(targetResource),
            new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }
}
