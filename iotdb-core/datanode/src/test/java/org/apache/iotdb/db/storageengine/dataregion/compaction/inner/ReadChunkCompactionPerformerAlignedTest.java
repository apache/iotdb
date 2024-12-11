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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.TestUtilsForAlignedSeries;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionConfigRestorer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
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
  }

  @After
  public void tearDown() throws Exception {
    new CompactionConfigRestorer().restoreCompactionConfig();
    FileUtils.forceDelete(dataDirectory);
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
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
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    List<IFullPath> fullPaths = new ArrayList<>();
    List<IMeasurementSchema> iMeasurementSchemas = new ArrayList<>();
    List<String> measurementIds = new ArrayList<>();
    schemas.forEach(
        (e) -> {
          measurementIds.add(e.getMeasurementName());
        });
    for (String device : devices) {
      iMeasurementSchemas.addAll(schemas);
      fullPaths.add(
          new AlignedFullPath(
              IDeviceID.Factory.DEFAULT_FACTORY.create(device), measurementIds, schemas));
    }
    Map<IFullPath, List<TimeValuePair>> originData =
        CompactionCheckerUtils.getDataByQuery(fullPaths, resources, new ArrayList<>());
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    CompactionUtils.moveTargetFile(
        Collections.singletonList(targetResource), CompactionTaskType.INNER_SEQ, storageGroup);
    Map<IFullPath, List<TimeValuePair>> compactedData =
        CompactionCheckerUtils.getDataByQuery(
            fullPaths, Collections.singletonList(targetResource), new ArrayList<>());
    CompactionCheckerUtils.validDataByValueList(originData, compactedData);
  }

  @Test
  public void testEmptyChunkWithModification() throws Exception {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s0", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("s1", TSDataType.FLOAT));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemas.add(new MeasurementSchema("s3", TSDataType.INT32));
    schemas.add(new MeasurementSchema("s4", TSDataType.TEXT));
    schemas.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
    Map<PartialPath, List<TimeValuePair>> originData = new HashMap<>();
    List<TsFileResource> resources = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      TsFileIOWriter writer =
          new TsFileIOWriter(new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(schemas);
      for (int j = i * 100; j < i * 100 + 100; j++) {
        TsPrimitiveType[] values = {
          new TsPrimitiveType.TsDouble(0.0D),
          new TsPrimitiveType.TsFloat(0.0F),
          null,
          null,
          new TsPrimitiveType.TsBinary(new Binary("", TSFileConfig.STRING_CHARSET)),
          new TsPrimitiveType.TsBoolean(false)
        };
        originData
            .computeIfAbsent(new PartialPath("root.sg.d1.s0"), k -> new ArrayList<>())
            .add(new TimeValuePair(j, values[0]));
        originData
            .computeIfAbsent(new PartialPath("root.sg.d1.s1"), k -> new ArrayList<>())
            .add(new TimeValuePair(j, values[1]));
        originData.computeIfAbsent(new PartialPath("root.sg.d1.s2"), k -> null);
        originData.computeIfAbsent(new PartialPath("root.sg.d1.s3"), k -> null);
        originData
            .computeIfAbsent(new PartialPath("root.sg.d1.s4"), k -> new ArrayList<>())
            .add(new TimeValuePair(j, values[4]));
        originData
            .computeIfAbsent(new PartialPath("root.sg.d1.s5"), k -> new ArrayList<>())
            .add(new TimeValuePair(j, values[5]));
        alignedChunkWriter.write(j, values);
      }
      writer.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"));
      alignedChunkWriter.writeToFileWriter(writer);
      writer.endChunkGroup();
      writer.endFile();
      TsFileResource resource = new TsFileResource(writer.getFile(), TsFileResourceStatus.NORMAL);
      resource
          .getModFileForWrite()
          .write(new TreeDeletionEntry(new MeasurementPath("root.sg.d1.*"), i * 100, i * 100 + 20));
      resource.getModFileForWrite().close();
      int finalI = i;
      originData.forEach(
          (x, y) ->
              y.removeIf(
                  timeValuePair ->
                      timeValuePair.getTimestamp() >= finalI * 100
                          && timeValuePair.getTimestamp() < finalI * 100 + 20));
      resources.add(resource);
    }
    performer.setSourceFiles(resources);
    TsFileResource targetResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    performer.setSummary(new CompactionTaskSummary());
    performer.perform();
    Assert.assertTrue(targetResource.getTsFile().exists());
    RestorableTsFileIOWriter checkWriter = new RestorableTsFileIOWriter(targetResource.getTsFile());
    Assert.assertFalse(checkWriter.hasCrashed());
  }
}
