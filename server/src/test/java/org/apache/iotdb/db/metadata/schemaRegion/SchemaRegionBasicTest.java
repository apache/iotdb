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
package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.ActivateTemplateInClusterPlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.DeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.PreDeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.RollbackPreDeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class define test cases for {@link ISchemaRegion}. All test cases will be run in both Memory
 * and Schema_File modes. In Schema_File mode, there are three kinds of test environment: full
 * memory, partial memory and non memory.
 */
public abstract class SchemaRegionBasicTest {

  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  boolean isClusterMode;

  @Before
  public void setUp() {
    isClusterMode = config.isClusterMode();
    config.setClusterMode(true);
    LocalConfigNode.getInstance().init();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setClusterMode(isClusterMode);
  }

  @Test
  public void testRatisModeSnapshot() throws Exception {
    String schemaRegionConsensusProtocolClass = config.getSchemaRegionConsensusProtocolClass();
    config.setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    try {
      PartialPath storageGroup = new PartialPath("root.sg");
      SchemaRegionId schemaRegionId = new SchemaRegionId(0);
      SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
      ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);

      File mLogFile =
          SystemFileFactory.INSTANCE.getFile(
              schemaRegion.getStorageGroupFullPath()
                  + File.separator
                  + schemaRegion.getSchemaRegionId().getId(),
              MetadataConstant.METADATA_LOG);
      Assert.assertFalse(mLogFile.exists());

      Map<String, String> tags = new HashMap<>();
      tags.put("tag-key", "tag-value");
      schemaRegion.createTimeseries(
          new CreateTimeSeriesPlan(
              new PartialPath("root.sg.d1.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              null,
              tags,
              null,
              null),
          -1);

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);

      Pair<List<ShowTimeSeriesResult>, Integer> result =
          schemaRegion.showTimeseries(
              new ShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value", 0, 0, false),
              null);

      ShowTimeSeriesResult seriesResult = result.left.get(0);
      Assert.assertEquals(new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getName());
      Map<String, String> resultTagMap = seriesResult.getTag();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

      LocalConfigNode.getInstance().clear();
      LocalConfigNode.getInstance().init();
      SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
      ISchemaRegion newSchemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
      newSchemaRegion.loadSnapshot(snapshotDir);
      result =
          newSchemaRegion.showTimeseries(
              new ShowTimeSeriesPlan(
                  new PartialPath("root.sg.**"), false, "tag-key", "tag-value", 0, 0, false),
              null);

      seriesResult = result.left.get(0);
      Assert.assertEquals(new PartialPath("root.sg.d1.s1").getFullPath(), seriesResult.getName());
      resultTagMap = seriesResult.getTag();
      Assert.assertEquals(1, resultTagMap.size());
      Assert.assertEquals("tag-value", resultTagMap.get("tag-key"));

    } finally {
      config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }
  }

  @Test
  @Ignore
  public void testSnapshotPerformance() throws Exception {
    String schemaRegionConsensusProtocolClass = config.getSchemaRegionConsensusProtocolClass();
    config.setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    try {
      PartialPath storageGroup = new PartialPath("root.sg");
      SchemaRegionId schemaRegionId = new SchemaRegionId(0);
      SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
      ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);

      Map<String, String> tags = new HashMap<>();
      tags.put("tag-key", "tag-value");

      long time = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        for (int j = 0; j < 1000; j++) {
          schemaRegion.createTimeseries(
              new CreateTimeSeriesPlan(
                  new PartialPath("root.sg.d" + i + ".s" + j),
                  TSDataType.INT32,
                  TSEncoding.PLAIN,
                  CompressionType.UNCOMPRESSED,
                  null,
                  tags,
                  null,
                  null),
              -1);
        }
      }
      System.out.println(
          "Timeseries creation costs " + (System.currentTimeMillis() - time) + "ms.");

      File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
      snapshotDir.mkdir();
      schemaRegion.createSnapshot(snapshotDir);

      schemaRegion.loadSnapshot(snapshotDir);
    } finally {
      config.setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
    }
  }

  @Test
  public void testFetchSchema() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);

    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
                put("tag2", "t2");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
                put("attr2", "a2");
              }
            },
            "temp"),
        -1);
    List<MeasurementPath> schemas =
        schemaRegion.fetchSchema(
            new PartialPath("root.sg.wf01.wt01.*"), Collections.EMPTY_MAP, true);
    Assert.assertEquals(schemas.size(), 2);
    for (MeasurementPath measurementPath : schemas) {
      if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.status")) {
        Assert.assertTrue(StringUtils.isEmpty(measurementPath.getMeasurementAlias()));
        Assert.assertEquals(0, measurementPath.getTagMap().size());
        Assert.assertEquals(TSDataType.BOOLEAN, measurementPath.getMeasurementSchema().getType());
        Assert.assertEquals(
            TSEncoding.PLAIN, measurementPath.getMeasurementSchema().getEncodingType());
        Assert.assertEquals(
            CompressionType.SNAPPY, measurementPath.getMeasurementSchema().getCompressor());
      } else if (measurementPath.getFullPath().equals("root.sg.wf01.wt01.temperature")) {
        // only when user query with alias, the alias in path will be set
        Assert.assertEquals("", measurementPath.getMeasurementAlias());
        Assert.assertEquals(2, measurementPath.getTagMap().size());
        Assert.assertEquals(TSDataType.FLOAT, measurementPath.getMeasurementSchema().getType());
        Assert.assertEquals(
            TSEncoding.RLE, measurementPath.getMeasurementSchema().getEncodingType());
        Assert.assertEquals(
            CompressionType.GZIP, measurementPath.getMeasurementSchema().getCompressor());
      } else {
        Assert.fail("Unexpected MeasurementPath " + measurementPath);
      }
    }
    schemas =
        schemaRegion.fetchSchema(
            new PartialPath("root.sg.wf01.wt01.temp"), Collections.EMPTY_MAP, false);
    Assert.assertEquals(schemas.size(), 1);
    Assert.assertEquals("root.sg.wf01.wt01.temperature", schemas.get(0).getFullPath());
    Assert.assertEquals("temp", schemas.get(0).getMeasurementAlias());
    Assert.assertNull(schemas.get(0).getTagMap());
    Assert.assertEquals(TSDataType.FLOAT, schemas.get(0).getMeasurementSchema().getType());
    Assert.assertEquals(TSEncoding.RLE, schemas.get(0).getMeasurementSchema().getEncodingType());
    Assert.assertEquals(
        CompressionType.GZIP, schemas.get(0).getMeasurementSchema().getCompressor());
  }

  @Test
  public void testCheckMeasurementExistence() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
                put("tag2", "t2");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
                put("attr2", "a2");
              }
            },
            "temp"),
        -1);
    // all non exist
    Map<Integer, MetadataException> res1 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01.wt01"),
            IntStream.range(0, 5).mapToObj(i -> "s" + i).collect(Collectors.toList()),
            IntStream.range(0, 5).mapToObj(i -> "alias" + i).collect(Collectors.toList()));
    Assert.assertEquals(0, res1.size());
    Map<Integer, MetadataException> res2 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01"),
            Collections.singletonList("wt01"),
            Collections.singletonList("alias1"));
    Assert.assertEquals(0, res2.size());
    // all exist
    Map<Integer, MetadataException> res3 =
        schemaRegion.checkMeasurementExistence(
            new PartialPath("root.sg.wf01.wt01"),
            Arrays.asList("status", "s1", "v1"),
            Arrays.asList("", "temp", ""));
    Assert.assertEquals(3, res3.size());
    Assert.assertTrue(res3.get(0) instanceof MeasurementAlreadyExistException);
    Assert.assertTrue(res3.get(1) instanceof AliasAlreadyExistException);
    Assert.assertTrue(res3.get(2) instanceof PathAlreadyExistException);
  }

  @Test
  public void testConstructSchemaBlackList() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt02.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf02.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.wt01.*"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf01.*.status"));
    patternTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    patternTree.constructTree();
    Assert.assertTrue(schemaRegion.constructSchemaBlackList(patternTree) >= 3);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.*"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"),
                new PartialPath("root.sg.wf02.wt01.temperature"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    PathPatternTree rollbackTree = new PathPatternTree();
    rollbackTree.appendPathPattern(new PartialPath("root.sg.wf02.wt01.temperature"));
    rollbackTree.constructTree();
    schemaRegion.rollbackSchemaBlackList(rollbackTree);
    Assert.assertEquals(
        new HashSet<>(
            Arrays.asList(
                new PartialPath("root.sg.wf01.wt01.*"),
                new PartialPath("root.sg.wf01.wt02.status"),
                new PartialPath("root.sg.wf01.wt01.status"))),
        schemaRegion.fetchSchemaBlackList(patternTree));
    schemaRegion.deleteTimeseriesInBlackList(patternTree);
    List<MeasurementPath> schemas =
        schemaRegion.fetchSchema(new PartialPath("root.**"), Collections.EMPTY_MAP, false);
    Assert.assertEquals(1, schemas.size());
    Assert.assertEquals("root.sg.wf02.wt01.temperature", schemas.get(0).getFullPath());
  }

  /** Test {@link ISchemaRegion#activateSchemaTemplate}. */
  @Test
  public void testActivateSchemaTemplate() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    int templateId = 1;
    Template template =
        new Template(
            new CreateSchemaTemplateStatement(
                "t1",
                new String[][] {new String[] {"s1"}, new String[] {"s2"}},
                new TSDataType[][] {
                  new TSDataType[] {TSDataType.DOUBLE}, new TSDataType[] {TSDataType.INT32}
                },
                new TSEncoding[][] {
                  new TSEncoding[] {TSEncoding.RLE}, new TSEncoding[] {TSEncoding.RLE}
                },
                new CompressionType[][] {
                  new CompressionType[] {CompressionType.SNAPPY},
                  new CompressionType[] {CompressionType.SNAPPY}
                }));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        new ActivateTemplateInClusterPlanImpl(new PartialPath("root.sg.wf01.wt01"), 3, templateId),
        template);
    schemaRegion.activateSchemaTemplate(
        new ActivateTemplateInClusterPlanImpl(new PartialPath("root.sg.wf02"), 2, templateId),
        template);
    Set<String> expectedPaths = new HashSet<>(Arrays.asList("root.sg.wf01.wt01", "root.sg.wf02"));
    Set<String> pathsUsingTemplate =
        new HashSet<>(schemaRegion.getPathsUsingTemplate(new PartialPath("root.**"), templateId));
    Assert.assertEquals(expectedPaths, pathsUsingTemplate);
    PathPatternTree allPatternTree = new PathPatternTree();
    allPatternTree.appendPathPattern(new PartialPath("root.**"));
    allPatternTree.constructTree();
    Assert.assertEquals(2, schemaRegion.countPathsUsingTemplate(templateId, allPatternTree));
    PathPatternTree wf01PatternTree = new PathPatternTree();
    wf01PatternTree.appendPathPattern(new PartialPath("root.sg.wf01.*"));
    wf01PatternTree.constructTree();
    Assert.assertEquals(1, schemaRegion.countPathsUsingTemplate(templateId, wf01PatternTree));
    Assert.assertEquals(
        "root.sg.wf01.wt01",
        schemaRegion.getPathsUsingTemplate(new PartialPath("root.sg.wf01.*"), templateId).get(0));
    PathPatternTree wf02PatternTree = new PathPatternTree();
    wf02PatternTree.appendPathPattern(new PartialPath("root.sg.wf02"));
    wf02PatternTree.constructTree();
    Assert.assertEquals(1, schemaRegion.countPathsUsingTemplate(templateId, wf02PatternTree));
    Assert.assertEquals(
        "root.sg.wf02",
        schemaRegion.getPathsUsingTemplate(new PartialPath("root.sg.wf02"), templateId).get(0));
  }

  /**
   * Test {@link ISchemaRegion#constructSchemaBlackListWithTemplate}, {@link
   * ISchemaRegion#rollbackSchemaBlackListWithTemplate} and {@link
   * ISchemaRegion#deactivateTemplateInBlackList}
   */
  @Test
  public void testDeactivateTemplate() throws Exception {
    PartialPath storageGroup = new PartialPath("root.sg");
    SchemaRegionId schemaRegionId = new SchemaRegionId(0);
    SchemaEngine.getInstance().createSchemaRegion(storageGroup, schemaRegionId);
    ISchemaRegion schemaRegion = SchemaEngine.getInstance().getSchemaRegion(schemaRegionId);
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath("root.sg.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    int templateId = 1;
    Template template =
        new Template(
            new CreateSchemaTemplateStatement(
                "t1",
                new String[][] {new String[] {"s1"}, new String[] {"s2"}},
                new TSDataType[][] {
                  new TSDataType[] {TSDataType.DOUBLE}, new TSDataType[] {TSDataType.INT32}
                },
                new TSEncoding[][] {
                  new TSEncoding[] {TSEncoding.RLE}, new TSEncoding[] {TSEncoding.RLE}
                },
                new CompressionType[][] {
                  new CompressionType[] {CompressionType.SNAPPY},
                  new CompressionType[] {CompressionType.SNAPPY}
                }));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        new ActivateTemplateInClusterPlanImpl(new PartialPath("root.sg.wf01.wt01"), 3, templateId),
        template);
    schemaRegion.activateSchemaTemplate(
        new ActivateTemplateInClusterPlanImpl(new PartialPath("root.sg.wf02"), 2, templateId),
        template);

    // construct schema blacklist with template on root.sg.wf01.wt01 and root.sg.wf02
    Map<PartialPath, List<Integer>> allDeviceTemplateMap = new HashMap<>();
    allDeviceTemplateMap.put(new PartialPath("root.**"), Collections.singletonList(templateId));
    schemaRegion.constructSchemaBlackListWithTemplate(
        new PreDeactivateTemplatePlanImpl(allDeviceTemplateMap));

    // rollback schema blacklist with template on root.sg.wf02
    Map<PartialPath, List<Integer>> wf02TemplateMap = new HashMap<>();
    wf02TemplateMap.put(new PartialPath("root.sg.wf02"), Collections.singletonList(templateId));
    schemaRegion.rollbackSchemaBlackListWithTemplate(
        new RollbackPreDeactivateTemplatePlanImpl(wf02TemplateMap));

    // deactivate schema blacklist with template on root.sg.wf01.wt01
    schemaRegion.deactivateTemplateInBlackList(
        new DeactivateTemplatePlanImpl(allDeviceTemplateMap));

    // check using getPathsUsingTemplate
    List<String> expectedPaths = Collections.singletonList("root.sg.wf02");
    List<String> pathsUsingTemplate =
        schemaRegion.getPathsUsingTemplate(new PartialPath("root.**"), templateId);
    Assert.assertEquals(expectedPaths, pathsUsingTemplate);
    // check using countPathsUsingTemplate
    PathPatternTree allPatternTree = new PathPatternTree();
    allPatternTree.appendPathPattern(new PartialPath("root.**"));
    allPatternTree.constructTree();
    Assert.assertEquals(1, schemaRegion.countPathsUsingTemplate(templateId, allPatternTree));
  }
}
