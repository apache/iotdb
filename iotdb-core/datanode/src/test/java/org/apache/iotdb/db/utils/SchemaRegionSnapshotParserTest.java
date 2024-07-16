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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SchemaRegionSnapshotParserTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private SchemaRegionSnapshotParserTestParams rawConfig;

  protected final SchemaRegionSnapshotParserTestParams testParams;

  protected static class SchemaRegionSnapshotParserTestParams {
    private final String testModeName;
    private final String schemaRegionMode;

    private SchemaRegionSnapshotParserTestParams(String testModeName, String schemaEngineMode) {
      this.testModeName = testModeName;
      this.schemaRegionMode = schemaEngineMode;
    }

    public String getTestModeName() {
      return this.testModeName;
    }

    public String getSchemaRegionMode() {
      return this.schemaRegionMode;
    }

    @Override
    public String toString() {
      return testModeName;
    }
  }

  private String snapshotFileName;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchemaRegionSnapshotParserTestParams> getTestModes() {
    return Arrays.asList(
        new SchemaRegionSnapshotParserTestParams("MemoryMode", "Memory"),
        new SchemaRegionSnapshotParserTestParams("PBTree", "PBTree"));
  }

  @Before
  public void setUp() throws Exception {
    rawConfig =
        new SchemaRegionSnapshotParserTestParams("Raw-Config", COMMON_CONFIG.getSchemaEngineMode());
    COMMON_CONFIG.setSchemaEngineMode(testParams.schemaRegionMode);
    SchemaEngine.getInstance().init();
    if (testParams.schemaRegionMode.equals("Memory")) {
      snapshotFileName = SchemaConstant.MTREE_SNAPSHOT;
    } else if (testParams.schemaRegionMode.equals("PBTree")) {
      snapshotFileName = SchemaConstant.PBTREE_SNAPSHOT;
    }
  }

  @After
  public void tearDown() throws Exception {
    SchemaEngine.getInstance().clear();
    cleanEnv();
    COMMON_CONFIG.setSchemaEngineMode(rawConfig.schemaRegionMode);
  }

  protected void cleanEnv() throws IOException {
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  public SchemaRegionSnapshotParserTest(SchemaRegionSnapshotParserTestParams params) {
    this.testParams = params;
  }

  public ISchemaRegion getSchemaRegion(String database, int schemaRegionId) throws Exception {
    SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(new PartialPath(database), regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  @Test
  public void testSimpleTranslateSnapshot() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    PartialPath databasePath = new PartialPath("root.sg");
    // Tree in memtree:
    // root->sg->s1->g1->temp
    //          |     |->status
    //          |->s2->g2->t2->temp
    //              |->g4->status
    //              |->g5->level
    HashMap<String, ICreateTimeSeriesPlan> planMap = new HashMap<>();
    planMap.put(
        "root.sg.s1.g1.temp",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s1.g1.status",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.status"),
            TSDataType.INT64,
            TSEncoding.TS_2DIFF,
            CompressionType.LZ4,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g2.t2.temp",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g2.t2.temp"),
            TSDataType.DOUBLE,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g4.status",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g4.status"),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.ZSTD,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g5.level",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g5.level"),
            TSDataType.INT32,
            TSEncoding.GORILLA,
            CompressionType.LZMA2,
            null,
            null,
            null,
            null));
    for (ICreateTimeSeriesPlan plan : planMap.values()) {
      schemaRegion.createTimeSeries(plan, -1);
    }

    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);

    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            null,
            databasePath);

    assert statements != null;
    for (Statement stmt : statements) {
      CreateTimeSeriesStatement createTimeSeriesStatement = (CreateTimeSeriesStatement) stmt;
      ICreateTimeSeriesPlan plan =
          planMap.get(createTimeSeriesStatement.getPaths().get(0).toString());
      Assert.assertEquals(plan.getEncoding(), createTimeSeriesStatement.getEncoding());
      Assert.assertEquals(plan.getCompressor(), createTimeSeriesStatement.getCompressor());
      Assert.assertEquals(plan.getDataType(), createTimeSeriesStatement.getDataType());
      Assert.assertEquals(plan.getAlias(), createTimeSeriesStatement.getAlias());
      Assert.assertEquals(plan.getProps(), createTimeSeriesStatement.getProps());
      Assert.assertEquals(plan.getAttributes(), createTimeSeriesStatement.getAttributes());
      Assert.assertEquals(plan.getTags(), createTimeSeriesStatement.getTags());
    }
    statements.checkException();
  }

  @Test
  public void testAlignedTimeseriesTranslateSnapshot() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    PartialPath database = new PartialPath("root.sg");
    ICreateAlignedTimeSeriesPlan plan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.t1.t2"),
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE, TSEncoding.PLAIN),
            Arrays.asList(
                CompressionType.SNAPPY, CompressionType.LZ4, CompressionType.UNCOMPRESSED),
            Arrays.asList("alias1", "alias2", null),
            Arrays.asList(
                new HashMap<String, String>() {
                  {
                    put("tag1", "t1");
                    put("tag_1", "value2");
                  }
                },
                new HashMap<String, String>() {
                  {
                    put("tag2", "t2");
                    put("tag_2", "t_2");
                  }
                },
                new HashMap<>()), // not all measurements have tag
            Arrays.asList(
                new HashMap<String, String>() {
                  {
                    put("attr1", "a1");
                    put("attr_1", "a_1");
                  }
                },
                new HashMap<String, String>() {
                  {
                    put("attr2", "a2");
                  }
                },
                new HashMap<String, String>() {
                  {
                    put("tag2", "t2");
                    put("tag_2", "t_2");
                  }
                }));
    schemaRegion.createAlignedTimeSeries(plan);
    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + SchemaConstant.TAG_LOG_SNAPSHOT),
            database);
    assert statements != null;
    for (Statement stmt : statements) {
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement =
          (CreateAlignedTimeSeriesStatement) stmt;
      Assert.assertEquals(plan.getDevicePath(), createAlignedTimeSeriesStatement.getDevicePath());

      Assert.assertEquals(
          plan.getTagsList().size(), createAlignedTimeSeriesStatement.getTagsList().size());
      Assert.assertEquals(
          plan.getAliasList().size(), createAlignedTimeSeriesStatement.getAliasList().size());
      Assert.assertEquals(
          plan.getAttributesList().size(),
          createAlignedTimeSeriesStatement.getAttributesList().size());
      Assert.assertEquals(
          createAlignedTimeSeriesStatement.getMeasurements().size(),
          createAlignedTimeSeriesStatement.getAttributesList().size());
      Assert.assertEquals(
          createAlignedTimeSeriesStatement.getMeasurements().size(),
          createAlignedTimeSeriesStatement.getTagsList().size());

      Comparator<Map<String, String>> comp =
          new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> o1, Map<String, String> o2) {
              if (o1 == null && o2 == null) {
                return 0;
              }
              if (o1 == null) {
                return -1;
              }
              if (o2 == null) {
                return 1;
              }
              return Integer.compare(o1.hashCode(), o2.hashCode());
            }
          };

      Comparator<String> comp_str =
          new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
              if (o1 == null && o2 == null) {
                return 0;
              }
              if (o1 == null) {
                return -1;
              }
              if (o2 == null) {
                return 1;
              }
              return Integer.compare(o1.hashCode(), o2.hashCode());
            }
          };

      Collections.sort(plan.getAliasList(), comp_str);
      Collections.sort(createAlignedTimeSeriesStatement.getAliasList(), comp_str);
      Collections.sort(plan.getAttributesList(), comp);
      Collections.sort(createAlignedTimeSeriesStatement.getAttributesList(), comp);
      Collections.sort(plan.getMeasurements(), comp_str);
      Collections.sort(createAlignedTimeSeriesStatement.getMeasurements(), comp_str);
      Collections.sort(plan.getTagsList(), comp);
      Collections.sort(createAlignedTimeSeriesStatement.getTagsList(), comp);
      Collections.sort(plan.getEncodings());
      Collections.sort(createAlignedTimeSeriesStatement.getEncodings());
      Collections.sort(plan.getCompressors());
      Collections.sort(createAlignedTimeSeriesStatement.getCompressors());
      Collections.sort(plan.getDataTypes());
      Collections.sort(createAlignedTimeSeriesStatement.getDataTypes());
      Assert.assertEquals(
          plan.getMeasurements(), createAlignedTimeSeriesStatement.getMeasurements());
      Assert.assertEquals(plan.getAliasList(), createAlignedTimeSeriesStatement.getAliasList());
      Assert.assertEquals(plan.getEncodings(), createAlignedTimeSeriesStatement.getEncodings());
      Assert.assertEquals(plan.getCompressors(), createAlignedTimeSeriesStatement.getCompressors());
      Assert.assertEquals(
          plan.getAttributesList(), createAlignedTimeSeriesStatement.getAttributesList());
      Assert.assertEquals(plan.getTagsList(), createAlignedTimeSeriesStatement.getTagsList());
    }
    statements.checkException();
  }

  @Test
  public void testTemplateActivateTranslateSnapshot() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    PartialPath databasePath = new PartialPath("root.sg");
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g3.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    Template template =
        new Template(
            "t1",
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.GZIP));
    template.setId(0);
    HashMap<String, IActivateTemplateInClusterPlan> planMap = new HashMap<>();
    IActivateTemplateInClusterPlan plan1 =
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.s2"), 1, template.getId());
    IActivateTemplateInClusterPlan plan2 =
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.s3"), 1, template.getId());
    planMap.put("root.sg.s2", plan1);
    planMap.put("root.sg.s3", plan2);
    schemaRegion.activateSchemaTemplate(plan1, template);
    schemaRegion.activateSchemaTemplate(plan2, template);
    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);

    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            null,
            databasePath);
    int count = 0;
    assert statements != null;
    for (Statement stmt : statements) {
      if (stmt instanceof ActivateTemplateStatement) {
        ActivateTemplateStatement ATStatement = (ActivateTemplateStatement) stmt;
        IActivateTemplateInClusterPlan plan = planMap.get(ATStatement.getPath().toString());
        Assert.assertEquals(plan.getActivatePath(), ATStatement.getPath());
        count++;
      }
    }
    Assert.assertEquals(2, count);
    statements.checkException();
  }

  @Test
  public void testComplicatedSnapshotParser() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }

    // ----------------------------------------------------------------------
    //                            Schema Tree
    // ----------------------------------------------------------------------
    // This test will construct a complicated mtree. This tree will have
    // aligned timeseries, tags and attributes, normal timeseries device template.
    //
    //
    //
    //                          status(BOOLEAN, RLE) alias(stat)
    //                         /
    //                      t2------temperature(INT64, TS_2DIFF,LZ4)
    //                     /
    //          sg1------s1------t1(activate template: t1)
    //         /
    // root ->|
    //         \
    //          sg2-------t1(aligned)------status(INT64, TS_2DIFF, LZMA2){attr1:atr1}
    //            \
    //             t2-------level{tags:"tag1"="t1", attributes: "attri1"="attr1"}
    //              \
    //                t1(aligned)-------temperature(INT32, TS_2DIFF, LZ4){attributes:"attr1"="a1"}
    //                     \
    //                      level(INT32m RLE){tags:"tag1"="t1"} alias(lev)
    //
    //
    ISchemaRegion schemaRegion = getSchemaRegion("root", 0);
    PartialPath databasePath = new PartialPath("root");
    Template template = new Template();
    template.setId(1);
    template.addMeasurement("date", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    HashMap<String, ISchemaRegionPlan> planMap = new HashMap<>();
    planMap.put(
        "root.sg1.s1.t1",
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg1.s1.t1"), 3, 1));
    planMap.put(
        "root.sg1.s1.t2.temperature",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg1.s1.t2.temperature"),
            TSDataType.INT64,
            TSEncoding.TS_2DIFF,
            CompressionType.LZ4,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg1.s1.t2.status",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg1.s1.t2.status"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            "statusA"));
    planMap.put(
        "root.sg2.t1",
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg2.t1"),
            new ArrayList<String>() {
              {
                add("status");
              }
            },
            new ArrayList<TSDataType>() {
              {
                add(TSDataType.INT64);
              }
            },
            new ArrayList<TSEncoding>() {
              {
                add(TSEncoding.TS_2DIFF);
              }
            },
            new ArrayList<CompressionType>() {
              {
                add(CompressionType.SNAPPY);
              }
            },
            new ArrayList<String>() {
              {
                add("stat");
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(new HashMap<>());
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("attr1", "a1");
                      }
                    });
              }
            }));
    planMap.put(
        "root.sg2.t2.level",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg2.t2.level"),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.UNCOMPRESSED,
            null,
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
              }
            },
            new HashMap<String, String>() {
              {
                put("attri1", "atr1");
              }
            },
            null));
    planMap.put(
        "root.sg2.t2.t1",
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg2.t2.t1"),
            new ArrayList<String>() {
              {
                add("temperature");
                add("level");
              }
            },
            new ArrayList<TSDataType>() {
              {
                add(TSDataType.INT64);
                add(TSDataType.INT32);
              }
            },
            new ArrayList<TSEncoding>() {
              {
                add(TSEncoding.RLE);
                add(TSEncoding.RLE);
              }
            },
            new ArrayList<CompressionType>() {
              {
                add(CompressionType.SNAPPY);
                add(CompressionType.UNCOMPRESSED);
              }
            },
            new ArrayList<String>() {
              {
                add(null);
                add("lev");
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(new HashMap<>());
                add(
                    new HashMap<String, String>() {
                      {
                        put("tag1", "t1");
                      }
                    });
              }
            },
            new ArrayList<Map<String, String>>() {
              {
                add(
                    new HashMap<String, String>() {
                      {
                        put("attr1", "a1");
                      }
                    });
                add(new HashMap<>());
              }
            }));
    for (ISchemaRegionPlan plan : planMap.values()) {
      if (plan instanceof ICreateTimeSeriesPlan) {
        schemaRegion.createTimeSeries((ICreateTimeSeriesPlan) plan, -1);
      } else if (plan instanceof ICreateAlignedTimeSeriesPlan) {
        schemaRegion.createAlignedTimeSeries((ICreateAlignedTimeSeriesPlan) plan);
      } else if (plan instanceof IActivateTemplateInClusterPlan) {
        schemaRegion.activateSchemaTemplate((IActivateTemplateInClusterPlan) plan, template);
      }
    }

    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);

    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + SchemaConstant.TAG_LOG_SNAPSHOT),
            databasePath);
    assert statements != null;
    int count = 0;
    Comparator<String> comparator =
        new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            if (o1 == null && o2 == null) {
              return 0;
            } else if (o1 == null) {
              return 1;
            } else if (o2 == null) {
              return -1;
            } else {
              return o1.compareTo(o2);
            }
          }
        };
    for (Statement stmt : statements) {
      if (stmt instanceof CreateAlignedTimeSeriesStatement) {
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement =
            (CreateAlignedTimeSeriesStatement) stmt;
        ICreateAlignedTimeSeriesPlan plan =
            (ICreateAlignedTimeSeriesPlan)
                planMap.get(createAlignedTimeSeriesStatement.getDevicePath().toString());
        Assert.assertNotNull(plan);
        Collections.sort(plan.getMeasurements());
        Collections.sort(createAlignedTimeSeriesStatement.getMeasurements());
        Assert.assertEquals(
            plan.getMeasurements(), createAlignedTimeSeriesStatement.getMeasurements());
        Collections.sort(plan.getAliasList(), comparator);
        Collections.sort(createAlignedTimeSeriesStatement.getAliasList(), comparator);
        Assert.assertEquals(plan.getAliasList(), createAlignedTimeSeriesStatement.getAliasList());
        Assert.assertEquals(plan.getEncodings(), createAlignedTimeSeriesStatement.getEncodings());
        Collections.sort(plan.getCompressors());
        Collections.sort(createAlignedTimeSeriesStatement.getCompressors());
        Assert.assertEquals(
            plan.getCompressors(), createAlignedTimeSeriesStatement.getCompressors());
        Assert.assertEquals(
            plan.getAttributesList().size(),
            createAlignedTimeSeriesStatement.getAttributesList().size());
        Assert.assertEquals(
            plan.getTagsList().size(), createAlignedTimeSeriesStatement.getTagsList().size());
      } else if (stmt instanceof CreateTimeSeriesStatement) {
        CreateTimeSeriesStatement createTimeSeriesStatement = (CreateTimeSeriesStatement) stmt;
        ICreateTimeSeriesPlan plan =
            (ICreateTimeSeriesPlan) planMap.get(createTimeSeriesStatement.getPath().toString());
        Assert.assertNotNull(plan);
        Assert.assertEquals(plan.getEncoding(), createTimeSeriesStatement.getEncoding());
        Assert.assertEquals(plan.getCompressor(), createTimeSeriesStatement.getCompressor());
        Assert.assertEquals(plan.getDataType(), createTimeSeriesStatement.getDataType());
        Assert.assertEquals(plan.getAlias(), createTimeSeriesStatement.getAlias());
        Assert.assertEquals(plan.getProps(), createTimeSeriesStatement.getProps());
        Assert.assertEquals(plan.getAttributes(), createTimeSeriesStatement.getAttributes());
        Assert.assertEquals(plan.getTags(), createTimeSeriesStatement.getTags());
      } else if (stmt instanceof ActivateTemplateStatement) {
        ActivateTemplateStatement activateTemplateStatement = (ActivateTemplateStatement) stmt;
        IActivateTemplateInClusterPlan plan =
            (IActivateTemplateInClusterPlan)
                planMap.get(activateTemplateStatement.getPath().toString());
        Assert.assertNotNull(plan);
      }
      count++;
    }
    Assert.assertEquals(planMap.size(), count);
    statements.checkException();
  }
}
