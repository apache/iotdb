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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateAlignedTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE;
import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.checkSingleTimeSeries;

public class SchemaRegionSimpleRecoverTest extends AbstractSchemaRegionTest {

  private String schemaRegionConsensusProtocolClass;

  public SchemaRegionSimpleRecoverTest(final SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    schemaRegionConsensusProtocolClass =
        IoTDBDescriptor.getInstance().getConfig().getSchemaRegionConsensusProtocolClass();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaRegionConsensusProtocolClass(schemaRegionConsensusProtocolClass);
  }

  @Test
  public void testRecoverWithAlignedTemplate() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    final int templateId = 1;
    final Template template =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            true);
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.d1"), 2, templateId),
        template);
    ClusterSchemaTree schemaTree =
        schemaRegion.fetchSeriesSchema(
            ALL_MATCH_SCOPE,
            Collections.singletonMap(templateId, template),
            true,
            false,
            true,
            false);
    Assert.assertTrue(schemaTree.getAllDevices().get(0).isAligned());

    simulateRestart();
    schemaRegion = getSchemaRegion("root.sg", 0);
    schemaTree =
        schemaRegion.fetchSeriesSchema(
            ALL_MATCH_SCOPE,
            Collections.singletonMap(templateId, template),
            true,
            false,
            true,
            false);
    Assert.assertTrue(schemaTree.getAllDevices().get(0).isAligned());
  }

  @Test
  public void testRecoverAfterCreateAlignedTimeSeriesWithMerge() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    final Map<String, String> oldTagMap = Collections.singletonMap("tagK", "tagV");
    final Map<String, String> oldAttrMap = Collections.singletonMap("attrK1", "attrV1");
    schemaRegion.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.wf02.wt01"),
            Arrays.asList("temperature", "status"),
            Arrays.asList(TSDataType.valueOf("FLOAT"), TSDataType.valueOf("INT32")),
            Arrays.asList(TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            null,
            Arrays.asList(Collections.emptyMap(), oldTagMap),
            Arrays.asList(Collections.emptyMap(), oldAttrMap)));

    final Map<String, String> newTagMap = Collections.singletonMap("tagK", "newTagV");
    final Map<String, String> newAttrMap = Collections.singletonMap("attrK2", "attrV2");
    final ICreateAlignedTimeSeriesPlan mergePlan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.wf02.wt01"),
            // The lists must be mutable
            new ArrayList<>(Arrays.asList("status", "height")),
            new ArrayList<>(
                Arrays.asList(TSDataType.valueOf("INT32"), TSDataType.valueOf("INT64"))),
            new ArrayList<>(
                Arrays.asList(TSEncoding.valueOf("PLAIN"), TSEncoding.valueOf("PLAIN"))),
            new ArrayList<>(Arrays.asList(CompressionType.ZSTD, CompressionType.GZIP)),
            new ArrayList<>(Arrays.asList("alias2", null)),
            new ArrayList<>(Arrays.asList(newTagMap, oldTagMap)),
            new ArrayList<>(Arrays.asList(newAttrMap, oldAttrMap)));
    ((CreateAlignedTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
    schemaRegion.createAlignedTimeSeries(mergePlan);

    simulateRestart();
    schemaRegion = getSchemaRegion("root.sg", 0);

    // The encoding and compressor won't be changed
    // The alias/tags/attributes are updated

    final Map<String, String> resultAttrMap = new HashMap<>(oldAttrMap);
    resultAttrMap.putAll(newAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf02.wt01.status"),
        true,
        TSDataType.INT32,
        TSEncoding.RLE,
        CompressionType.SNAPPY,
        "alias2",
        newTagMap,
        resultAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf02.wt01.height"),
        true,
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.GZIP,
        null,
        oldTagMap,
        oldAttrMap);
  }

  @Test
  public void testRecoverAfterCreateTimeSeriesWithMerge() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);

    final Map<String, String> oldTagMap = Collections.singletonMap("tagK", "tagV");
    final Map<String, String> oldAttrMap = Collections.singletonMap("attrK1", "attrV1");
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            oldTagMap,
            oldAttrMap,
            null),
        -1);

    final Map<String, String> newTagMap = Collections.singletonMap("tagK", "newTagV");
    final Map<String, String> newAttrMap = Collections.singletonMap("attrK2", "attrV2");
    final ICreateTimeSeriesPlan mergePlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.wf01.wt01.v1.s1"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.ZSTD,
            null,
            newTagMap,
            newAttrMap,
            "alias2");
    ((CreateTimeSeriesPlanImpl) mergePlan).setWithMerge(true);
    schemaRegion.createTimeSeries(mergePlan, -1);

    simulateRestart();
    schemaRegion = getSchemaRegion("root.sg", 0);

    // The encoding and compressor won't be changed
    // The alias/tags/attributes are updated

    final Map<String, String> resultAttrMap = new HashMap<>(oldAttrMap);
    resultAttrMap.putAll(newAttrMap);

    checkSingleTimeSeries(
        schemaRegion,
        new PartialPath("root.sg.wf01.wt01.v1.s1"),
        false,
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        "alias2",
        newTagMap,
        resultAttrMap);
  }
}
