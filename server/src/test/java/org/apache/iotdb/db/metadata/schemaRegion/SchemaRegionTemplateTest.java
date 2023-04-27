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
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.metadata.schemaRegion.SchemaRegionTestUtil.getPathsUsingTemplate;

public class SchemaRegionTemplateTest extends AbstractSchemaRegionTest {

  public SchemaRegionTemplateTest(SchemaRegionTestParams testParams) {
    super(testParams);
  }

  /** Test {@link ISchemaRegion#activateSchemaTemplate}. */
  @Test
  public void testActivateSchemaTemplate() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf01.wt01"), 3, templateId),
        template);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf02"), 2, templateId),
        template);
    Set<String> expectedPaths = new HashSet<>(Arrays.asList("root.sg.wf01.wt01", "root.sg.wf02"));
    Set<String> pathsUsingTemplate =
        new HashSet<>(getPathsUsingTemplate(schemaRegion, new PartialPath("root.**"), templateId));
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
        getPathsUsingTemplate(schemaRegion, new PartialPath("root.sg.wf01.*"), templateId).get(0));
    PathPatternTree wf02PatternTree = new PathPatternTree();
    wf02PatternTree.appendPathPattern(new PartialPath("root.sg.wf02"));
    wf02PatternTree.constructTree();
    Assert.assertEquals(1, schemaRegion.countPathsUsingTemplate(templateId, wf02PatternTree));
    Assert.assertEquals(
        "root.sg.wf02",
        getPathsUsingTemplate(schemaRegion, new PartialPath("root.sg.wf02"), templateId).get(0));
  }

  /**
   * Test {@link ISchemaRegion#constructSchemaBlackListWithTemplate}, {@link
   * ISchemaRegion#rollbackSchemaBlackListWithTemplate} and {@link
   * ISchemaRegion#deactivateTemplateInBlackList}
   */
  @Test
  public void testDeactivateTemplate() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
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
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf01.wt01"), 3, templateId),
        template);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf02"), 2, templateId),
        template);

    // construct schema blacklist with template on root.sg.wf01.wt01 and root.sg.wf02
    Map<PartialPath, List<Integer>> allDeviceTemplateMap = new HashMap<>();
    allDeviceTemplateMap.put(new PartialPath("root.**"), Collections.singletonList(templateId));
    schemaRegion.constructSchemaBlackListWithTemplate(
        SchemaRegionWritePlanFactory.getPreDeactivateTemplatePlan(allDeviceTemplateMap));

    // rollback schema blacklist with template on root.sg.wf02
    Map<PartialPath, List<Integer>> wf02TemplateMap = new HashMap<>();
    wf02TemplateMap.put(new PartialPath("root.sg.wf02"), Collections.singletonList(templateId));
    schemaRegion.rollbackSchemaBlackListWithTemplate(
        SchemaRegionWritePlanFactory.getRollbackPreDeactivateTemplatePlan(wf02TemplateMap));

    // deactivate schema blacklist with template on root.sg.wf01.wt01
    schemaRegion.deactivateTemplateInBlackList(
        SchemaRegionWritePlanFactory.getDeactivateTemplatePlan(allDeviceTemplateMap));

    // check using getPathsUsingTemplate
    List<String> expectedPaths = Collections.singletonList("root.sg.wf02");
    List<String> pathsUsingTemplate =
        getPathsUsingTemplate(schemaRegion, new PartialPath("root.**"), templateId);
    Assert.assertEquals(expectedPaths, pathsUsingTemplate);
    // check using countPathsUsingTemplate
    PathPatternTree allPatternTree = new PathPatternTree();
    allPatternTree.appendPathPattern(new PartialPath("root.**"));
    allPatternTree.constructTree();
    Assert.assertEquals(1, schemaRegion.countPathsUsingTemplate(templateId, allPatternTree));
  }

  @Test
  public void testFetchSchemaWithTemplate() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    SchemaRegionTestUtil.createSimpleTimeseriesByList(
        schemaRegion, Arrays.asList("root.sg.wf01.wt01.status", "root.sg.wf01.wt01.temperature"));
    int templateId = 1;
    Template template =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf01.wt01"), 3, templateId),
        template);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.wf02"), 2, templateId),
        template);
    Map<Integer, Template> templateMap = Collections.singletonMap(templateId, template);
    List<String> expectedTimeseries =
        Arrays.asList(
            "root.sg.wf01.wt01.s1",
            "root.sg.wf01.wt01.s2",
            "root.sg.wf01.wt01.status",
            "root.sg.wf01.wt01.temperature",
            "root.sg.wf02.s1",
            "root.sg.wf02.s2");

    // check fetch schema
    List<MeasurementPath> schemas =
        schemaRegion.fetchSchema(new PartialPath("root.**"), templateMap, true);
    Assert.assertEquals(expectedTimeseries.size(), schemas.size());
    schemas.sort(Comparator.comparing(PartialPath::getFullPath));
    for (int i = 0; i < schemas.size(); i++) {
      Assert.assertEquals(expectedTimeseries.get(i), schemas.get(i).getFullPath());
    }

    // check show timeseries
    List<ITimeSeriesSchemaInfo> result =
        SchemaRegionTestUtil.showTimeseries(
            schemaRegion,
            SchemaRegionReadPlanFactory.getShowTimeSeriesPlan(
                new PartialPath("root.**"), templateMap));
    result.sort(Comparator.comparing(ISchemaInfo::getFullPath));
    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(expectedTimeseries.get(i), result.get(i).getFullPath());
    }
  }

  @Test
  public void testDeleteSchemaWithTemplate() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.db", 0);
    int templateId = 1;
    Template template =
        new Template(
            "t1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.DOUBLE, TSDataType.INT32),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    template.setId(templateId);
    schemaRegion.activateSchemaTemplate(
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.db.d1"), 3, templateId),
        template);

    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.db.d1.s3"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);

    Assert.assertEquals(
        0, SchemaRegionTestUtil.deleteTimeSeries(schemaRegion, new PartialPath("root.db.d1.s1")));
    Assert.assertEquals(
        1, SchemaRegionTestUtil.deleteTimeSeries(schemaRegion, new PartialPath("root.db.d1.s3")));

    Assert.assertEquals(
        1,
        schemaRegion
            .fetchSchema(
                new PartialPath("root.db.d1.s1"),
                Collections.singletonMap(templateId, template),
                false)
            .size());
    Assert.assertEquals(
        0,
        schemaRegion
            .fetchSchema(
                new PartialPath("root.db.d1.s3"),
                Collections.singletonMap(templateId, template),
                false)
            .size());
  }
}
