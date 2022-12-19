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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.ActivateTemplateInClusterPlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.DeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.PreDeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.RollbackPreDeactivateTemplatePlanImpl;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaRegionTemplateTest extends AbstractSchemaRegionTest {

  public SchemaRegionTemplateTest(SchemaRegionTestParams testParams) {
    super(testParams);
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
