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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class PipeConfigPhysicalPlanPatternParseVisitorTest {

  private final IoTDBTreePattern prefixPathPattern = new IoTDBTreePattern("root.db.device.**");
  private final IoTDBTreePattern fullPathPattern = new IoTDBTreePattern("root.db.device.s1");

  @Test
  public void testCreateDatabase() {
    final DatabaseSchemaPlan createDatabasePlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db"));
    final DatabaseSchemaPlan createDatabasePlanToFilter =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1"));

    Assert.assertEquals(
        createDatabasePlan,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCreateDatabase(createDatabasePlan, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCreateDatabase(createDatabasePlanToFilter, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testAlterDatabase() {
    final DatabaseSchemaPlan alterDatabasePlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.db"));
    final DatabaseSchemaPlan alterDatabasePlanToFilter =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.db1"));

    Assert.assertEquals(
        alterDatabasePlan,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitAlterDatabase(alterDatabasePlan, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitAlterDatabase(alterDatabasePlanToFilter, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testDeleteDatabase() {
    final DeleteDatabasePlan deleteDatabasePlan = new DeleteDatabasePlan("root.db");
    final DeleteDatabasePlan deleteDatabasePlanToFilter = new DeleteDatabasePlan("root.db1");

    Assert.assertEquals(
        deleteDatabasePlan,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitDeleteDatabase(deleteDatabasePlan, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitDeleteDatabase(deleteDatabasePlanToFilter, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testCreateSchemaTemplate() throws IllegalPathException {
    final CreateSchemaTemplatePlan createSchemaTemplatePlan =
        new CreateSchemaTemplatePlan(
            new Template(
                    "template_name",
                    Arrays.asList("s1", "s2"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
                    Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
                    Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY))
                .serialize()
                .array());

    Assert.assertEquals(
        createSchemaTemplatePlan,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCreateSchemaTemplate(createSchemaTemplatePlan, prefixPathPattern)
            .orElseThrow(AssertionError::new));

    final CreateSchemaTemplatePlan parsedTemplatePlan =
        (CreateSchemaTemplatePlan)
            IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                .visitCreateSchemaTemplate(createSchemaTemplatePlan, fullPathPattern)
                .orElseThrow(AssertionError::new);
    Assert.assertEquals(
        Collections.singleton("s1"), parsedTemplatePlan.getTemplate().getSchemaMap().keySet());
    Assert.assertEquals(
        createSchemaTemplatePlan.getTemplate().getSchemaMap().get("s1"),
        parsedTemplatePlan.getTemplate().getSchemaMap().get("s1"));
  }

  @Test
  public void testCommitSetSchemaTemplate() {
    final CommitSetSchemaTemplatePlan setSchemaTemplatePlanOnPrefix =
        new CommitSetSchemaTemplatePlan("t1", "root.db");
    final CommitSetSchemaTemplatePlan setSchemaTemplatePlanOnFullPath =
        new CommitSetSchemaTemplatePlan("t1", "root.db.device.s1");

    Assert.assertEquals(
        setSchemaTemplatePlanOnPrefix,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCommitSetSchemaTemplate(setSchemaTemplatePlanOnPrefix, fullPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertEquals(
        setSchemaTemplatePlanOnFullPath,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCommitSetSchemaTemplate(setSchemaTemplatePlanOnFullPath, fullPathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testPipeUnsetSchemaTemplate() {
    final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlanOnPrefix =
        new PipeUnsetSchemaTemplatePlan("t1", "root.db");
    final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlanOrFullPath =
        new PipeUnsetSchemaTemplatePlan("t1", "root.db.device.s1");

    Assert.assertEquals(
        pipeUnsetSchemaTemplatePlanOnPrefix,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitPipeUnsetSchemaTemplate(pipeUnsetSchemaTemplatePlanOnPrefix, fullPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertEquals(
        pipeUnsetSchemaTemplatePlanOrFullPath,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitPipeUnsetSchemaTemplate(pipeUnsetSchemaTemplatePlanOrFullPath, fullPathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testExtendSchemaTemplate() {
    final ExtendSchemaTemplatePlan extendSchemaTemplatePlan =
        new ExtendSchemaTemplatePlan(
            new TemplateExtendInfo(
                "template_name",
                Arrays.asList("s1", "s2"),
                Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
                Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
                Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY)));

    final ExtendSchemaTemplatePlan parsedTemplatePlan =
        (ExtendSchemaTemplatePlan)
            IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                .visitExtendSchemaTemplate(extendSchemaTemplatePlan, fullPathPattern)
                .orElseThrow(AssertionError::new);
    Assert.assertEquals(
        Collections.singletonList("s1"),
        parsedTemplatePlan.getTemplateExtendInfo().getMeasurements());
    Assert.assertEquals(
        extendSchemaTemplatePlan.getTemplateExtendInfo().getTemplateName(),
        parsedTemplatePlan.getTemplateExtendInfo().getTemplateName());
    Assert.assertEquals(
        extendSchemaTemplatePlan.getTemplateExtendInfo().getDataTypes().get(0),
        parsedTemplatePlan.getTemplateExtendInfo().getDataTypes().get(0));
    Assert.assertEquals(
        extendSchemaTemplatePlan.getTemplateExtendInfo().getEncodings().get(0),
        parsedTemplatePlan.getTemplateExtendInfo().getEncodings().get(0));
    Assert.assertEquals(
        extendSchemaTemplatePlan.getTemplateExtendInfo().getCompressors().get(0),
        parsedTemplatePlan.getTemplateExtendInfo().getCompressors().get(0));
  }

  @Test
  public void testGrantUser() throws IllegalPathException {
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.**")),
        ((AuthorPlan)
                IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                    .visitGrantUser(
                        new AuthorPlan(
                            ConfigPhysicalPlanType.GrantUser,
                            "tempUser",
                            "",
                            "",
                            "",
                            new HashSet<>(Arrays.asList(1, 2)),
                            true,
                            Arrays.asList(
                                new PartialPath("root.db.**"), new PartialPath("root.abc.**"))),
                        prefixPathPattern)
                    .orElseThrow(AssertionError::new))
            .getNodeNameList());
  }

  @Test
  public void testRevokeUser() throws IllegalPathException {
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.**")),
        ((AuthorPlan)
                IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                    .visitRevokeUser(
                        new AuthorPlan(
                            ConfigPhysicalPlanType.RevokeUser,
                            "tempUser",
                            "",
                            "",
                            "",
                            new HashSet<>(Arrays.asList(1, 2)),
                            false,
                            Arrays.asList(
                                new PartialPath("root.db.**"), new PartialPath("root.abc.**"))),
                        prefixPathPattern)
                    .orElseThrow(AssertionError::new))
            .getNodeNameList());
  }

  @Test
  public void testGrantRole() throws IllegalPathException {
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.**")),
        ((AuthorPlan)
                IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                    .visitGrantRole(
                        new AuthorPlan(
                            ConfigPhysicalPlanType.GrantRole,
                            "",
                            "tempRole",
                            "",
                            "",
                            new HashSet<>(Arrays.asList(1, 2)),
                            true,
                            Arrays.asList(
                                new PartialPath("root.db.**"), new PartialPath("root.abc.**"))),
                        prefixPathPattern)
                    .orElseThrow(AssertionError::new))
            .getNodeNameList());
  }

  @Test
  public void testRevokeRole() throws IllegalPathException {
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.**")),
        ((AuthorPlan)
                IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                    .visitRevokeRole(
                        new AuthorPlan(
                            ConfigPhysicalPlanType.RevokeRole,
                            "",
                            "tempRole",
                            "",
                            "",
                            new HashSet<>(Arrays.asList(1, 2)),
                            false,
                            Arrays.asList(
                                new PartialPath("root.db.**"), new PartialPath("root.abc.**"))),
                        prefixPathPattern)
                    .orElseThrow(AssertionError::new))
            .getNodeNameList());
  }

  @Test
  public void testPipeDeleteTimeSeries() throws IllegalPathException, IOException {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.*.device.s1"));
    patternTree.constructTree();

    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.s1")),
        PathPatternTree.deserialize(
                ((PipeDeleteTimeSeriesPlan)
                        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                            .visitPipeDeleteTimeSeries(
                                new PipeDeleteTimeSeriesPlan(patternTree.serialize()),
                                prefixPathPattern)
                            .orElseThrow(AssertionError::new))
                    .getPatternTreeBytes())
            .getAllPathPatterns());
  }

  @Test
  public void testPipeDeleteLogicalView() throws IllegalPathException, IOException {
    final PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath("root.*.device.s1"));
    patternTree.constructTree();

    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.s1")),
        PathPatternTree.deserialize(
                ((PipeDeleteTimeSeriesPlan)
                        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                            .visitPipeDeleteLogicalView(
                                new PipeDeleteLogicalViewPlan(patternTree.serialize()),
                                prefixPathPattern)
                            .orElseThrow(AssertionError::new))
                    .getPatternTreeBytes())
            .getAllPathPatterns());
  }

  @Test
  public void testPipeDeactivateTemplate() throws IllegalPathException {
    final Template template1 = newSchemaTemplate("template1");
    final Template template2 = newSchemaTemplate("template2");
    final Template template3 = newSchemaTemplate("template3");

    Assert.assertEquals(
        new HashMap<PartialPath, List<Template>>() {
          {
            put(
                new PartialPath("root.db.device.s1"),
                Arrays.asList(template1, template2, template3));
          }
        },
        ((PipeDeactivateTemplatePlan)
                IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                    .visitPipeDeactivateTemplate(
                        new PipeDeactivateTemplatePlan(
                            new HashMap<PartialPath, List<Template>>() {
                              {
                                put(
                                    new PartialPath("root.*.device.s1"),
                                    Collections.singletonList(template1));
                                put(
                                    new PartialPath("root.db.*.s1"),
                                    Arrays.asList(template2, template3));
                              }
                            }),
                        prefixPathPattern)
                    .orElseThrow(AssertionError::new))
            .getTemplateSetInfo());
  }

  private Template newSchemaTemplate(final String name) throws IllegalPathException {
    return new Template(
        name,
        Arrays.asList(name + "_" + "temperature", name + "_" + "status"),
        Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
        Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
  }

  @Test
  public void testSetTTL() throws IllegalPathException {
    final SetTTLPlan plan =
        ((SetTTLPlan)
            IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
                .visitTTL(
                    new SetTTLPlan(Arrays.asList("root", "db", "**"), Long.MAX_VALUE),
                    prefixPathPattern)
                .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new PartialPath("root.db.device.**"), new PartialPath(plan.getPathPattern()));
    Assert.assertEquals(Long.MAX_VALUE, plan.getTTL());
  }
}
