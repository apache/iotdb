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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TemplateQueryOrderTest {

  @Test
  public void testRawWildcardOrderAfterSerialization() throws Exception {
    assertQueryOrder("*", false);
  }

  @Test
  public void testAggregationWildcardOrderAfterSerialization() throws Exception {
    assertQueryOrder("count(*)", true);
  }

  @Test
  public void testSchemaTreeWildcardOrderAfterSerialization() throws Exception {
    for (Template template : createTemplateVersions()) {
      List<String> expected = sortedMeasurements(template);
      ClusterSchemaTree schemaTree = createSchemaTree(template);
      PartialPath pattern = new PartialPath("root.sg.d1.*");
      assertEquals(
          expected,
          schemaTree.searchMeasurementPaths(pattern).left.stream()
              .map(MeasurementPath::getMeasurement)
              .collect(Collectors.toList()));
      assertEquals(
          expected.subList(3, 8),
          schemaTree.searchMeasurementPaths(pattern, 5, 3, false).left.stream()
              .map(MeasurementPath::getMeasurement)
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testExplicitColumnsHonorSeriesLimit() throws Exception {
    for (Template template : createTemplateVersions()) {
      List<String> measurements = sortedMeasurements(template);
      assertEquals(
          measurements.subList(3, 8),
          analyzeOutputSymbols(
              template,
              "select "
                  + String.join(", ", measurements)
                  + " from root.sg.* slimit 5 soffset 3 align by device"));
    }
  }

  @Test
  public void testExplicitAggregationsHonorSeriesLimit() throws Exception {
    for (Template template : createTemplateVersions()) {
      List<String> aggregations =
          sortedMeasurements(template).stream()
              .map(name -> "count(" + name + ")")
              .collect(Collectors.toList());
      assertEquals(
          aggregations.subList(3, 8),
          analyzeOutputSymbols(
              template,
              "select "
                  + String.join(", ", aggregations)
                  + " from root.sg.* slimit 5 soffset 3 align by device"));
    }
  }

  @Test
  public void testSchemaTreeOffsetWithoutLimit() throws Exception {
    for (Template template : createTemplateVersions()) {
      List<String> expected = sortedMeasurements(template);
      assertEquals(
          expected.subList(3, expected.size()),
          createSchemaTree(template)
              .searchMeasurementPaths(new PartialPath("root.sg.d1.*"), 0, 3, false)
              .left
              .stream()
              .map(MeasurementPath::getMeasurement)
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testCountTimeHonorsSeriesOffset() throws Exception {
    for (Template template : createTemplateVersions()) {
      assertEmptyProjectionFinishesQuery(
          template, "select count_time(*) from root.sg.* slimit 1 soffset 1 align by device");
    }
  }

  @Test
  public void testRawSeriesOffsetBeyondMeasurementsFinishesQuery() throws Exception {
    for (Template template : createTemplateVersions()) {
      assertEmptyProjectionFinishesQuery(
          template, "select * from root.sg.* slimit 1 soffset 16 align by device");
    }
  }

  @Test
  public void testAggregationSeriesOffsetBeyondMeasurementsFinishesQuery() throws Exception {
    for (Template template : createTemplateVersions()) {
      assertEmptyProjectionFinishesQuery(
          template, "select count(*) from root.sg.* slimit 1 soffset 16 align by device");
    }
  }

  private void assertEmptyProjectionFinishesQuery(Template template, String sql) throws Exception {
    Analysis analysis =
        analyze(
            template,
            sql,
            Arrays.asList(new PartialPath("root.sg.d1"), new PartialPath("root.sg.d2")));
    assertTrue(analysis.getOutputExpressions().isEmpty());
    assertTrue(analysis.isFinishQueryAfterAnalyze());
  }

  private void assertQueryOrder(String projection, boolean aggregation) throws Exception {
    for (Template template : createTemplateVersions()) {
      List<String> expected = sortedMeasurements(template);
      if (aggregation) {
        expected =
            expected.stream().map(name -> "count(" + name + ")").collect(Collectors.toList());
      }
      assertEquals(
          expected,
          analyzeOutputSymbols(
              template, "select " + projection + " from root.sg.* align by device"));
      assertEquals(
          expected.subList(3, 8),
          analyzeOutputSymbols(
              template,
              "select " + projection + " from root.sg.* slimit 5 soffset 3 align by device"));
    }
  }

  private List<String> analyzeOutputSymbols(Template template, String sql) throws Exception {
    return analyze(template, sql, Collections.emptyList()).getOutputExpressions().stream()
        .map(pair -> pair.left.getOutputSymbol())
        .collect(Collectors.toList());
  }

  private Analysis analyze(Template template, String sql, List<PartialPath> devices)
      throws Exception {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    statement.semanticCheck();
    Analysis analysis = new Analysis();
    MPPQueryContext context =
        new MPPQueryContext(
            sql,
            new QueryId("test_query"),
            new SessionInfo(0, "test", ZoneId.systemDefault()),
            new TEndPoint(),
            new TEndPoint());
    assertTrue(
        TemplatedAnalyze.canBuildPlanUseTemplate(
            analysis,
            statement,
            new FakePartitionFetcherImpl(),
            createSchemaTree(template),
            context,
            devices,
            new TreeAnalysisMutationJournal()));
    return analysis;
  }

  private ClusterSchemaTree createSchemaTree(Template template) throws Exception {
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    schemaTree.setDatabases(Collections.singleton("root.sg"));
    for (String device : Arrays.asList("root.sg.d1", "root.sg.d2")) {
      schemaTree.appendTemplateDevice(new PartialPath(device), true, template.getId(), template);
    }
    return schemaTree;
  }

  private List<String> sortedMeasurements(Template template) {
    List<String> measurements = new ArrayList<>(template.getSchemaMap().keySet());
    Collections.sort(measurements);
    return measurements;
  }

  private List<Template> createTemplateVersions() throws Exception {
    List<String> measurements = new ArrayList<>();
    // All names have the same String hash. Rebuilding ConcurrentHashMap tree bins changes
    // iteration order even when the template contents and map capacity stay the same.
    for (int i = 0; i < 16; i++) {
      StringBuilder measurement = new StringBuilder();
      for (int bit = 0; bit < 4; bit++) {
        measurement.append((i & (1 << bit)) == 0 ? "Aa" : "BB");
      }
      measurements.add(measurement.toString());
    }
    Template template =
        new Template(
            "template",
            measurements,
            Collections.nCopies(measurements.size(), TSDataType.INT32),
            Collections.nCopies(measurements.size(), TSEncoding.RLE),
            Collections.nCopies(measurements.size(), CompressionType.SNAPPY),
            true);
    template.setId(1);
    List<Template> templates = new ArrayList<>();
    templates.add(template);
    for (int i = 0; i < 2; i++) {
      Template restored = new Template();
      restored.deserialize(template.serialize());
      templates.add(restored);
      template = restored;
    }
    return templates;
  }
}
