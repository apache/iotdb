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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregationDescriptorTest {

  private static final List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
  private static final List<GroupByLevelDescriptor> groupByLevelDescriptorList = new ArrayList<>();

  public static final Map<String, PartialPath> pathMap = new HashMap<>();

  static {
    try {
      pathMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      pathMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      pathMap.put("root.sg.*.s1", new MeasurementPath("root.sg.*.s1", TSDataType.INT32));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  static {
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.SINGLE,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.SUM.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.INTERMEDIATE,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.LAST_VALUE.name().toLowerCase(),
            AggregationStep.INTERMEDIATE,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.MAX_VALUE.name().toLowerCase(),
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));
    aggregationDescriptorList.add(
        new AggregationDescriptor(
            AggregationType.COUNT.name().toLowerCase(),
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")))));

    groupByLevelDescriptorList.add(
        new GroupByLevelDescriptor(
            AggregationType.COUNT.name().toLowerCase(),
            AggregationStep.FINAL,
            Arrays.asList(
                new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")),
                new TimeSeriesOperand(pathMap.get("root.sg.d1.s1"))),
            new TimeSeriesOperand(pathMap.get("root.sg.*.s1"))));
    groupByLevelDescriptorList.add(
        new GroupByLevelDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.FINAL,
            Arrays.asList(
                new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")),
                new TimeSeriesOperand(pathMap.get("root.sg.d2.s1"))),
            new TimeSeriesOperand(pathMap.get("root.sg.*.s1"))));
    groupByLevelDescriptorList.add(
        new GroupByLevelDescriptor(
            AggregationType.COUNT.name().toLowerCase(),
            AggregationStep.INTERMEDIATE,
            Arrays.asList(
                new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")),
                new TimeSeriesOperand(pathMap.get("root.sg.d1.s1"))),
            new TimeSeriesOperand(pathMap.get("root.sg.*.s1"))));
    groupByLevelDescriptorList.add(
        new GroupByLevelDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.INTERMEDIATE,
            Arrays.asList(
                new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")),
                new TimeSeriesOperand(pathMap.get("root.sg.d2.s1"))),
            new TimeSeriesOperand(pathMap.get("root.sg.*.s1"))));
  }

  @Test
  public void testOutputColumnNames() {
    List<String> expectedOutputColumnNames =
        Arrays.asList(
            "avg(root.sg.d1.s1)",
            "sum(root.sg.d1.s1)",
            "count(root.sg.d1.s1)",
            "last_value(root.sg.d1.s1)",
            "max_time(root.sg.d1.s1)",
            "max_value(root.sg.d1.s1)");
    Assert.assertEquals(
        expectedOutputColumnNames,
        aggregationDescriptorList.stream()
            .map(AggregationDescriptor::getOutputColumnNames)
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList()));
  }

  @Test
  public void testInputColumnNames() {
    List<List<List<String>>> expectedInputColumnNames =
        Arrays.asList(
            Collections.singletonList(Collections.singletonList("root.sg.d1.s1")),
            Collections.singletonList(Collections.singletonList("root.sg.d1.s1")),
            Collections.singletonList(Arrays.asList("count(root.sg.d1.s1)", "sum(root.sg.d1.s1)")),
            Collections.singletonList(
                Arrays.asList("last_value(root.sg.d1.s1)", "max_time(root.sg.d1.s1)")),
            Collections.singletonList(Collections.singletonList("max_value(root.sg.d1.s1)")),
            Collections.singletonList(Collections.singletonList("count(root.sg.d1.s1)")));
    Assert.assertEquals(
        expectedInputColumnNames,
        aggregationDescriptorList.stream()
            .map(AggregationDescriptor::getInputColumnNamesList)
            .collect(Collectors.toList()));
  }

  @Test
  public void testOutputColumnNamesInGroupByLevel() {
    List<String> expectedOutputColumnNames =
        Arrays.asList("count(root.sg.*.s1)", "avg(root.sg.*.s1)", "sum(root.sg.*.s1)");
    Assert.assertEquals(
        expectedOutputColumnNames,
        groupByLevelDescriptorList.stream()
            .map(GroupByLevelDescriptor::getOutputColumnNames)
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList()));
  }

  @Test
  public void testInputColumnNamesInGroupByLevel() {
    List<List<List<String>>> expectedInputColumnNames =
        Arrays.asList(
            Arrays.asList(
                Collections.singletonList("count(root.sg.d2.s1)"),
                Collections.singletonList("count(root.sg.d1.s1)")),
            Arrays.asList(
                Arrays.asList("count(root.sg.d1.s1)", "sum(root.sg.d1.s1)"),
                Arrays.asList("count(root.sg.d2.s1)", "sum(root.sg.d2.s1)")),
            Arrays.asList(
                Collections.singletonList("count(root.sg.d2.s1)"),
                Collections.singletonList("count(root.sg.d1.s1)")),
            Arrays.asList(
                Arrays.asList("count(root.sg.d1.s1)", "sum(root.sg.d1.s1)"),
                Arrays.asList("count(root.sg.d2.s1)", "sum(root.sg.d2.s1)")));
    Assert.assertEquals(
        expectedInputColumnNames,
        groupByLevelDescriptorList.stream()
            .map(GroupByLevelDescriptor::getInputColumnNamesList)
            .collect(Collectors.toList()));
  }

  @Test
  public void testGroupByLevelInputColumnCandidate() {
    List<Map<String, Expression>> expectedMapList =
        Arrays.asList(
            new HashMap<String, Expression>() {
              {
                put("count(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("count(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
                put("count(root.sg.*.s1)", new TimeSeriesOperand(pathMap.get("root.sg.*.s1")));
              }
            },
            new HashMap<String, Expression>() {
              {
                put("avg(root.sg.*.s1)", new TimeSeriesOperand(pathMap.get("root.sg.*.s1")));
                put("count(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("count(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
                put("sum(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("sum(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
              }
            },
            new HashMap<String, Expression>() {
              {
                put("count(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("count(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
                put("count(root.sg.*.s1)", new TimeSeriesOperand(pathMap.get("root.sg.*.s1")));
              }
            },
            new HashMap<String, Expression>() {
              {
                put("count(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("count(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
                put("count(root.sg.*.s1)", new TimeSeriesOperand(pathMap.get("root.sg.*.s1")));
                put("sum(root.sg.d1.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d1.s1")));
                put("sum(root.sg.d2.s1)", new TimeSeriesOperand(pathMap.get("root.sg.d2.s1")));
                put("sum(root.sg.*.s1)", new TimeSeriesOperand(pathMap.get("root.sg.*.s1")));
              }
            });
    Assert.assertEquals(
        expectedMapList,
        groupByLevelDescriptorList.stream()
            .map(GroupByLevelDescriptor::getInputColumnCandidateMap)
            .collect(Collectors.toList()));
  }
}
