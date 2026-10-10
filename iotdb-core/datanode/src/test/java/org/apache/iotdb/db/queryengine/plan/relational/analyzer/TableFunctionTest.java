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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.ForecastTableFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.FFTTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.LTTBTableFunction;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.TableFunctionProcessorMatcher;
import org.apache.iotdb.udf.api.relational.EmptyTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Consumer;

import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.ForecastTableFunction.DEFAULT_OUTPUT_INTERVAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.function.tvf.ForecastTableFunction.DEFAULT_OUTPUT_START_TIME;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.FIRST;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.LAST;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem.Ordering.ASCENDING;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem.Ordering.DESCENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.group;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topK;
import static org.apache.iotdb.udf.api.type.Type.DOUBLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableFunctionTest {

  @Test
  public void testSimpleRowSemantic() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM TABLE(HOP("
            + "DATA => TABLE(table1), "
            + "TIMECOL => 'time', "
            + "SLIDE => 30m, "
            + "SIZE => 1h))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3"));
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("hop")
                .properOutputs("window_start", "window_end")
                .requiredSymbols("time")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("SIZE", 3600000L)
                        .addProperty("SLIDE", 1800000L)
                        .addProperty("ORIGIN", 0L)
                        .build());
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, tableScan)));
    // Verify DistributionPlan

    /*
     *   └──OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──TableFunctionProcessor
     *                        └──TableScan
     */
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(
        planTester.getFragmentPlan(1), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(2), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(3), tableFunctionProcessor(tableFunctionMatcher, tableScan));
  }

  @Test
  public void testSimpleRowSemantic2() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM TABLE(EXCLUDE(DATA => TABLE(table1), EXCLUDE => 'attr1'))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.of(
                "time_0", "time", "tag1_1", "tag1", "tag2_2", "tag2", "tag3_3", "tag3", "attr2_4",
                "attr2", "s1_5", "s1", "s2_6", "s2", "s3_7", "s3"));

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("exclude")
                .properOutputs("time", "tag1", "tag2", "tag3", "attr2", "s1", "s2", "s3")
                .requiredSymbols(
                    "time_0", "tag1_1", "tag2_2", "tag3_3", "attr2_4", "s1_5", "s2_6", "s3_7")
                .handle(new EmptyTableFunctionHandle());
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, tableScan)));
    // Verify DistributionPlan
    /*
     *   └──OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──TableFunctionProcessor
     *                         └──TableScan
     *                            ├──ExchangeNode
     *                            │     └──TableScan
     *                            └──ExchangeNode
     *                                  └──TableScan
     */
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(
        planTester.getFragmentPlan(1), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(2), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(3), tableFunctionProcessor(tableFunctionMatcher, tableScan));
  }

  @Test
  public void testSimpleSetSemantic() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM TABLE(REPEAT(DATA => TABLE(table1) PARTITION BY (tag1,tag2,tag3), N => 2))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3"));

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("repeat")
                .properOutputs("repeat_index")
                .requiredSymbols("time")
                .handle(new MapTableFunctionHandle.Builder().addProperty("N", 2).build());
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - GroupNode - TableScan
    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, group(tableScan))));
    // Verify DistributionPlan
    /*
     *   └──OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──TableFunctionProcessor
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──TableFunctionProcessor
     *                              └──MergeSortNode
     *                                     ├──ExchangeNode
     *                                     │     └──TableScan
     *                                     └──ExchangeNode
     *                                           └──TableScan
     */
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(
        planTester.getFragmentPlan(1), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(2), tableFunctionProcessor(tableFunctionMatcher, tableScan));
    assertPlan(
        planTester.getFragmentPlan(3),
        tableFunctionProcessor(tableFunctionMatcher, mergeSort(exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(4), tableScan);
    assertPlan(planTester.getFragmentPlan(5), tableScan);
  }

  @Test
  public void testLeafFunction() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM TABLE(SPLIT('1,2,3,4,5'))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("split")
                .properOutputs("output")
                .requiredSymbols()
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("INPUT", "1,2,3,4,5")
                        .addProperty("SPLIT", ",")
                        .build());
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher)));

    sql =
        "select * from TABLE(SPLIT('1,2,4,5')) a join TABLE(SPLIT('2,3,4')) b on a.output=b.output";
    logicalQueryPlan = planTester.createPlan(sql);
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher1 =
        builder ->
            builder
                .name("split")
                .properOutputs("output")
                .requiredSymbols()
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("INPUT", "1,2,4,5")
                        .addProperty("SPLIT", ",")
                        .build());
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher2 =
        builder ->
            builder
                .name("split")
                .properOutputs("output_0")
                .requiredSymbols()
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("INPUT", "2,3,4")
                        .addProperty("SPLIT", ",")
                        .build());
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(
        logicalQueryPlan,
        anyTree(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .left(sort(tableFunctionProcessor(tableFunctionMatcher1)))
                        .right(sort(tableFunctionProcessor(tableFunctionMatcher2)))
                        .equiCriteria("output", "output_0"))));
  }

  @Test
  public void testHybrid() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT tag1, tag2, tag3, window_start, window_end, count(*) FROM TABLE(HOP("
            + "DATA => TABLE(SELECT * FROM TABLE(EXCLUDE(TABLE(table1), 'attr1'))), "
            + "TIMECOL => 'time', "
            + "SLIDE => 30m, "
            + "SIZE => 1h))"
            + "group by (tag1, tag2, tag3, window_start, window_end)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.of(
                "time_0", "time", "tag1_1", "tag1", "tag2_2", "tag2", "tag3_3", "tag3", "attr2_4",
                "attr2", "s1_5", "s1", "s2_6", "s2", "s3_7", "s3"));
    Consumer<TableFunctionProcessorMatcher.Builder> excludeMatcher =
        builder ->
            builder
                .name("exclude")
                .properOutputs("time", "tag1", "tag2", "tag3", "attr2", "s1", "s2", "s3")
                .requiredSymbols(
                    "time_0", "tag1_1", "tag2_2", "tag3_3", "attr2_4", "s1_5", "s2_6", "s3_7")
                .handle(new EmptyTableFunctionHandle());
    Consumer<TableFunctionProcessorMatcher.Builder> hopMatcher =
        builder ->
            builder
                .name("hop")
                .properOutputs("window_start", "window_end")
                .requiredSymbols("time")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("SIZE", 3600000L)
                        .addProperty("SLIDE", 1800000L)
                        .addProperty("ORIGIN", 0L)
                        .build());
    // Verify full LogicalPlan
    // Output - Aggregation - HOP - Project - EXCLUDE - TableScan
    assertPlan(
        logicalQueryPlan,
        anyTree(
            aggregation(
                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                tableFunctionProcessor(
                    hopMatcher, project(tableFunctionProcessor(excludeMatcher, tableScan))))));
  }

  @Test
  public void testSerDeserializeMapTableFunctionHandle() {
    MapTableFunctionHandle mapTableFunctionHandle =
        new MapTableFunctionHandle.Builder()
            .addProperty("key1", "value1")
            .addProperty("key2", 2)
            .addProperty("key3", 1L)
            .addProperty("key4", 3.0)
            .addProperty("key5", true)
            .addProperty("key6", 2.3f)
            .build();
    byte[] serialized = mapTableFunctionHandle.serialize();
    MapTableFunctionHandle deserialized = new MapTableFunctionHandle();
    deserialized.deserialize(serialized);
    assert mapTableFunctionHandle.equals(deserialized);
  }

  @Test
  public void testForecastFunction() {
    // default order by time asc
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM FORECAST("
            + "targets => (SELECT time,s3 FROM table1 WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY' ORDER BY time DESC LIMIT 1440), "
            + "model_id => 'timer_xl')";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableMap.of("time_0", "time", "s3_1", "s3"));
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("forecast")
                .properOutputs("time", "s3")
                .requiredSymbols("time_0", "s3_1")
                .handle(
                    new ForecastTableFunction.ForecastTableFunctionHandle(
                        false,
                        2880,
                        "timer_xl",
                        Collections.emptyMap(),
                        96,
                        DEFAULT_OUTPUT_START_TIME,
                        DEFAULT_OUTPUT_INTERVAL,
                        Collections.singletonList(DOUBLE)));
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(
        logicalQueryPlan,
        anyTree(
            tableFunctionProcessor(
                tableFunctionMatcher,
                sort(
                    ImmutableList.of(sort("time_0", ASCENDING, FIRST)),
                    topK(
                        1440,
                        ImmutableList.of(sort("time_0", DESCENDING, LAST)),
                        false,
                        tableScan)))));
    // Verify DistributionPlan

    /*
     *   └──OutputNode
     *         └──TableFunctionProcessor
     *               └──SortNode
     *                   └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            tableFunctionProcessor(
                tableFunctionMatcher,
                sort(ImmutableList.of(sort("time_0", ASCENDING, FIRST)), tableScan))));
  }

  @Test
  public void testForecastFunctionWithNoLowerCase() {
    // default order by time asc
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM FORECAST("
            + "targets => (SELECT time,s3 FROM table1 WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY' ORDER BY time DESC LIMIT 1440), "
            + "model_id => 'timer_xl', timecol=>'TiME')";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableMap.of("time_0", "time", "s3_1", "s3"));
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("forecast")
                .properOutputs("time", "s3")
                .requiredSymbols("time_0", "s3_1")
                .handle(
                    new ForecastTableFunction.ForecastTableFunctionHandle(
                        false,
                        2880,
                        "timer_xl",
                        Collections.emptyMap(),
                        96,
                        DEFAULT_OUTPUT_START_TIME,
                        DEFAULT_OUTPUT_INTERVAL,
                        Collections.singletonList(DOUBLE)));
    // Verify full LogicalPlan
    // Output - TableFunctionProcessor - TableScan
    assertPlan(
        logicalQueryPlan,
        anyTree(
            tableFunctionProcessor(
                tableFunctionMatcher,
                sort(
                    ImmutableList.of(sort("time_0", ASCENDING, FIRST)),
                    topK(
                        1440,
                        ImmutableList.of(sort("time_0", DESCENDING, LAST)),
                        false,
                        tableScan)))));
    // Verify DistributionPlan

    /*
     *   └──OutputNode
     *         └──TableFunctionProcessor
     *               └──SortNode
     *                   └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            tableFunctionProcessor(
                tableFunctionMatcher,
                sort(ImmutableList.of(sort("time_0", ASCENDING, FIRST)), tableScan))));
  }

  @Test
  public void testForecastFunctionAbnormal() {
    // default order by time asc
    String sql =
        "SELECT * FROM FORECAST("
            + "input => (SELECT time,s3 FROM table1 WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY' ORDER BY time DESC LIMIT 1440), "
            + "model_id => 'timer_xl', timecol => '')";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals("TIMECOL should never be null or empty.", e.getMessage());
    }
  }

  @Test
  public void testFFTFunction() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM FFT("
            + "DATA => table1 PARTITION BY tag1 ORDER BY time, "
            + "SAMPLE_INTERVAL => 1s, "
            + "N => 4, "
            + "NORM => 'ortho')";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.<String, String>builder()
                .put("time", "time")
                .put("tag1_0", "tag1")
                .put("s1", "s1")
                .put("s2", "s2")
                .put("s3", "s3")
                .buildOrThrow());

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("fft")
                .properOutputs(
                    "tag1",
                    "frequency_index",
                    "frequency",
                    "s1_real",
                    "s1_imag",
                    "s2_real",
                    "s2_imag",
                    "s3_real",
                    "s3_imag")
                .requiredSymbols("time", "tag1_0", "s1", "s2", "s3")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty(FFTTableFunction.SAMPLE_INTERVAL_PARAMETER_NAME, 1000L)
                        .addProperty(
                            FFTTableFunction.SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME, true)
                        .addProperty(FFTTableFunction.N_PARAMETER_NAME, 4L)
                        .addProperty(FFTTableFunction.NORM_PARAMETER_NAME, "ortho")
                        .addProperty("__FFT_PARTITION_TYPES", "STRING")
                        .addProperty("__FFT_VALUE_TYPES", "INT64,INT64,DOUBLE")
                        .addProperty("__FFT_VALUE_NAMES", "czE=,czI=,czM=")
                        .build());

    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, sort(tableScan))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            tableFunctionProcessor(
                tableFunctionMatcher, mergeSort(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(2), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(3), sort(tableScan));
  }

  @Test
  public void testFFTDefaultArguments() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM TABLE(FFT(DATA => TABLE(table1) ORDER BY time))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.<String, String>builder()
                .put("time", "time")
                .put("s1", "s1")
                .put("s2", "s2")
                .put("s3", "s3")
                .buildOrThrow());

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("fft")
                .properOutputs(
                    "frequency_index",
                    "frequency",
                    "s1_real",
                    "s1_imag",
                    "s2_real",
                    "s2_imag",
                    "s3_real",
                    "s3_imag")
                .requiredSymbols("time", "s1", "s2", "s3")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty(
                            FFTTableFunction.SAMPLE_INTERVAL_PARAMETER_NAME, Long.MIN_VALUE)
                        .addProperty(
                            FFTTableFunction.SAMPLE_INTERVAL_SPECIFIED_PARAMETER_NAME, false)
                        .addProperty(FFTTableFunction.N_PARAMETER_NAME, -1L)
                        .addProperty(FFTTableFunction.NORM_PARAMETER_NAME, "backward")
                        .addProperty("__FFT_PARTITION_TYPES", "")
                        .addProperty("__FFT_VALUE_TYPES", "INT64,INT64,DOUBLE")
                        .addProperty("__FFT_VALUE_NAMES", "czE=,czI=,czM=")
                        .build());

    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, sort(tableScan))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            tableFunctionProcessor(
                tableFunctionMatcher, mergeSort(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(2), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(3), sort(tableScan));
  }

  @Test
  public void testFFTWithSpecifiedTimeColumn() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM FFT("
            + "DATA => (SELECT time AS event_time, tag1, s1 FROM table1) "
            + "PARTITION BY tag1 ORDER BY event_time, "
            + "TIMECOL => 'event_time', "
            + "SAMPLE_INTERVAL => 1s, "
            + "N => 4)";

    planTester.createPlan(sql);
  }

  @Test
  public void testFFTPositionalArgumentsKeepExistingOrder() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM TABLE(FFT(TABLE(table1) ORDER BY time, 1s, 4, 'ortho'))";

    planTester.createPlan(sql);
  }

  @Test
  public void testFFTRejectsInvalidArguments() {
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1, SAMPLE_INTERVAL => 1ms)",
        "Table argument with set semantics requires an ORDER BY clause.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY time DESC, SAMPLE_INTERVAL => 1ms)",
        "The ORDER BY clause of the DATA argument must sort the time column in ascending order.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY s1, SAMPLE_INTERVAL => 1ms)",
        "The ORDER BY clause of the DATA argument must contain exactly the time column specified by the TIMECOL argument.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY time, SAMPLE_INTERVAL => 1)",
        "The SAMPLE_INTERVAL argument of FFT must be a duration literal.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY time, N => 0)",
        "Invalid scalar argument N, should be a positive value");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY time, N => 65537)",
        "FFT transform length N must not exceed 65536.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => table1 PARTITION BY tag1 ORDER BY time, NORM => 'bad')",
        "Invalid NORM value for FFT. Supported values are backward, forward and ortho.");
    assertAnalyzeFails(
        "SELECT * FROM FFT(DATA => (SELECT time, tag1 FROM table1) PARTITION BY tag1 ORDER BY time)",
        "No numeric columns found for FFT calculation.");
  }

  private void assertAnalyzeFails(String sql, String message) {
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals(message, e.getMessage());
    }
  }

  @Test
  public void testM4TimeWindowMode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM M4("
            + "DATA => table1 PARTITION BY tag1 ORDER BY time, "
            + "TIMECOL => 'time', "
            + "SIZE => 1h)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.<String, String>builder()
                .put("time", "time")
                .put("tag1", "tag1")
                .put("tag2", "tag2")
                .put("tag3", "tag3")
                .put("attr1", "attr1")
                .put("attr2", "attr2")
                .put("s1", "s1")
                .put("s2", "s2")
                .put("s3", "s3")
                .buildOrThrow());

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("m4")
                .properOutputs(
                    "window_start",
                    "window_end",
                    "m4_tag1",
                    "m4_tag2_time",
                    "m4_tag2",
                    "m4_tag3_time",
                    "m4_tag3",
                    "m4_attr1_time",
                    "m4_attr1",
                    "m4_attr2_time",
                    "m4_attr2",
                    "m4_s1_time",
                    "m4_s1",
                    "m4_s2_time",
                    "m4_s2",
                    "m4_s3_time",
                    "m4_s3")
                .requiredSymbols("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("SIZE", 3600000L)
                        .addProperty("SLIDE", 3600000L)
                        .addProperty("ORIGIN", 0L)
                        .addProperty("__M4_WINDOW_MODE", true)
                        .addProperty("__M4_PARTITION_TYPES", "STRING")
                        .addProperty(
                            "__M4_PARTICIPANT_TYPES",
                            "STRING,STRING,STRING,STRING,INT64,INT64,DOUBLE")
                        .build());

    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, sort(tableScan))));
  }

  @Test
  public void testM4CountWindowMode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM M4("
            + "DATA => table1 PARTITION BY tag1 ORDER BY time, "
            + "TIMECOL => 'time', "
            + "SIZE => 5)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableMap.<String, String>builder()
                .put("time", "time")
                .put("tag1", "tag1")
                .put("tag2", "tag2")
                .put("tag3", "tag3")
                .put("attr1", "attr1")
                .put("attr2", "attr2")
                .put("s1", "s1")
                .put("s2", "s2")
                .put("s3", "s3")
                .buildOrThrow());

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            builder
                .name("m4")
                .properOutputs(
                    "window_index",
                    "m4_tag1",
                    "m4_tag2_time",
                    "m4_tag2",
                    "m4_tag3_time",
                    "m4_tag3",
                    "m4_attr1_time",
                    "m4_attr1",
                    "m4_attr2_time",
                    "m4_attr2",
                    "m4_s1_time",
                    "m4_s1",
                    "m4_s2_time",
                    "m4_s2",
                    "m4_s3_time",
                    "m4_s3")
                .requiredSymbols("time", "tag1", "tag2", "tag3", "attr1", "attr2", "s1", "s2", "s3")
                .handle(
                    new MapTableFunctionHandle.Builder()
                        .addProperty("SIZE", 5L)
                        .addProperty("SLIDE", 5L)
                        .addProperty("__M4_WINDOW_MODE", false)
                        .addProperty("__M4_PARTITION_TYPES", "STRING")
                        .addProperty(
                            "__M4_PARTICIPANT_TYPES",
                            "STRING,STRING,STRING,STRING,INT64,INT64,DOUBLE")
                        .build());

    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, sort(tableScan))));
  }

  @Test
  public void testM4MissingOrderBy() {
    String sql = "SELECT * FROM M4(DATA => table1 PARTITION BY tag1, TIMECOL => 'time', SIZE => 5)";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          "Table argument with set semantics requires an ORDER BY clause.", e.getMessage());
    }
  }

  @Test
  public void testM4CountWindowRejectsOrigin() {
    String sql =
        "SELECT * FROM M4(DATA => table1 PARTITION BY tag1 ORDER BY time, TIMECOL => 'time', SIZE => 5, ORIGIN => 1970-01-01T00:00:00.000+00:00)";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals("The ORIGIN argument is only supported in time window mode.", e.getMessage());
    }
  }

  @Test
  public void testM4RejectsMismatchedSlideMode() {
    String sql =
        "SELECT * FROM M4(DATA => table1 PARTITION BY tag1 ORDER BY time, TIMECOL => 'time', SIZE => 1h, SLIDE => 5)";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          "The SLIDE argument must have the same window mode as the SIZE argument.",
          e.getMessage());
    }

    sql =
        "SELECT * FROM M4(DATA => table1 PARTITION BY tag1 ORDER BY time, TIMECOL => 'time', SIZE => 5, SLIDE => 1h)";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          "The SLIDE argument must have the same window mode as the SIZE argument.",
          e.getMessage());
    }
  }

  @Test
  public void testM4RejectsDescendingOrderBy() {
    String sql =
        "SELECT * FROM M4(DATA => table1 PARTITION BY tag1 ORDER BY time DESC, TIMECOL => 'time', SIZE => 1h)";
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail();
    } catch (SemanticException e) {
      assertEquals(
          "The ORDER BY clause of the DATA argument must sort the time column in ascending order.",
          e.getMessage());
    }
  }

  // Aliases use the planner's actual symbol names so the same matcher also resolves them through
  // the ExchangeNodes of the distributed plan (which bind symbols to their own names).
  private static final ImmutableMap<String, String> TABLE1_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("time", "time")
          .put("tag1_0", "tag1")
          .put("tag2_1", "tag2")
          .put("tag3_2", "tag3")
          .put("attr1_3", "attr1")
          .put("attr2_4", "attr2")
          .put("s1_5", "s1")
          .put("s2_6", "s2")
          .put("s3_7", "s3")
          .buildOrThrow();

  private static final String LTTB_DATA_ARGUMENT =
      "DATA => table1 PARTITION BY (tag1, tag2, tag3, attr1, attr2) ORDER BY time";

  private static MapTableFunctionHandle.Builder lttbHandle(String mode) {
    return new MapTableFunctionHandle.Builder()
        .addProperty(LTTBTableFunction.MODE_PROPERTY, mode)
        .addProperty(
            LTTBTableFunction.PARTITION_TYPES_PROPERTY, "STRING,STRING,STRING,STRING,STRING")
        .addProperty(LTTBTableFunction.PARTICIPANT_TYPES_PROPERTY, "INT64,INT64,DOUBLE");
  }

  private static TableFunctionProcessorMatcher.Builder lttbMatcher(
      TableFunctionProcessorMatcher.Builder builder, String... windowColumns) {
    ImmutableList.Builder<String> properOutputs = ImmutableList.builder();
    properOutputs.add(windowColumns);
    properOutputs.add(
        "lttb_tag1",
        "lttb_tag2",
        "lttb_tag3",
        "lttb_attr1",
        "lttb_attr2",
        "lttb_s1_time",
        "lttb_s1",
        "lttb_s2_time",
        "lttb_s2",
        "lttb_s3_time",
        "lttb_s3");
    return builder
        .name("lttb")
        .properOutputs(properOutputs.build().toArray(new String[0]))
        .requiredSymbols(
            "time", "tag1_0", "tag2_1", "tag3_2", "attr1_3", "attr2_4", "s1_5", "s2_6", "s3_7");
  }

  @Test
  public void testLTTBTargetCountMode() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM LTTB(" + LTTB_DATA_ARGUMENT + ", TIMECOL => 'time', N => 100)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1", TABLE1_COLUMNS);

    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            lttbMatcher(builder, "window_index")
                .handle(
                    lttbHandle("TARGET_COUNT")
                        .addProperty(LTTBTableFunction.N_PARAMETER_NAME, 100L)
                        .build());

    // PARTITION BY several columns plans a GroupNode (a SortNode subclass) below the function
    assertPlan(
        logicalQueryPlan, anyTree(tableFunctionProcessor(tableFunctionMatcher, group(tableScan))));
    // LTTB is not mergeable: the whole partition is gathered and ordered before the function runs
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            tableFunctionProcessor(
                tableFunctionMatcher, mergeSort(exchange(), exchange(), exchange()))));
  }

  @Test
  public void testLTTBTimeWindowMode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "SELECT * FROM LTTB("
            + LTTB_DATA_ARGUMENT
            + ", TIMECOL => 'time', SIZE => 1h, SLIDE => 30m, "
            + "ORIGIN => 1970-01-01T00:00:00.000+00:00)";
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            lttbMatcher(builder, "window_start", "window_end")
                .handle(
                    lttbHandle("TIME_WINDOW")
                        .addProperty(LTTBTableFunction.SIZE_PARAMETER_NAME, 3600000L)
                        .addProperty(LTTBTableFunction.SLIDE_PARAMETER_NAME, 1800000L)
                        .addProperty(LTTBTableFunction.ORIGIN_PARAMETER_NAME, 0L)
                        .build());
    assertPlan(
        planTester.createPlan(sql),
        anyTree(
            tableFunctionProcessor(
                tableFunctionMatcher, group(tableScan("testdb.table1", TABLE1_COLUMNS)))));
  }

  @Test
  public void testLTTBCountWindowMode() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT * FROM LTTB(" + LTTB_DATA_ARGUMENT + ", TIMECOL => 'time', SIZE => 5)";
    Consumer<TableFunctionProcessorMatcher.Builder> tableFunctionMatcher =
        builder ->
            lttbMatcher(builder, "window_index")
                .handle(
                    lttbHandle("COUNT_WINDOW")
                        .addProperty(LTTBTableFunction.SIZE_PARAMETER_NAME, 5L)
                        .addProperty(LTTBTableFunction.SLIDE_PARAMETER_NAME, 5L)
                        .build());
    assertPlan(
        planTester.createPlan(sql),
        anyTree(
            tableFunctionProcessor(
                tableFunctionMatcher, group(tableScan("testdb.table1", TABLE1_COLUMNS)))));
  }

  @Test
  public void testLTTBRejectsInvalidArguments() {
    String data = "DATA => (SELECT time, tag1, s3 FROM table1) PARTITION BY tag1 ORDER BY time";
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time')",
        "Exactly one of the N and SIZE arguments must be specified for LTTB.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time', N => 10, SIZE => 1h)",
        "Exactly one of the N and SIZE arguments must be specified for LTTB.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time', N => 2)",
        "The N argument of LTTB must be at least 3.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time', N => 1h)",
        "The N argument of LTTB must be a positive integer.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB("
            + data
            + ", TIMECOL => 'time', N => 10, ORIGIN => 1970-01-01T00:00:00.000+00:00)",
        "The N argument of LTTB cannot be combined with the SLIDE or ORIGIN arguments.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time', SLIDE => 1h)",
        "Exactly one of the N and SIZE arguments must be specified for LTTB.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB("
            + data
            + ", TIMECOL => 'time', SIZE => 5, ORIGIN => 1970-01-01T00:00:00.000+00:00)",
        "The ORIGIN argument is only supported in time window mode.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(" + data + ", TIMECOL => 'time', SIZE => 1h, SLIDE => 5)",
        "The SLIDE argument must have the same window mode as the SIZE argument.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(DATA => (SELECT time, tag1, s3 FROM table1) PARTITION BY tag1 ORDER BY time DESC, TIMECOL => 'time', N => 10)",
        "The ORDER BY clause of the DATA argument must sort the time column in ascending order.");
    assertAnalyzeFails(
        "SELECT * FROM LTTB(DATA => (SELECT time, tag1, attr1, s3 FROM table1) PARTITION BY tag1 ORDER BY time, TIMECOL => 'time', N => 10)",
        "Only column with double, float, int32, int64 can be calculated by the function, attr1 is the STRING.");
  }
}
