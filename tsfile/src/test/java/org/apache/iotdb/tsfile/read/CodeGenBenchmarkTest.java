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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.format.OutputFormat;
import org.openjdk.jmh.runner.format.OutputFormatFactory;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * This class is a Benchmarking Tool for the evaluation of code generation.
 * It makes heavy use of JMH.
 *
 * To start the Benchmarking, just run the main class.
 */
@State(Scope.Benchmark)
public class CodeGenBenchmarkTest {

  private static Logger logger = LoggerFactory.getLogger(CodeGenBenchmarkTest.class);

  @Param("standard")
  public String filter;

  @Param("1")
  public int runs;

  @Param("false")
  public boolean optimize;

  @Param("100000000")
  public int datapoints;

  String fileName = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
  boolean closed = false;

  public static String getFilename() {
    String filePath =
        String.format(
            "/tmp/root.sg1/0/0/",
            "root.sg1",
            0,
            0);
    String fileName =
        100
            + FilePathUtils.FILE_NAME_SEPARATOR
            + 1
            + "-0-0.tsfile";
    return filePath.concat(fileName);
  }

  @Benchmark
  public void runBenchmark(Blackhole blackhole) throws IOException {
    List<Filter> filters = getFilter();
    // set optimizing parameters
//    Generator.active.set(this.optimize);
//    Generator.active.set(false);
//    DataSetWithTimeGenerator.generate.set(this.optimize);
    // ExecutorWithTimeGenerator.optimize.set(optimize);
    BatchDataFactory.optimize.set(false);
    runTests(filters, blackhole);
  }

  @Test
  public void generate() throws IOException, WriteProcessException {
    generate(100_000_000);
  }

  public static void generate(int datapoints) throws IOException, WriteProcessException {
    String filename = getFilename();

    logger.info("Writing {} datapoints to {}", datapoints, filename);
    File f = new File(filename);
    if (!f.getParentFile().exists()) {
      Assert.assertTrue(f.getParentFile().mkdirs());
    }
    TsFileWriter writer = new TsFileWriter(f);
    registerTimeseries(writer);

    for (long t = 0; t <= datapoints; t++) {
      // normal
      TSRecord record = new TSRecord(t, "d1");
      record.addTuple(new FloatDataPoint("s1", (float) (100.0 * Math.random())));
      record.addTuple(new IntDataPoint("s2", (int) (100.0 * Math.random())));
      writer.write(record);
    }

    writer.close();
  }

  private double runTests(List<Filter> filter, Blackhole blackhole) throws IOException {

    long start = System.nanoTime();
    for (int i = 1; i <= runs; i++) {
      logger.info("Round " + i);
      read(filter, blackhole);
    }
    long end = System.nanoTime();

    double durationMs = (end - start) / 1e6;

    return durationMs;
  }

  @Test
  public void readTest() throws IOException {
    List<Filter> filters = getSimpleFilters();
    read(filters, null);
  }

  private List<Filter> getSimpleFilters() {
    Filter filter = TimeFilter.lt(50_000_000);
    Filter filter2 = ValueFilter.gt(50);
    Filter filter3 = TimeFilter.ltEq(50_000_000);

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  private List<Filter> getFilter() {
    if ("standard".equals(this.filter)){
      return getStandardFilters();
    } else if ("simple".equals(this.filter)) {
      return getSimpleFilters();
    } else if ("complex".equals(this.filter)) {
      return getComplexFilters();
    }
    throw new IllegalStateException();
  }

  private List<Filter> getStandardFilters() {
    Filter filter = TimeFilter.lt(50_000_000);
    Filter filter2 = FilterFactory.and(ValueFilter.gt(25), ValueFilter.lt(75));
    Filter filter3 =
        FilterFactory.and(TimeFilter.gtEq(10_000_000), TimeFilter.ltEq(40_000_000));

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  private List<Filter> getComplexFilters() {
    Filter filter = FilterFactory.and(TimeFilter.gt(10_000_000), TimeFilter.lt(90_000_000));
    Filter filter2 = FilterFactory.or(FilterFactory.or(
            FilterFactory.and(ValueFilter.gt(15), ValueFilter.lt(30)),
            FilterFactory.and(ValueFilter.gt(45), ValueFilter.lt(60))
        ),
        FilterFactory.and(ValueFilter.gt(70), ValueFilter.lt(90))
    );
    Filter filter3 =
        FilterFactory.or(
            FilterFactory.and(TimeFilter.gtEq(10_000_000), TimeFilter.ltEq(20_000_000)),
            FilterFactory.and(TimeFilter.gtEq(30_000_000), TimeFilter.ltEq(40_000_000))
        );

    List<Filter> filters = Arrays.asList(
        filter, filter2, filter3
    );
    return filters;
  }

  public void read(List<Filter> filters, Blackhole blackhole) throws IOException {
    TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(getFilename());
    TsFileReader fileReader = new TsFileReader(fileSequenceReader);

    IExpression IExpression =
        BinaryExpression.or(
            BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1", "s1"), filters.get(0)),
                new SingleSeriesExpression(new Path("d1", "s2"), filters.get(1))),
            new GlobalTimeExpression(filters.get(2)));

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1"))
            .addSelectedPath(new Path("d1", "s2"))
            .setExpression(IExpression);

    fileSequenceReader.getAllDevices();

    logger.debug("Starting query...");
    QueryDataSet dataSet = fileReader.query(queryExpression);


    logger.debug("Query done, start iteration...");

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      if (blackhole != null) {
        blackhole.consume(rowRecord);
      }
      count++;
    }

    logger.debug("Iterartion done, " + count + " points");
  }

  private static void registerTimeseries(TsFileWriter writer) {
    // register nonAligned timeseries "d1.s1","d1.s2","d1.s3"
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals("given nonAligned timeseries d1.s1 has been registered.", e.getMessage());
    }
    try {
      List<MeasurementSchema> schemas = new ArrayList<>();
      schemas.add(
          new MeasurementSchema(
              "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      writer.registerAlignedTimeseries(new Path("d1"), schemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d1 has been registered for nonAligned timeseries.", e.getMessage());
    }
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(
        new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    schemas.add(
        new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    writer.registerTimeseries(new Path("d1"), schemas);

    // Register aligned timeseries "d2.s1" , "d2.s2", "d2.s3"
    try {
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new MeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
      writer.registerAlignedTimeseries(new Path("d2"), measurementSchemas);
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries and should not be expanded.",
          e.getMessage());
    }
    try {
      writer.registerTimeseries(
          new Path("d2"),
          new MeasurementSchema(
              "s5", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals(
          "given device d2 has been registered for aligned timeseries.", e.getMessage());
    }

    /*try {
      for (int i = 2; i < 3; i++) {
        writer.registerTimeseries(
            new Path("d" + i, "s1"),
            new MeasurementSchema(
                "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }*/
  }

  @Setup(Level.Trial)
  public void setup() throws IOException, WriteProcessException {
    generate(datapoints);
  }

  /**
   * Benchmark                             (filter)  (generateSeries)  (optimizeFilters)  (runs)  Mode  Cnt      Score       Error  Units
   * TsFilterPerformanceTest.runBenchmark  standard             false              false       1  avgt    5   8665,837 ±   703,708  ms/op
   * TsFilterPerformanceTest.runBenchmark  standard             false              false       5  avgt    5  43861,836 ±  8981,774  ms/op
   * TsFilterPerformanceTest.runBenchmark  standard             false              false      10  avgt    5  88036,981 ± 13661,095  ms/op
   * @param args
   * @throws RunnerException
   */
  public static void main(String[] args) throws RunnerException, IOException, WriteProcessException {
    // Start with the execution
    OptionsBuilder optionsBuilder = new OptionsBuilder();
    optionsBuilder
        .include("runBenchmark")
        .forks(1)
        .measurementIterations(3)
        .warmupIterations(3)
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MILLISECONDS)
        .param("datapoints", "1000")
        .param("optimize", "false", "true")
        .param("filter", "standard")
    //        .param("runs", "1")
    ;
    OutputFormat outputFormat = OutputFormatFactory.createFormatInstance(System.out, VerboseMode.NORMAL);
    Runner runner = new Runner(optionsBuilder.build(), outputFormat);

    Collection<RunResult> results = runner.run();

    printResults(results);
  }

  private static void printResults(Collection<RunResult> results) {
    // Prepare Output for csv
    StringBuilder sb = new StringBuilder();
    RunResult first = results.iterator().next();

    ArrayList<String> orderedParameters = new ArrayList<>(first.getParams().getParamsKeys());

    for (RunResult result : results) {
      System.out.println("\n===================");
      for (String paramsKey : result.getParams().getParamsKeys()) {
        String paramValue = result.getParams().getParam(paramsKey);
        System.out.printf("%s: %s\n", paramsKey, paramValue);
      }

      // Now calculate the baseline
      double duration = result.getPrimaryResult().getScore();

      // Now find the score of the base run with this parameters
      Optional<RunResult> baseline = findBaselineForResult(results, result);

      if (!baseline.isPresent()) {
        System.out.println(" -- no baseline found -- ");
        continue;
      }

      double baseLineDuration = baseline.get().getPrimaryResult().getScore();

      double improvement = 100.0 * (1 - duration/baseLineDuration);

      String scoreUnit = result.getPrimaryResult().getScoreUnit();
      System.out.printf("Result: %.2f %s\n", duration, scoreUnit);
      System.out.printf("Baseline: %.2f %s\n", baseLineDuration, scoreUnit);
      System.out.printf("Improvement: %.2f %%\n", improvement);


      // Create all Parameters
      String prefix = orderedParameters.stream().map(key -> result.getParams().getParam(key)).collect(Collectors.joining(";"));

      sb.append(String.format(Locale.ENGLISH, "%s;%f;%f;%f\n", prefix, duration, baseLineDuration, improvement/100.0));
    }

    System.out.println("Final Result");
    System.out.println("==========");
    System.out.println(sb);
  }

  private static Optional<RunResult> findBaselineForResult(Collection<RunResult> results, RunResult result) {
    Optional<RunResult> baseline = results.stream().filter(new Predicate<RunResult>() {
      @Override
      public boolean test(RunResult runResult) {
        boolean baseline = "false".equals(runResult.getParams().getParam("optimize"));

        Set<String> compareKeys = result.getParams().getParamsKeys().stream().filter(key -> !Arrays.asList("optimize").contains(key)).collect(Collectors.toSet());

        boolean same = compareKeys.stream().allMatch(key -> result.getParams().getParam(key).equals(runResult.getParams().getParam(key)));

        return same && baseline;
      }
    }).findAny();
    return baseline;
  }

}
