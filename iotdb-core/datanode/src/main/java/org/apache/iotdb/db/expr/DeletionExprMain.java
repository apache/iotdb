/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.expr;

import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.distribution.FixedIntervalGenerator;
import org.apache.iotdb.db.expr.entity.SimDeletion;
import org.apache.iotdb.db.expr.entity.SimTsFile;
import org.apache.iotdb.db.expr.entity.SimpleModAllocator;
import org.apache.iotdb.db.expr.event.Event;
import org.apache.iotdb.db.expr.event.ExecuteLastPointQueryEvent;
import org.apache.iotdb.db.expr.event.ExecuteRangeQueryEvent;
import org.apache.iotdb.db.expr.event.GenerateDeletionEvent;
import org.apache.iotdb.db.expr.event.GenerateTsFileEvent;
import org.apache.iotdb.db.expr.simulator.SimpleSimulator;

import org.apache.tsfile.read.common.TimeRange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeletionExprMain {

  private SimulationConfig config;
  private SimpleSimulator simulator;
  private SimpleModAllocator simpleModAllocator;
  private long maxStep;
  private long maxTimestamp;
  private ExprReport report;
  private static int maxFileCntThreshold = 30;
  private static int cntThresholdStep = 1;

  public DeletionExprMain() {
    init();
    initReport();
  }

  public void init() {
    config = new SimulationConfig();
    simulator = new SimpleSimulator(config);
    simpleModAllocator = new SimpleModAllocator(config, simulator.getSimulationContext());
    maxStep = 10000;
    maxTimestamp = Long.MAX_VALUE;
  }

  public void initReport() {
    report = new ExprReport();
  }

  private List<Event> initEvents() {
    List<Event> events = new ArrayList<>();

    SimTsFile initTsFile =
        new SimTsFile(0, new TimeRange(0, config.tsfileRange), simpleModAllocator::allocate);
    GenerateTsFileEvent generateTsFileEvent =
        new GenerateTsFileEvent(
            config,
            initTsFile,
            config.tsfileRange,
            new FixedIntervalGenerator(config.generateTsFileInterval));
    events.add(generateTsFileEvent);

    GenerateDeletionEvent generatePartialDeletionEvent =
        new GenerateDeletionEvent(
            config,
            new SimDeletion(
                new TimeRange(
                    config.partialDeletionOffset,
                    config.partialDeletionOffset + config.partialDeletionRange)),
            config.partialDeletionStep,
            new FixedIntervalGenerator(config.generatePartialDeletionInterval));
    generatePartialDeletionEvent.generateTimestamp = config.deletionStartTime;
    GenerateDeletionEvent generateFullDeletionEvent =
        new GenerateDeletionEvent(
            config,
            new SimDeletion(new TimeRange(0, Long.MAX_VALUE)),
            0,
            new FixedIntervalGenerator(config.generateFullDeletionInterval));
    generateFullDeletionEvent.generateTimestamp = config.deletionStartTime;
    events.add(generatePartialDeletionEvent);
    events.add(generateFullDeletionEvent);

    ExecuteRangeQueryEvent executeRangeQueryEvent =
        new ExecuteRangeQueryEvent(
            config,
            new TimeRange(
                config.rangeQueryOffset, config.rangeQueryRange + config.rangeQueryOffset),
            config.rangeQueryStep,
            new FixedIntervalGenerator(config.rangeQueryInterval));
    ExecuteLastPointQueryEvent executeLastPointQueryEvent =
        new ExecuteLastPointQueryEvent(
            config, new TimeRange(0, 1), new FixedIntervalGenerator(config.pointQueryInterval));
    ExecuteRangeQueryEvent executeFullQueryEvent =
        new ExecuteRangeQueryEvent(
            config,
            new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE),
            0,
            new FixedIntervalGenerator(config.fullQueryInterval));
    events.add(executeRangeQueryEvent);
    events.add(executeLastPointQueryEvent);
    events.add(executeFullQueryEvent);

    return events;
  }

  public void doExpr(boolean printState) {
    simulator.maxStep = maxStep;
    simulator.maxTimestamp = maxTimestamp;

    List<Event> events = initEvents();
    simulator.addEvents(events);

    simulator.start();

    writeReport();

    if (printState) {
      System.out.println(simulator);
      System.out.println(simulator.getStatistics());
      System.out.println(
          simulator.getSimulationContext().modFileManager.modFileList.stream()
              .map(s -> s.mods.size())
              .collect(Collectors.toList()));
      System.out.println(
          simulator.getSimulationContext().modFileManager.modFileList.stream()
              .map(s -> s.tsfileReferences.size())
              .collect(Collectors.toList()));
    }
  }

  private void writeReport() {
    report.deletionWriteTime =
        simulator.getStatistics().partialDeletionExecutedTime
            + simulator.getStatistics().fullDeletionExecutedTime;
    report.deletionTimeList.add(report.deletionWriteTime);
    report.deletionReadTime = simulator.getStatistics().queryReadDeletionTime;
    report.queryTimeList.add(report.deletionReadTime);
    report.totalTimeList.add(
        (long) (report.deletionWriteTime * config.writeTimeWeight) + report.deletionReadTime);
  }

  public static class ExprReport {

    public List<Long> deletionTimeList = new ArrayList<>();
    public List<Long> queryTimeList = new ArrayList<>();
    public List<Long> totalTimeList = new ArrayList<>();
    public long deletionWriteTime;
    public long deletionReadTime;

    public void print() {
      System.out.println(deletionTimeList);
      System.out.println(queryTimeList);
      System.out.println(totalTimeList);
    }
  }

  @FunctionalInterface
  public interface Configurer {

    void configure(DeletionExprMain exprMain, int i);
  }

  public static ExprReport oneExpr(Configurer configurer, int exprNum, boolean runBaseLine) {

    DeletionExprMain expr = new DeletionExprMain();

    if (runBaseLine) {
      // baseline, each TsFile has one Mod File
      initExpr(expr);
      configurer.configure(expr, exprNum);
      expr.config.modFileCntThreshold = Integer.MAX_VALUE;
      expr.config.modFileSizeThreshold = 0;
      expr.doExpr(true);
    }

    // use modFileCntThreshold as the x-axis
    for (int i = 1; i < maxFileCntThreshold; i+=cntThresholdStep) {
      initExpr(expr);
      configurer.configure(expr, exprNum);
      expr.config.modFileCntThreshold = i;
      expr.doExpr(true);
    }

    return expr.report;
  }

  public static void initExpr(DeletionExprMain expr) {
    expr.init();

    expr.maxStep = Long.MAX_VALUE;
    expr.maxTimestamp = 24 * 60 * 60 * 1000 * 1000L;
    expr.config.rangeQueryInterval = 1000_000;
    expr.config.fullQueryInterval = 1000_000;
    expr.config.pointQueryInterval = 1000_000;
    expr.config.generateFullDeletionInterval = 2_000_000;
    expr.config.generatePartialDeletionInterval = 2_000_000;
    expr.config.partialDeletionRange = expr.config.tsfileRange * 3;
    expr.config.partialDeletionOffset = -expr.config.partialDeletionRange;
    expr.config.partialDeletionStep =
        (long)
            (expr.config.tsfileRange
                / (1.0
                * expr.config.generateTsFileInterval
                / expr.config.generatePartialDeletionInterval));

    expr.config.generateTsFileInterval = 10_000_000;
    expr.config.modFileSizeThreshold = 64 * 1024;
    expr.config.deletionStartTime = 1000 * expr.config.generateTsFileInterval;
    //    expr.config.queryRange = maxTimestamp;
    //    expr.config.queryStep = 0;
    expr.config.rangeQueryRange = expr.config.tsfileRange * 1000;
    expr.config.rangeQueryStep =
        expr.config.tsfileRange
            / (expr.config.generateTsFileInterval / expr.config.rangeQueryInterval);
    expr.config.rangeQueryOffset =
        -expr.config.rangeQueryRange
            + expr.config.deletionStartTime
            / expr.config.generateTsFileInterval
            * expr.config.tsfileRange;
  }

  private static void parallelExpr(
      Configurer configurer,
      int exprNum,
      Function<Integer, String> argsToString,
      boolean runBaseline)
      throws ExecutionException, InterruptedException {
    ExecutorService service = Executors.newCachedThreadPool();
    List<Future<ExprReport>> asyncReports = new ArrayList<>();
    for (int i = 0; i < exprNum; i++) {
      int finalI = i;
      asyncReports.add(service.submit(() -> oneExpr(configurer, finalI, runBaseline)));
    }

    for (Future<ExprReport> asyncReport : asyncReports) {
      asyncReport.get();
    }

    for (int i = 0; i < asyncReports.size(); i++) {
      System.out.println(argsToString.apply(i));
      asyncReports.get(i).get().print();
    }
    service.shutdownNow();
  }

  private static void testSizeThreshold() throws ExecutionException, InterruptedException {
    String argName = "sizeThreshold";
    long[] exprArgs =
        new long[]{
            16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024,
        };
    Configurer configurer =
        (expr, j) -> {
          expr.config.modFileSizeThreshold = exprArgs[j];
        };
    parallelExpr(configurer, exprArgs.length, (i) -> argName + ":" + exprArgs[i], true);
  }

  private static void testQueryInterval() throws ExecutionException, InterruptedException {
    String argName = "queryInterval";
    long[] exprArgs = new long[]{500_000, 1000_000, 1500_000, 2000_000, 2500_000};
    Configurer configurer =
        (expr, j) -> {
          expr.config.pointQueryInterval = exprArgs[j];
          expr.config.rangeQueryInterval = exprArgs[j];
          expr.config.fullQueryInterval = exprArgs[j];
        };
    parallelExpr(configurer, exprArgs.length, (i) -> argName + ":" + exprArgs[i], true);
  }

  private static void testSimulationTime() throws ExecutionException, InterruptedException {
    String argName = "simulationTime";
    long[] exprArgs =
        new long[]{
            24 * 60 * 60 * 1000 * 1000L,
            2 * 24 * 60 * 60 * 1000 * 1000L,
            3 * 24 * 60 * 60 * 1000 * 1000L,
            4 * 24 * 60 * 60 * 1000 * 1000L,
            5 * 24 * 60 * 60 * 1000 * 1000L
        };
    Configurer configurer =
        (expr, j) -> {
          expr.maxTimestamp = exprArgs[j];
        };
    parallelExpr(configurer, exprArgs.length, (i) -> argName + ":" + exprArgs[i], false);
  }

  private static void testDeletionRatio() throws ExecutionException, InterruptedException {
    String argName1 = "fullDeletionInterval";
    long[] exprArgs1;
    exprArgs1 = new long[]{200_000_000, 20_000_000, 2_000_000, 2_000_000, 2_000_000};
//    exprArgs1 = new long[]{2_000_000, 2_000_000};
    String argName2 = "partialDeletionInterval";
    long[] exprArgs2;
    exprArgs2 = new long[]{
        2_000_000, 2_000_000, 2_000_000, 20_000_000, 200_000_000,
    };
//    exprArgs2 =
//        new long[]{
//            20_000_000, 200_000_000,
//        };
    Configurer configurer =
        (expr, j) -> {
          expr.config.generateFullDeletionInterval = exprArgs1[j];
          expr.config.generatePartialDeletionInterval = exprArgs2[j];
          expr.config.partialDeletionStep =
              (long)
                  (expr.config.tsfileRange
                      / (1.0
                      * expr.config.generateTsFileInterval
                      / expr.config.generatePartialDeletionInterval));
        };
    parallelExpr(
        configurer,
        exprArgs1.length,
        (i) -> argName1 + ":" + exprArgs1[i] + ";" + argName2 + ":" + exprArgs2[i],
        true);
  }

  private static void testTsFileGenInterval() throws ExecutionException, InterruptedException {
    String argName = "tsFileGenerationInterval";
    long[] exprArgs = new long[]{
        10_000_000L,
        20_000_000L,
        40_000_000L,
        80_000_000L,
        160_000_000L,
    };
    Configurer configurer = (expr, j) -> {
      expr.config.generateTsFileInterval = exprArgs[j];
      expr.config.partialDeletionStep = (long) (expr.config.tsfileRange / (
          1.0 * expr.config.generateTsFileInterval / expr.config.generatePartialDeletionInterval));
      expr.config.rangeQueryStep = expr.config.tsfileRange / (expr.config.generateTsFileInterval
          / expr.config.rangeQueryInterval);
      expr.config.rangeQueryOffset = -expr.config.rangeQueryRange
          + expr.config.deletionStartTime / expr.config.generateTsFileInterval
          * expr.config.tsfileRange;
    };
    parallelExpr(configurer, exprArgs.length, (i) -> argName + ":" + exprArgs[i], true);
  }


  public static void main(String[] args) throws ExecutionException, InterruptedException {
    maxFileCntThreshold = 101;
    cntThresholdStep = 5;

        testSizeThreshold();
    //    testQueryInterval();
    //    testSimulationTime();
//    testDeletionRatio();
//    testTsFileGenInterval();
  }
}
