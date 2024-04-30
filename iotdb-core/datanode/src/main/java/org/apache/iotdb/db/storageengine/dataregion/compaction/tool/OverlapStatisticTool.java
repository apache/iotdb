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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OverlapStatisticTool {

  private static final String WORKER_NUM_ARG = "worker_num";
  public static final int DEFAULT_WORKER_NUM = 4;
  private static final String SUB_TASK_NUM_ARG = "sub_task_num";
  public static final int DEFAULT_WORKER_SUB_TASK_NUM = 1;
  private static final String DATA_DIRS_ARG = "data_dirs";

  public static int workerNum;
  public static int subTaskNum;
  public static List<String> dataDirs;

  public static Lock outputInfolock = new ReentrantLock();
  public static long seqFileCount = 0;
  public static long processedTimePartitionCount = 0;
  public static long processedSeqFileCount = 0;
  public static final Map<String, Pair<List<String>, List<String>>> timePartitionFileMap =
      new HashMap<>();

  public static void main(String[] args) throws InterruptedException {
    // process parameters to get the path to the data directory from the input
    parseArgs(args);

    OverlapStatisticTool tool = new OverlapStatisticTool();
    long startTime = System.currentTimeMillis();
    tool.process(dataDirs);
    System.out.printf(
        "Total time cost: %.2fs\n", ((double) System.currentTimeMillis() - startTime) / 1000);
  }

  public static void parseArgs(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    workerNum =
        Integer.parseInt(
            getArgOrDefault(commandLine, WORKER_NUM_ARG, String.valueOf(DEFAULT_WORKER_NUM)));
    subTaskNum =
        Integer.parseInt(
            getArgOrDefault(
                commandLine, SUB_TASK_NUM_ARG, String.valueOf(DEFAULT_WORKER_SUB_TASK_NUM)));
    String[] dataDirsParam = commandLine.getOptionValues(DATA_DIRS_ARG);

    if (dataDirsParam == null || dataDirsParam.length == 0) {
      throw new RuntimeException("data_dirs must not be empty");
    }
    dataDirs = Arrays.asList(dataDirsParam);
  }

  private static Options createOptions() {
    Options options = new Options();
    options
        .addOption(
            Option.builder()
                .argName(WORKER_NUM_ARG)
                .longOpt(WORKER_NUM_ARG)
                .hasArg()
                .desc("Concurrent time partition num(default: 10)")
                .build())
        .addOption(
            Option.builder()
                .argName(SUB_TASK_NUM_ARG)
                .longOpt(SUB_TASK_NUM_ARG)
                .hasArg()
                .desc("Concurrent file num in one time partition(default: 10)")
                .build())
        .addOption(
            Option.builder()
                .argName(DATA_DIRS_ARG)
                .longOpt(DATA_DIRS_ARG)
                .hasArg()
                .desc("Data dirs(Required)")
                .required()
                .build());
    return options;
  }

  private static String getArgOrDefault(CommandLine commandLine, String arg, String defaultValue) {
    String value = commandLine.getOptionValue(arg);
    return value == null ? defaultValue : value;
  }

  public void process(List<String> dataDirs) throws InterruptedException {
    processDataDirs(dataDirs);

    int workerNum = Math.min(timePartitionFileMap.size(), OverlapStatisticTool.workerNum);
    TimePartitionProcessWorker[] workers = constructWorkers(workerNum);

    CountDownLatch countDownLatch = new CountDownLatch(workerNum);
    for (TimePartitionProcessWorker worker : workers) {
      worker.run(countDownLatch);
    }
    countDownLatch.await();

    OverlapStatistic statistic = new OverlapStatistic();
    for (TimePartitionProcessWorker worker : workers) {
      for (OverlapStatistic partialRet : worker.getWorkerResults()) {
        statistic.merge(partialRet);
      }
    }
    PrintUtil.printOneStatistics(statistic, "All EXECUTED");
  }

  public TimePartitionProcessWorker[] constructWorkers(int workerNum) {
    TimePartitionProcessWorker[] workers = new TimePartitionProcessWorker[workerNum];

    int workerIdx = 0;
    for (Map.Entry<String, Pair<List<String>, List<String>>> timePartitionFilesEntry :
        timePartitionFileMap.entrySet()) {
      String timePartition = timePartitionFilesEntry.getKey();
      Pair<List<String>, List<String>> timePartitionFiles = timePartitionFilesEntry.getValue();

      if (workers[workerIdx] == null) {
        workers[workerIdx] = new TimePartitionProcessWorker();
      }

      workers[workerIdx].addTask(new TimePartitionProcessTask(timePartition, timePartitionFiles));
      workerIdx = (workerIdx + 1) % workerNum;
    }
    return workers;
  }

  private void processDataDirs(List<String> dataDirs) {
    // 1. Traverse all time partitions and construct timePartitions
    // 2. Count the total number of sequential files
    for (String dataDirPath : dataDirs) {
      File dataDir = new File(dataDirPath);
      if (!dataDir.exists() || !dataDir.isDirectory()) {
        continue;
      }
      processDataDirWithIsSeq(dataDirPath, true);
      processDataDirWithIsSeq(dataDirPath, false);
    }
  }

  private void processDataDirWithIsSeq(String dataDirPath, boolean isSeq) {
    String dataDirWithIsSeq;
    if (isSeq) {
      dataDirWithIsSeq = dataDirPath + File.separator + "sequence";
    } else {
      dataDirWithIsSeq = dataDirPath + File.separator + "unsequence";
    }
    File dataDirWithIsSequence = new File(dataDirWithIsSeq);
    if (!dataDirWithIsSequence.exists() || !dataDirWithIsSequence.isDirectory()) {
      System.out.println(dataDirWithIsSequence + " is not a correct path");
      return;
    }

    for (File storageGroupDir : Objects.requireNonNull(dataDirWithIsSequence.listFiles())) {
      if (!storageGroupDir.isDirectory()) {
        continue;
      }
      String storageGroup = storageGroupDir.getName();
      for (File dataRegionDir : Objects.requireNonNull(storageGroupDir.listFiles())) {
        if (!dataRegionDir.isDirectory()) {
          continue;
        }
        String dataRegion = dataRegionDir.getName();
        for (File timePartitionDir : Objects.requireNonNull(dataRegionDir.listFiles())) {
          if (!timePartitionDir.isDirectory()) {
            continue;
          }

          String timePartitionKey =
              calculateTimePartitionKey(storageGroup, dataRegion, timePartitionDir.getName());
          Pair<List<String>, List<String>> timePartitionFiles =
              timePartitionFileMap.computeIfAbsent(
                  timePartitionKey, v -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
          for (File file : Objects.requireNonNull(timePartitionDir.listFiles())) {
            if (!file.isFile()) {
              continue;
            }
            if (!file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
              continue;
            }
            String resourceFilePath = file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;
            if (!new File(resourceFilePath).exists()) {
              System.out.println(
                  resourceFilePath
                      + " is not exist, the tsfile is skipped because it is not closed.");
              continue;
            }
            String filePath = file.getAbsolutePath();
            if (isSeq) {
              timePartitionFiles.left.add(filePath);
              seqFileCount++;
            } else {
              timePartitionFiles.right.add(filePath);
            }
          }
        }
      }
    }
  }

  private String calculateTimePartitionKey(
      String storageGroup, String dataRegion, String timePartition) {
    return storageGroup + "-" + dataRegion + "-" + timePartition;
  }
}
