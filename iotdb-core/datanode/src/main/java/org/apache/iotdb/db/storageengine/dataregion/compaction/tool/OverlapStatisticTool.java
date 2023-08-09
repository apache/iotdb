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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.Pair;

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
  public static long seqFileCount = 0;

  public static Lock outputInfolock = new ReentrantLock();
  public static long processedTimePartitionCount = 0;
  public static long processedSeqFileCount = 0;
  public static final Map<String, Pair<List<String>, List<String>>> timePartitionFileMap =
      new HashMap<>();

  public static void main(String[] args) throws InterruptedException {
    if (args.length == 0) {
      System.out.println("Please input data dir paths.");
      return;
    }
    OverlapStatisticTool tool = new OverlapStatisticTool();
    long startTime = System.currentTimeMillis();
    // 1. 处理参数，从输入中获取数据目录的路径
    List<String> dataDirs = tool.getDataDirsFromArgs(args);
    // 2. 进行计算
    tool.process(dataDirs);
    System.out.printf(
        "Total time cost: %.2fs\n", ((double) System.currentTimeMillis() - startTime) / 1000);
  }

  private List<String> getDataDirsFromArgs(String[] args) {
    return new ArrayList<>(Arrays.asList(args));
  }

  public void process(List<String> dataDirs) throws InterruptedException {
    // 0. 预处理
    processDataDirs(dataDirs);

    // 1. 构造最终结果集
    OverlapStatistic statistic = new OverlapStatistic();
    List<TimePartitionProcessTask> taskList = new ArrayList<>();
    for (Map.Entry<String, Pair<List<String>, List<String>>> timePartitionFilesEntry :
        timePartitionFileMap.entrySet()) {
      String timePartition = timePartitionFilesEntry.getKey();
      Pair<List<String>, List<String>> timePartitionFiles = timePartitionFilesEntry.getValue();
      taskList.add(new TimePartitionProcessTask(timePartition, timePartitionFiles));
    }
    int workerNum = Math.min(taskList.size(), 10);
    TimePartitionProcessWorker[] workers = new TimePartitionProcessWorker[workerNum];
    for (int i = 0; i < taskList.size(); i++) {
      int workerIdx = i % 10;
      TimePartitionProcessWorker worker = workers[workerIdx];
      if (worker == null) {
        worker = new TimePartitionProcessWorker();
        workers[workerIdx] = worker;
      }
      worker.addTask(taskList.get(i));
    }
    CountDownLatch countDownLatch = new CountDownLatch(workerNum);
    for (TimePartitionProcessWorker worker : workers) {
      worker.run(countDownLatch);
    }
    countDownLatch.await();
    for (TimePartitionProcessWorker worker : workers) {
      for (OverlapStatistic partialRet : worker.getWorkerResults()) {
        statistic.merge(partialRet);
      }
    }
    PrintUtil.printOneStatistics(statistic, "All EXECUTED");
  }

  private void processDataDirs(List<String> dataDirs) {
    // 1. 遍历所有的时间分区，构造 timePartitions
    // 2. 统计顺序文件的总数
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
