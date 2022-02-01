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
 *
 */
package org.apache.iotdb.db.newsync.receiver.collector;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.sender.recovery.TsFilePipeLogAnalyzer;
import org.apache.iotdb.db.newsync.utils.SyncConstant;
import org.apache.iotdb.db.newsync.utils.SyncPathUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

/** scan sync receiver folder and load pipeData into IoTDB */
public class Collector {

  private static final Logger logger = LoggerFactory.getLogger(Collector.class);
  // TODO: multi thread for multi pipe
  private ExecutorService executorService;
  private ScanTask task;

  public Collector() {
    this.executorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
    this.task = new ScanTask();
  }

  public void startCollect() {
    task.start();
    executorService =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
    executorService.submit(task);
  }

  public void stopCollect() {
    task.stop();
    executorService.shutdown();
  }

  public void startPipe(String pipeName, String remoteIp, long createTime) {
    task.addScanDir(SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
  }

  public void stopPipe(String pipeName, String remoteIp, long createTime) {
    task.removeScanDir(SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime));
  }

  private class ScanTask implements Runnable {
    private final Set<String> scanPathSet;
    private volatile boolean stopped;

    private ScanTask() {
      scanPathSet = new HashSet<>();
      stopped = false;
    }

    private void addScanDir(String dirPath) {
      scanPathSet.add(dirPath);
    }

    private void removeScanDir(String dirPath) {
      scanPathSet.remove(dirPath);
    }

    private void start() {
      this.stopped = false;
    }

    private void stop() {
      this.stopped = true;
    }

    @Override
    public void run() {
      try {
        while (!stopped) {
          for (String scanPath : scanPathSet) {
            File dir = new File(scanPath);
            if (dir.exists() && dir.isDirectory()) {
              List<PipeData> pipeDataList = new ArrayList<>();
              File[] files = dir.listFiles((d, s) -> !s.endsWith(SyncConstant.COLLECTOR_SUFFIX));
              DataOutputStream outputStream = null;
              int startNumber = -1;
              if (files.length > 0) {
                // TODO: Assuming that the file name is incremented by number
                Arrays.sort(files, Comparator.comparingLong(o -> Long.parseLong(o.getName())));
                try {
                  pipeDataList = TsFilePipeLogAnalyzer.parseFile(files[0]);
                } catch (IOException e) {
                  logger.error("Parse pipe data log {} error.", files[0].getPath());
                }
                File recordFile = new File(files[0].getPath() + SyncConstant.COLLECTOR_SUFFIX);
                if (recordFile.exists()) {
                  RandomAccessFile raf = new RandomAccessFile(recordFile, "r");
                  if (raf.length() > Integer.BYTES) {
                    raf.seek(raf.length() - Integer.BYTES);
                    startNumber = raf.readInt();
                  }
                  raf.close();
                }
                outputStream = new DataOutputStream(new FileOutputStream(recordFile, true));
              } else {
                // TODO: get buffer from transport
                // pipeDataList = transport.getBufferPipeDataList();
              }
              boolean allLoaded = true;
              for (int i = startNumber + 1; i < pipeDataList.size(); i++) {
                allLoaded = false;
                PipeData pipeData = pipeDataList.get(i);
                try {
                  logger.info(
                      "Start load pipeData with serialize number {} and type {}",
                      pipeData.getSerialNumber(),
                      pipeData.getType());
                  pipeData.createLoader().load();
                  if (outputStream != null) {
                    outputStream.writeInt(i);
                  }
                  if (i == pipeDataList.size() - 1) {
                    allLoaded = true;
                  }
                } catch (Exception e) {
                  // TODO: how to response error message to sender?
                  // TODO: should drop this pipe?
                  logger.error(
                      "Cannot load pipeData with serialize number {} and type {}, because {}",
                      pipeData.getSerialNumber(),
                      pipeData.getType(),
                      e.getMessage());
                  break;
                }
              }
              if (allLoaded) {
                // if all success loaded, remove pipelog and record file
                outputStream.close();
                Files.deleteIfExists(files[0].toPath());
                Files.deleteIfExists(Paths.get(files[0].getPath() + SyncConstant.COLLECTOR_SUFFIX));
              }
            }
          }
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }
  }
}
