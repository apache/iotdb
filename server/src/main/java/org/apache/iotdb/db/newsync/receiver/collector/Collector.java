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
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.utils.BufferedPipeDataBlockingQueue;
import org.apache.iotdb.db.newsync.utils.SyncConstant;
import org.apache.iotdb.db.newsync.utils.SyncPathUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** scan sync receiver folder and load pipeData into IoTDB */
public class Collector {

  private static final Logger logger = LoggerFactory.getLogger(Collector.class);
  private static final int WAIT_TIMEOUT = 2000;
  private ExecutorService executorService;
  private Map<String, Future> taskFutures;

  public Collector() {
    taskFutures = new HashMap<>();
  }

  public void startCollect() {
    this.executorService =
        IoTDBThreadPoolFactory.newCachedThreadPool(ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
  }

  public void stopCollect() {
    for (Future f : taskFutures.values()) {
      f.cancel(true);
    }
    if (executorService != null) {
      executorService.shutdownNow();
      int totalWaitTime = WAIT_TIMEOUT;
      while (!executorService.isTerminated()) {
        try {
          if (!executorService.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
            logger.info(
                "{} thread pool doesn't exit after {}ms.",
                ThreadName.SYNC_RECEIVER_COLLECTOR.getName(),
                totalWaitTime);
          }
          totalWaitTime += WAIT_TIMEOUT;
        } catch (InterruptedException e) {
          logger.error(
              "Interrupted while waiting {} thread pool to exit. ",
              ThreadName.SYNC_RECEIVER_COLLECTOR.getName());
          Thread.currentThread().interrupt();
        }
      }
      executorService = null;
    }
  }

  public void startPipe(String pipeName, String remoteIp, long createTime) {
    String dir = SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime);
    ScanTask task = new ScanTask(dir);
    taskFutures.put(dir, executorService.submit(task));
  }

  public void stopPipe(String pipeName, String remoteIp, long createTime) {
    String dir = SyncPathUtil.getReceiverPipeLogDir(pipeName, remoteIp, createTime);
    taskFutures.get(dir).cancel(true);
    taskFutures.remove(dir);
  }

  private static BufferedPipeDataBlockingQueue parsePipeLogToBlockingQueue(File file)
      throws IOException {
    BufferedPipeDataBlockingQueue blockingQueue =
        new BufferedPipeDataBlockingQueue(file.getAbsolutePath(), 100);
    DataInputStream inputStream = new DataInputStream(new FileInputStream(file));
    try {
      while (true) {
        blockingQueue.offer(PipeData.deserialize(inputStream));
      }
    } catch (EOFException e) {
      logger.info(String.format("Finish parsing pipeLog %s.", file.getPath()));
    } catch (IllegalPathException e) {
      logger.error(String.format("Parsing pipeLog %s error, because %s", file.getPath(), e));
      throw new IOException(e);
    } finally {
      inputStream.close();
    }
    blockingQueue.end();
    return blockingQueue;
  }

  private class ScanTask implements Runnable {
    private final String scanPath;

    private ScanTask(String dirPath) {
      scanPath = dirPath;
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          File dir = new File(scanPath);
          if (dir.exists() && dir.isDirectory()) {
            BufferedPipeDataBlockingQueue pipeDataQueue;
            File[] files = dir.listFiles((d, s) -> !s.endsWith(SyncConstant.COLLECTOR_SUFFIX));
            int nextIndex = 0;

            if (files.length > 0) {
              // read from disk
              // TODO: Assuming that the file name is incremented by number
              Arrays.sort(files, Comparator.comparingLong(o -> Long.parseLong(o.getName())));
              try {
                pipeDataQueue = parsePipeLogToBlockingQueue(files[0]);
              } catch (IOException e) {
                logger.error("Parse pipe data log {} error.", files[0].getPath());
                // TODO: stop
                return;
              }
            } else {
              // read from buffer
              // TODO: get buffer from transport, this is mock implement
              pipeDataQueue = BufferedPipeDataBlockingQueue.getMock();
            }

            File recordFile = new File(pipeDataQueue.getFileName() + SyncConstant.COLLECTOR_SUFFIX);
            if (recordFile.exists()) {
              RandomAccessFile raf = new RandomAccessFile(recordFile, "r");
              if (raf.length() > Integer.BYTES) {
                raf.seek(raf.length() - Integer.BYTES);
                nextIndex = raf.readInt() + 1;
              }
              raf.close();
            }
            DataOutputStream outputStream =
                new DataOutputStream(new FileOutputStream(recordFile, true));
            while (!pipeDataQueue.isEnd()) {
              PipeData pipeData = pipeDataQueue.take();
              int currentIndex = pipeDataQueue.getAndIncreaseIndex();
              if (currentIndex < nextIndex) {
                continue;
              }
              try {
                logger.info(
                    "Start load pipeData with serialize number {} and type {}",
                    pipeData.getSerialNumber(),
                    pipeData.getType());
                pipeData.createLoader().load();
                outputStream.writeInt(currentIndex);
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
            outputStream.close();
            // if all success loaded, remove pipelog and record file
            File pipeLog = new File(pipeDataQueue.getFileName());
            if (pipeLog.exists()) {
              Files.deleteIfExists(pipeLog.toPath());
              Files.deleteIfExists(Paths.get(files[0].getPath() + SyncConstant.COLLECTOR_SUFFIX));
            }
          }
        }
      } catch (IOException e) {
        logger.error(e.getMessage());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
