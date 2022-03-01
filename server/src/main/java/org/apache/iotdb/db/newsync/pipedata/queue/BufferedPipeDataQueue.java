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
package org.apache.iotdb.db.newsync.pipedata.queue;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class BufferedPipeDataQueue implements PipeDataQueue {
  private static final Logger logger = LoggerFactory.getLogger(BufferedPipeDataQueue.class);

  private final String pipeLogDir;

  /** input */
  private long lastMaxSerialNumber;

  private BlockingDeque<PipeData> inputDeque;

  private BlockingDeque<Long> pipeLogStartNumber;
  private DataOutputStream outputStream;
  private long currentPipeLogSize;

  /** output */
  private final Object waitLock = new Object();

  private BlockingDeque<PipeData> outputDeque;

  private long pullSerialNumber;
  private long commitSerialNumber;
  private BufferedWriter commitLogWriter;
  private long currentCommitLogSize;

  private static Map<String, BufferedPipeDataQueue> instances = new ConcurrentHashMap<>();

  public static BufferedPipeDataQueue getInstance(String pipeLogDir) {
    return instances.computeIfAbsent(pipeLogDir, i -> new BufferedPipeDataQueue(pipeLogDir));
  }

  public BufferedPipeDataQueue(String pipeLogDir) {
    this.pipeLogDir = pipeLogDir;

    this.lastMaxSerialNumber = 0;
    this.pipeLogStartNumber = new LinkedBlockingDeque<>();

    this.outputDeque = new LinkedBlockingDeque<>();
    this.commitSerialNumber = Long.MIN_VALUE;

    recover();
  }

  /** recover */
  private void recover() {
    if (!new File(pipeLogDir).exists()) {
      return;
    }

    recoverPipeLogStartNumber();
    recoverLastMaxSerialNumber();
    recoverCommitSerialNumber();
    recoverOutputDeque();
  }

  private void recoverPipeLogStartNumber() {
    File logDir = new File(pipeLogDir);
    List<Long> startNumbers = new ArrayList<>();

    for (File file : logDir.listFiles())
      if (file.getName().endsWith(SyncConstant.PIPE_LOG_NAME_SUFFIX)) {
        startNumbers.add(SyncConstant.getSerialNumberFromPipeLogName(file.getName()));
      }
    if (startNumbers.size() != 0) {
      Collections.sort(startNumbers);
      for (Long startTime : startNumbers) {
        pipeLogStartNumber.offer(startTime);
      }
    }
  }

  private void recoverLastMaxSerialNumber() {
    if (pipeLogStartNumber.isEmpty()) {
      return;
    }

    File writingPipeLog =
        new File(pipeLogDir, SyncConstant.getPipeLogName(pipeLogStartNumber.peekLast()));
    try {
      List<PipeData> recoverPipeData = parsePipeLog(writingPipeLog);
      int recoverPipeDataSize = recoverPipeData.size();
      lastMaxSerialNumber =
          recoverPipeDataSize == 0
              ? pipeLogStartNumber.peekLast() - 1
              : recoverPipeData.get(recoverPipeDataSize - 1).getSerialNumber();
    } catch (IOException e) {
      logger.error(
          String.format(
              "Can not recover inputQueue from %s, because %s.", writingPipeLog.getPath(), e));
    }
  }

  private void recoverCommitSerialNumber() {
    File commitLog = new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME);
    if (!commitLog.exists()) {
      return;
    }

    try (RandomAccessFile raf = new RandomAccessFile(commitLog, "r")) {
      if (raf.length() >= Integer.BYTES) {
        raf.seek(raf.length() - Integer.BYTES);
        commitSerialNumber = raf.readInt();
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "deserialize remove serial number error, remove serial number has been set to %d, because %s",
              commitSerialNumber, e));
    }
  }

  private void recoverOutputDeque() {
    if (pipeLogStartNumber.isEmpty()) {
      return;
    }

    File readingPipeLog =
        new File(pipeLogDir, SyncConstant.getPipeLogName(pipeLogStartNumber.peek()));
    try {
      List<PipeData> recoverPipeData = parsePipeLog(readingPipeLog);
      int recoverPipeDataSize = recoverPipeData.size();
      for (int i = recoverPipeDataSize - 1; i >= 0; --i) {
        PipeData pipeData = recoverPipeData.get(i);
        if (pipeData.getSerialNumber() <= commitSerialNumber) {
          break;
        }
        outputDeque.addFirst(pipeData);
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Recover output deque from pipe log %s error, because %s.",
              readingPipeLog.getPath(), e));
    }
  }

  public long getLastMaxSerialNumber() {
    return lastMaxSerialNumber;
  }

  public long getCommitSerialNumber() {
    return commitSerialNumber;
  }

  /** input */
  @Override
  public boolean offer(PipeData pipeData) {
    if (outputStream == null || currentPipeLogSize > SyncConstant.DEFAULT_PIPE_LOG_SIZE_IN_BYTE) {
      try {
        moveToNextPipeLog(pipeData.getSerialNumber());
      } catch (IOException e) {
        logger.warn(String.format("Move to next pipe log %s error, because %s.", pipeData, e));
      }
    }
    if (!inputDeque.offer(pipeData)) {
      return false;
    }
    synchronized (waitLock) {
      waitLock.notifyAll();
    }

    try {
      writeToDisk(pipeData);
    } catch (IOException e) {
      logger.error(String.format("Record pipe data %s error, because %s.", pipeData, e));
      return false;
    }
    return true;
  }

  private synchronized void moveToNextPipeLog(long startSerialNumber) throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
    File newPipeLog = new File(pipeLogDir, SyncConstant.getPipeLogName(startSerialNumber));
    createFile(newPipeLog);

    outputStream = new DataOutputStream(new FileOutputStream(newPipeLog));
    pipeLogStartNumber.offer(startSerialNumber);
    currentPipeLogSize = 0;

    inputDeque = new LinkedBlockingDeque<>();
    if (commitSerialNumber == Long.MIN_VALUE) {
      commitSerialNumber = startSerialNumber - 1;
    }
  }

  private void writeToDisk(PipeData pipeData) throws IOException {
    // skip trick

    currentPipeLogSize += pipeData.serialize(outputStream);
    outputStream.flush();
  }

  /** output */
  private synchronized PipeData pullOnePipeData(long serialNumber) throws IOException {
    if (!outputDeque.isEmpty()) {
      return outputDeque.poll();
    } else if (outputDeque != inputDeque) {
      long writingPipeLogStartNumber = -1;
      List<Long> pullPipeLogStartNumber = new ArrayList<>();
      while (!pipeLogStartNumber.isEmpty()) {
        long number = pipeLogStartNumber.poll();
        pullPipeLogStartNumber.add(number);
        if (number > serialNumber) {
          break;
        }
        writingPipeLogStartNumber = number;
      }
      int size = pullPipeLogStartNumber.size();
      for (int i = size - 1; i >= 0; --i) {
        pipeLogStartNumber.addFirst(pullPipeLogStartNumber.get(i));
      }

      if (writingPipeLogStartNumber == pipeLogStartNumber.peekLast() && inputDeque != null) {
        outputDeque = inputDeque;
      } else {
        List<PipeData> parsePipeData =
            parsePipeLog(
                new File(pipeLogDir, SyncConstant.getPipeLogName(writingPipeLogStartNumber)));
        int parsePipeDataSize = parsePipeData.size();
        outputDeque = new LinkedBlockingDeque<>();
        for (int i = 0; i < parsePipeDataSize; i++) {
          outputDeque.offer(parsePipeData.get(i));
        }
      }
      return outputDeque.poll();
    }
    return null;
  }

  @Override
  public List<PipeData> pull(long serialNumber) {
    List<PipeData> resPipeData = new ArrayList<>();

    pullSerialNumber = commitSerialNumber;
    while (pullSerialNumber < serialNumber) {
      pullSerialNumber += 1;
      try {
        PipeData pullPipeData = pullOnePipeData(pullSerialNumber);
        if (pullPipeData != null) {
          resPipeData.add(pullPipeData);
        } else {
          pullSerialNumber -= 1;
          break;
        }
      } catch (IOException e) {
        logger.error(
            String.format(
                "Pull pipe data serial number %s error, because %s.", pullSerialNumber, e));
      }
    }

    for (int i = resPipeData.size() - 1; i >= 0; --i) {
      outputDeque.addFirst(resPipeData.get(i));
    }
    return resPipeData;
  }

  @Override
  public PipeData take() throws InterruptedException {
    PipeData pipeData = null;
    try {
      pullSerialNumber = commitSerialNumber + 1;
      synchronized (waitLock) {
        pipeData = pullOnePipeData(pullSerialNumber);
        if (pipeData == null) {
          waitLock.wait();
          waitLock.notifyAll();
          pipeData = pullOnePipeData(pullSerialNumber);
        }
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Blocking pull pipe data number %s error, because %s", commitSerialNumber + 1, e));
    }
    outputDeque.addFirst(pipeData);
    return pipeData;
  }

  @Override
  public void commit() {
    deletePipeData();
    deletePipeLog();
    serializeCommitSerialNumber();
  }

  private void deletePipeData() {
    while (commitSerialNumber < pullSerialNumber) {
      commitSerialNumber += 1;
      try {
        PipeData commitData = pullOnePipeData(commitSerialNumber);
        if (PipeData.Type.TSFILE.equals(commitData.getType())) {
          //          List<File> tsFiles = ((TsFilePipeData) commitData).getTsFiles();
          //          for (File file : tsFiles) {
          //            Files.deleteIfExists(file.toPath());
          //          }
        }
      } catch (IOException e) {
        logger.error(
            String.format(
                "Commit pipe data serial number %s error, because %s.", commitSerialNumber, e));
      }
    }
  }

  private void deletePipeLog() {
    if (pipeLogStartNumber.size() >= 2) {
      long nowPipeLogStartNumber;
      while (true) {
        nowPipeLogStartNumber = pipeLogStartNumber.poll();
        if (!pipeLogStartNumber.isEmpty() && pipeLogStartNumber.peek() <= commitSerialNumber) {
          try {
            Files.deleteIfExists(
                new File(pipeLogDir, SyncConstant.getPipeLogName(nowPipeLogStartNumber)).toPath());
          } catch (IOException e) {
            logger.warn(
                String.format("Delete %s-pipe.log error, because %s.", nowPipeLogStartNumber, e));
          }
        } else {
          break;
        }
      }
      pipeLogStartNumber.addFirst(nowPipeLogStartNumber);
    }
  }

  private void serializeCommitSerialNumber() {
    try {
      if (commitLogWriter == null) {
        commitLogWriter =
            new BufferedWriter(new FileWriter(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME)));
        currentCommitLogSize = 0;
      }
      commitLogWriter.newLine();
      commitLogWriter.write(String.valueOf(commitSerialNumber));
      commitLogWriter.flush();
      currentCommitLogSize += Long.BYTES;
      if (currentCommitLogSize >= SyncConstant.DEFAULT_PIPE_LOG_SIZE_IN_BYTE) {
        commitLogWriter.close();
        commitLogWriter = null;
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Serialize commit serial number %s error, because %s.", commitSerialNumber, e));
    }
  }

  /** common */
  @Override
  public void clear() {
    try {
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
      }
      if (commitLogWriter != null) {
        commitLogWriter.close();
        commitLogWriter = null;
      }

      inputDeque = null;
      pipeLogStartNumber = null;
      outputDeque = null;
      File logDir = new File(pipeLogDir);
      if (logDir.exists()) {
        FileUtils.deleteDirectory(logDir);
      }
    } catch (IOException e) {
      logger.warn(String.format("Clear pipe log dir %s error, because %s.", pipeLogDir, e));
    }
  }

  private boolean createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    return file.createNewFile();
  }

  public static List<PipeData> parsePipeLog(File file) throws IOException {
    List<PipeData> pipeData = new ArrayList<>();
    DataInputStream inputStream = new DataInputStream(new FileInputStream(file));
    try {
      while (true) {
        pipeData.add(PipeData.deserialize(inputStream));
      }
    } catch (EOFException e) {
      logger.info(String.format("Finish parsing pipeLog %s.", file.getPath()));
    } catch (IllegalPathException e) {
      logger.error(String.format("Parsing pipeLog %s error, because %s", file.getPath(), e));
      throw new IOException(e);
    } finally {
      inputStream.close();
    }
    return pipeData;
  }
}
