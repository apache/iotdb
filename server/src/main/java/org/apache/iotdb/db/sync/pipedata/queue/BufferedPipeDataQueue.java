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
package org.apache.iotdb.db.sync.pipedata.queue;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.commons.sync.utils.SyncPathUtil;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
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
  private DataOutputStream commitLogWriter;
  private long currentCommitLogSize;

  public BufferedPipeDataQueue(String pipeLogDir) {
    this.pipeLogDir = pipeLogDir;

    this.lastMaxSerialNumber = 0;
    this.pipeLogStartNumber = new LinkedBlockingDeque<>();

    this.outputDeque = new LinkedBlockingDeque<>();
    this.pullSerialNumber = Long.MIN_VALUE;
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
      if (file.getName().endsWith(SyncConstant.PIPE_LOG_NAME_SUFFIX) && file.length() > 0) {
        startNumbers.add(SyncPathUtil.getSerialNumberFromPipeLogName(file.getName()));
      }
    if (!startNumbers.isEmpty()) {
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
        new File(pipeLogDir, SyncPathUtil.getPipeLogName(pipeLogStartNumber.peekLast()));
    try {
      List<PipeData> recoverPipeData = parsePipeLog(writingPipeLog);
      int recoverPipeDataSize = recoverPipeData.size();
      lastMaxSerialNumber =
          recoverPipeDataSize == 0
              ? pipeLogStartNumber.peekLast() - 1
              : recoverPipeData.get(recoverPipeDataSize - 1).getSerialNumber();
    } catch (IOException e) {
      logger.error(
          String.format("Can not recover inputQueue from %s.", writingPipeLog.getPath()), e);
    }
  }

  private void recoverCommitSerialNumber() {
    File commitLog = new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME);
    if (!commitLog.exists()) {
      if (!pipeLogStartNumber.isEmpty()) {
        commitSerialNumber = pipeLogStartNumber.peek() - 1;
      }
      return;
    }

    try (RandomAccessFile raf = new RandomAccessFile(commitLog, "r")) {
      if (raf.length() >= Long.BYTES) {
        raf.seek(raf.length() - Long.BYTES);
        commitSerialNumber = raf.readLong();
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "deserialize remove serial number error, remove serial number has been set to %d.",
              commitSerialNumber),
          e);
    }
  }

  private void recoverOutputDeque() {
    if (pipeLogStartNumber.isEmpty()) {
      return;
    }

    File readingPipeLog =
        new File(pipeLogDir, SyncPathUtil.getPipeLogName(pipeLogStartNumber.peek()));
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
          String.format("Recover output deque from pipe log %s error.", readingPipeLog.getPath()),
          e);
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
        logger.error(String.format("Move to next pipe log %s error.", pipeData), e);
      }
    }
    synchronized (waitLock) {
      if (!inputDeque.offer(pipeData)) {
        waitLock.notifyAll();
        return false;
      }
      waitLock.notifyAll();
    }

    try {
      writeToDisk(pipeData);
    } catch (IOException e) {
      logger.error(String.format("Record pipe data %s error.", pipeData), e);
      return false;
    }
    return true;
  }

  private synchronized void moveToNextPipeLog(long startSerialNumber) throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
    File newPipeLog = new File(pipeLogDir, SyncPathUtil.getPipeLogName(startSerialNumber));
    SyncPathUtil.createFile(newPipeLog);

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
  private synchronized PipeData pullOnePipeData(long lastSerialNumber) throws IOException {
    long serialNumber = lastSerialNumber + 1;
    if (!outputDeque.isEmpty()) {
      return outputDeque.poll();
    } else if (outputDeque != inputDeque) {
      if (pipeLogStartNumber.isEmpty() || lastSerialNumber == Long.MIN_VALUE) {
        return null;
      }

      if (serialNumber > pipeLogStartNumber.peekLast()) {
        return null;
      } else if (serialNumber == pipeLogStartNumber.peekLast() && inputDeque != null) {
        outputDeque = inputDeque;
      } else {
        long nextStartNumber =
            pipeLogStartNumber.stream().filter(o -> o >= serialNumber).findFirst().get();
        List<PipeData> parsePipeData =
            parsePipeLog(new File(pipeLogDir, SyncPathUtil.getPipeLogName(nextStartNumber)));
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
    throw new NotImplementedException("Not implement pull");
  }

  @Override
  public PipeData take() throws InterruptedException {
    PipeData pipeData = null;
    try {
      synchronized (waitLock) {
        while ((pipeData = pullOnePipeData(commitSerialNumber)) == null) {
          waitLock.wait();
          waitLock.notifyAll();
        }
      }
    } catch (IOException e) {
      logger.error(
          String.format("Blocking pull pipe data number %s error.", commitSerialNumber + 1), e);
    }
    outputDeque.addFirst(pipeData);
    pullSerialNumber = pipeData.getSerialNumber();
    return pipeData;
  }

  @Override
  public void commit() {
    if (pullSerialNumber == Long.MIN_VALUE) {
      return;
    }
    commit(pullSerialNumber);
  }

  @Override
  public void commit(long serialNumber) {
    deletePipeData(serialNumber);
    deletePipeLog();
    serializeCommitSerialNumber();
  }

  private void deletePipeData(long serialNumber) {
    while (commitSerialNumber < serialNumber) {
      PipeData commitData = null;
      try {
        commitData = pullOnePipeData(commitSerialNumber);
        if (commitData == null) {
          return;
        }
        if (PipeData.PipeDataType.TSFILE.equals(commitData.getPipeDataType())) {
          List<File> tsFiles = ((TsFilePipeData) commitData).getTsFiles(false);
          for (File file : tsFiles) {
            Files.deleteIfExists(file.toPath());
          }
        }
      } catch (IOException e) {
        logger.error(
            String.format("Commit pipe data serial number %s error.", commitSerialNumber), e);
      }
      if (commitData != null) {
        commitSerialNumber = commitData.getSerialNumber();
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
                new File(pipeLogDir, SyncPathUtil.getPipeLogName(nowPipeLogStartNumber)).toPath());
          } catch (IOException e) {
            logger.warn(String.format("Delete %s-pipe.log error.", nowPipeLogStartNumber), e);
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
            new DataOutputStream(
                new FileOutputStream(new File(pipeLogDir, SyncConstant.COMMIT_LOG_NAME)));
        currentCommitLogSize = 0;
      }
      commitLogWriter.writeLong(commitSerialNumber);
      commitLogWriter.flush();
      currentCommitLogSize += Long.BYTES;
      if (currentCommitLogSize >= SyncConstant.DEFAULT_PIPE_LOG_SIZE_IN_BYTE) {
        commitLogWriter.close();
        commitLogWriter = null;
      }
    } catch (IOException e) {
      logger.error(
          String.format("Serialize commit serial number %s error.", commitSerialNumber), e);
    }
  }

  /** common */
  @Override
  public synchronized boolean isEmpty() {
    if (outputDeque == null || pipeLogStartNumber.isEmpty()) {
      return true;
    }
    return pipeLogStartNumber.size() == 1
        && outputDeque.isEmpty()
        && (inputDeque == null || inputDeque.isEmpty());
  }

  @Override
  public void close() {
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
    } catch (IOException e) {
      logger.warn(String.format("Close pipe log dir %s error.", pipeLogDir), e);
    }
  }

  @Override
  public void clear() {
    close();

    File logDir = new File(pipeLogDir);
    if (logDir.exists()) {
      FileUtils.deleteDirectory(logDir);
    }
  }

  public static List<PipeData> parsePipeLog(File file) throws IOException {
    List<PipeData> pipeData = new ArrayList<>();
    try (DataInputStream inputStream = new DataInputStream(new FileInputStream(file))) {
      while (true) {
        pipeData.add(PipeData.createPipeData(inputStream));
      }
    } catch (EOFException e) {
    } catch (IllegalPathException e) {
      logger.error(String.format("Parsing pipeLog %s error.", file.getPath()), e);
      throw new IOException(e);
    }
    return pipeData;
  }
}
