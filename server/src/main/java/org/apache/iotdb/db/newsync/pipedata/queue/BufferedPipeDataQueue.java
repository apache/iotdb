package org.apache.iotdb.db.newsync.pipedata.queue;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class BufferedPipeDataQueue implements PipeDataQueue {
  private static final Logger logger = LoggerFactory.getLogger(BufferedPipeDataQueue.class);

  private final String pipeLogDir;

  private long lastMaxSerialNumber;
  private BlockingDeque<PipeData> inputDeque;

  private BlockingDeque<Long> pipeLogStartNumber;
  private DataOutputStream outputStream;
  private long currentPipeLogSize;

  private BlockingDeque<PipeData> outputDeque;
  private long commitSerialNumber;
  private BufferedWriter commitSerialNumberWriter;
  private long currentCommitLogSize;

  public BufferedPipeDataQueue(String pipeLogDir) {
    this.pipeLogDir = pipeLogDir;

    this.lastMaxSerialNumber = 0;
    this.inputDeque = new LinkedBlockingDeque<>();

    this.pipeLogStartNumber = new LinkedBlockingDeque<>();

    recover();
  }

  /** recover */
  private void recover() {
    if (!new File(pipeLogDir).exists()) {
      return;
    }

    recoverPipeLogStartNumber();
    recoverLastMaxSerialNumber();
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
      lastMaxSerialNumber = recoverPipeData.get(recoverPipeDataSize - 1).getSerialNumber();
    } catch (IOException e) {
      logger.error(
          String.format(
              "Can not recover inputQueue from %s, because %s.", writingPipeLog.getPath(), e));
    }
  }

  public long getLastMaxSerialNumber() {
    return lastMaxSerialNumber;
  }

  /** input */
  @Override
  public boolean offer(PipeData pipeData) {
    if (!inputDeque.offer(pipeData)) {
      return false;
    }

    try {
      writeToDisk(pipeData);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  private void writeToDisk(PipeData pipeData) throws IOException {
    // skip trick

    if (outputStream == null) {
      moveToNextPipeLog(pipeData.getSerialNumber());
    }
    currentPipeLogSize += pipeData.serialize(outputStream);
    outputStream.flush();
    if (currentPipeLogSize > SyncConstant.DEFAULT_PIPE_LOG_SIZE_IN_BYTE) {
      moveToNextPipeLog(pipeData.getSerialNumber());
    }
  }

  private void moveToNextPipeLog(long startSerialNumber) throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
    File newPipeLog = new File(pipeLogDir, SyncConstant.getPipeLogName(startSerialNumber));
    createFile(newPipeLog);

    outputStream = new DataOutputStream(new FileOutputStream(newPipeLog));
    pipeLogStartNumber.offer(startSerialNumber);
    currentPipeLogSize = 0;

    inputDeque = new LinkedBlockingDeque<>();
  }

  /** output */
  private PipeData pullOnePipeData(long serialNumber) throws IOException {
    if (!outputDeque.isEmpty()) {
      return outputDeque.poll();
    } else if (outputDeque != inputDeque) {
      if (pipeLogStartNumber.size() > 1) {
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
        for (int i = size - 1;i >= 0; --i) {
          pipeLogStartNumber.addFirst(pullPipeLogStartNumber.get(i));
        }

        List<PipeData> parsePipeData =
            parsePipeLog(
                new File(pipeLogDir, SyncConstant.getPipeLogName(writingPipeLogStartNumber)));
        int parsePipeDataSize = parsePipeData.size();
        outputDeque = new LinkedBlockingDeque<>();
        for (int i = 0; i < parsePipeDataSize; i++) {
          outputDeque.offer(parsePipeData.get(i));
        }
      } else {
        outputDeque = inputDeque;
      }
      return outputDeque.poll();
    }
    return null;
  }

  @Override
  public List<PipeData> pull(long serialNumber) {
    List<PipeData> resPipeData = new ArrayList<>();

    long pullSerialNumber = commitSerialNumber;
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
  public PipeData blockingPull() {
    PipeData pipeData = null;
    try {
      if (outputDeque != inputDeque) {
        pipeData = pullOnePipeData(commitSerialNumber + 1);
      } else {
        pipeData = outputDeque.take();
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Blocking pull pipe data number %s error, because %s", commitSerialNumber + 1, e));
    } catch (InterruptedException e) {
      logger.warn(String.format("Be interrupted when waiting for pipe data, because %s", e));
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
    while (commitSerialNumber < commitSerialNumber + 1) {
      commitSerialNumber += 1;
      try {
        PipeData commitData = pullOnePipeData(commitSerialNumber);
        if (PipeData.Type.TSFILE.equals(commitData.getType())) {
          List<File> tsFiles = ((TsFilePipeData) commitData).getTsFiles();
          for (File file : tsFiles) {
            Files.deleteIfExists(file.toPath());
          }
        }
      } catch (IOException e) {
        logger.error(
            String.format(
                "Commit pipe data serial number %s error, because %s.", commitSerialNumber, e));
        break;
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
      if (commitSerialNumberWriter == null) {
        commitSerialNumberWriter =
            new BufferedWriter(new FileWriter(new File(pipeLogDir, SyncConstant.REMOVE_LOG_NAME)));
        currentCommitLogSize = 0;
      }
      commitSerialNumberWriter.write(String.valueOf(commitSerialNumber));
      commitSerialNumberWriter.newLine();
      commitSerialNumberWriter.flush();
      currentCommitLogSize += Long.BYTES;
      if (currentCommitLogSize >= SyncConstant.DEFAULT_PIPE_LOG_SIZE_IN_BYTE) {
        commitSerialNumberWriter.close();
        commitSerialNumberWriter = null;
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
      if (commitSerialNumberWriter != null) {
        commitSerialNumberWriter.close();
        commitSerialNumberWriter = null;
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
