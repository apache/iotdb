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
package org.apache.iotdb.db.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLogWriter {

  private static final Logger logger = LoggerFactory.getLogger(MLogWriter.class);
  private static final String STRING_TYPE = "%s,%s,%s" + System.lineSeparator();
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private final File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel channel;
  private final AtomicInteger lineNumber;

  public MLogWriter(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lineNumber = new AtomicInteger(0);
  }

  public void close() throws IOException {
    fileOutputStream.close();
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("%s,%s,%s,%s,%s", MetadataOperationType.CREATE_TIMESERIES,
            plan.getPath().getFullPath(), plan.getDataType().serialize(),
            plan.getEncoding().serialize(), plan.getCompressor().serialize()));

    buf.append(",");
    if (plan.getProps() != null) {
      boolean first = true;
      for (Map.Entry<String, String> entry : plan.getProps().entrySet()) {
        if (first) {
          buf.append(String.format("%s=%s", entry.getKey(), entry.getValue()));
          first = false;
        } else {
          buf.append(String.format("&%s=%s", entry.getKey(), entry.getValue()));
        }
      }
    }

    buf.append(",");
    if (plan.getAlias() != null) {
      buf.append(plan.getAlias());
    }

    buf.append(",");
    if (offset >= 0) {
      buf.append(offset);
    }
    buf.append(LINE_SEPARATOR);
    channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
    lineNumber.incrementAndGet();
  }

  public void deleteTimeseries(String path) throws IOException {
    String outputStr = MetadataOperationType.DELETE_TIMESERIES + "," + path + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
  }

  public void setStorageGroup(String storageGroup) throws IOException {
    String outputStr = MetadataOperationType.SET_STORAGE_GROUP + "," + storageGroup + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void deleteStorageGroup(String storageGroup) throws IOException {
    String outputStr = MetadataOperationType.DELETE_STORAGE_GROUP + "," + storageGroup + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void setTTL(String storageGroup, long ttl) throws IOException {
    String outputStr = String.format(STRING_TYPE, MetadataOperationType.SET_TTL, storageGroup, ttl);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void changeOffset(String path, long offset) throws IOException {
    String outputStr = String.format(STRING_TYPE, MetadataOperationType.CHANGE_OFFSET, path, offset);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void changeAlias(String path, String alias) throws IOException {
    String outputStr = String.format(STRING_TYPE, MetadataOperationType.CHANGE_ALIAS, path, alias);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public static void upgradeMLog(String schemaDir, String logFileName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    File tmpLogFile = SystemFileFactory.INSTANCE.getFile(logFile.getAbsolutePath() + ".tmp");

    // if both old mlog and mlog.tmp do not exist, nothing to do
    if (!logFile.exists() && !tmpLogFile.exists()) {
      return;
    } else if (!logFile.exists() && tmpLogFile.exists()) {
      // if old mlog doesn't exsit but mlog.tmp exists, rename tmp file to mlog  
      FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);
      return;
    }

    // if both old mlog and mlog.tmp exist, delete mlog tmp, then do upgrading
    if (tmpLogFile.exists() && !tmpLogFile.delete()) {
      throw new IOException("Deleting " + tmpLogFile + "failed.");
    }
    // upgrading
    try (BufferedReader reader = new BufferedReader(new FileReader(logFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpLogFile, true))) {
      String line;
      while ((line = reader.readLine()) != null) {
        StringBuilder buf = new StringBuilder();
        buf.append(line);
        if (line.startsWith(MetadataOperationType.CREATE_TIMESERIES)) {
          buf.append(",,,");
        }
        writer.write(buf.toString());
        writer.newLine();
        writer.flush();
      }
    }
  }

  public void clear() throws IOException {
    channel.close();
    fileOutputStream.close();
    Files.delete(logFile.toPath());
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lineNumber.set(0);
  }

  int getLineNumber() {
    return lineNumber.get();
  }

  /**
   * only used for initialize a mlog file writer.
   */
  void setLineNumber(int number) {
    lineNumber.set(number);
  }
}
