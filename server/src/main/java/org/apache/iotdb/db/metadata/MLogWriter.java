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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLogWriter {

  private static final Logger logger = LoggerFactory.getLogger(MLogWriter.class);
  private static final String STRING_TYPE = "%s,%s,%s"; 
  private File logFile;
  private BufferedWriter writer;
  private int lineNumber;

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
    FileWriter fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
  }

  public void close() throws IOException {
    writer.close();
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws IOException {
    writer.write(String.format("%s,%s,%s,%s,%s", MetadataOperationType.CREATE_TIMESERIES,
        plan.getPath().getFullPath(), plan.getDataType().serialize(),
        plan.getEncoding().serialize(), plan.getCompressor().serialize()));

    writer.write(",");
    if (plan.getProps() != null) {
      boolean first = true;
      for (Map.Entry<String, String> entry : plan.getProps().entrySet()) {
        if (first) {
          writer.write(String.format("%s=%s", entry.getKey(), entry.getValue()));
          first = false;
        } else {
          writer.write(String.format("&%s=%s", entry.getKey(), entry.getValue()));
        }
      }
    }

    writer.write(",");
    if (plan.getAlias() != null) {
      writer.write(plan.getAlias());
    }

    writer.write(",");
    if (offset >= 0) {
      writer.write(String.valueOf(offset));
    }
    newLine();
  }

  public void deleteTimeseries(String path) throws IOException {
    writer.write(MetadataOperationType.DELETE_TIMESERIES + "," + path);
    newLine();
  }

  public void setStorageGroup(String storageGroup) throws IOException {
    writer.write(MetadataOperationType.SET_STORAGE_GROUP + "," + storageGroup);
    newLine();
  }

  public void deleteStorageGroup(String storageGroup) throws IOException {
    writer.write(MetadataOperationType.DELETE_STORAGE_GROUP + "," + storageGroup);
    newLine();
  }

  public void setTTL(String storageGroup, long ttl) throws IOException {
    writer.write(String.format(STRING_TYPE, MetadataOperationType.SET_TTL, storageGroup, ttl));
    newLine();
  }

  public void changeOffset(String path, long offset) throws IOException {
    writer.write(String.format(STRING_TYPE, MetadataOperationType.CHANGE_OFFSET, path, offset));
    newLine();
  }

  public void changeAlias(String path, String alias) throws IOException {
    writer.write(String.format(STRING_TYPE, MetadataOperationType.CHANGE_ALIAS, path, alias));
    newLine();
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
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpLogFile, true));) {
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
    writer.close();
    Files.delete(logFile.toPath());
    FileWriter fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
    lineNumber = 0;
  }

  private void newLine() throws IOException {
    writer.newLine();
    writer.flush();
    ++lineNumber;
  }

  int getLineNumber() {
    return lineNumber;
  }

  /**
   * only used for initialize a mlog file writer.
   */
  void setLineNumber(int number) {
    lineNumber = number;
  }
}
