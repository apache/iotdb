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

package org.apache.iotdb.db.engine.alter.log;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.log.TsFileIdentifier;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Alteringlogger records the progress of modifying the encoding compression method in the form of
 * text lines in the file "alter.log".
 */
public class AlteringLogger implements AutoCloseable {

  public static final String ALTERING_LOG_NAME = "alter.log";
  public static final String FLAG_DONE = "done";
  public static final String FLAG_ALTER_PARAM_BEGIN = "apb";
  public static final String FLAG_CLEAR_BEGIN = "cb";

  private final BufferedWriter logStream;

  public AlteringLogger(File logFile) throws IOException {
    logStream = new BufferedWriter(new FileWriter(logFile, true));
  }

  @Override
  public void close() throws IOException {
    logStream.close();
  }

  public synchronized void addAlterParam(
      PartialPath fullPath, TSEncoding curEncoding, CompressionType curCompressionType)
      throws IOException {
    if (fullPath == null || curEncoding == null || curCompressionType == null) {
      throw new IOException("alter params is null");
    }
    logStream.write(FLAG_ALTER_PARAM_BEGIN);
    logStream.newLine();
    logStream.write(fullPath.getFullPath());
    logStream.newLine();
    logStream.write(Byte.toString(curEncoding.serialize()));
    logStream.newLine();
    logStream.write(Byte.toString(curCompressionType.serialize()));
    logStream.newLine();
    logStream.flush();
  }

  public static void clearBegin(File logFile) throws IOException {

    try (BufferedWriter tempLogWriter = new BufferedWriter(new FileWriter(logFile, true))) {
      tempLogWriter.write(FLAG_CLEAR_BEGIN);
      tempLogWriter.newLine();
      tempLogWriter.flush();
    }
  }

  public void clearBegin() throws IOException {

    logStream.write(FLAG_CLEAR_BEGIN);
    logStream.newLine();
    logStream.flush();
  }

  /**
   * need to be thread-safe
   *
   * @param file
   * @throws IOException
   */
  public synchronized void doneFile(TsFileResource file) throws IOException {
    if (file == null) {
      throw new IOException("file is null");
    }
    logStream.write(FLAG_DONE);
    logStream.newLine();
    logStream.write(
        "" + TsFileIdentifier.getFileIdentifierFromFilePath(file.getTsFile().getAbsolutePath()));
    logStream.newLine();
    logStream.flush();
  }
}
