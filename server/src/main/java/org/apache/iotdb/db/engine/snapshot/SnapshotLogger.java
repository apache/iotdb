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
package org.apache.iotdb.db.engine.snapshot;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class SnapshotLogger implements AutoCloseable {
  public static final String SNAPSHOT_LOG_NAME = "snapshot.log";
  public static final String SPLIT_CHAR = " ";
  public static final String END_FLAG = "END";
  public static final int FILE_NAME_OFFSET = 1;
  public static final int TIME_PARTITION_OFFSET = 2;
  public static final int SEQUENCE_OFFSET = 5;

  private File logFile;
  private BufferedOutputStream os;

  public SnapshotLogger(File logFile) throws IOException {
    this.logFile = logFile;
    if (!logFile.getParentFile().exists() && !logFile.getParentFile().mkdirs()) {
      throw new IOException("Cannot create parent folder for " + logFile.getAbsolutePath());
    }
    if (!this.logFile.createNewFile()) {
      throw new IOException("Cannot create file " + logFile.getAbsolutePath());
    }
    os = new BufferedOutputStream(new FileOutputStream(logFile));
  }

  @Override
  public void close() throws Exception {
    os.close();
  }

  /**
   * Log the logical info for the link file, including its file name, time partition, data region
   * id, database name, sequence or not.
   *
   * @param sourceFile
   * @throws IOException
   */
  public void logFile(File sourceFile) throws IOException {
    String[] splitInfo =
        sourceFile.getAbsolutePath().split(File.separator.equals("\\") ? "\\\\" : "/");
    int length = splitInfo.length;
    String fileName = splitInfo[length - FILE_NAME_OFFSET];
    String timePartition = splitInfo[length - TIME_PARTITION_OFFSET];
    String sequence = splitInfo[length - SEQUENCE_OFFSET];
    os.write(fileName.getBytes(StandardCharsets.UTF_8));
    os.write(SPLIT_CHAR.getBytes(StandardCharsets.UTF_8));
    os.write(timePartition.getBytes(StandardCharsets.UTF_8));
    os.write(SPLIT_CHAR.getBytes(StandardCharsets.UTF_8));
    os.write(sequence.getBytes(StandardCharsets.UTF_8));
    os.write("\n".getBytes(StandardCharsets.UTF_8));
    os.flush();
  }

  /**
   * Log the snapshot id to identify this snapshot.
   *
   * @param id
   * @throws IOException
   */
  public void logSnapshotId(String id) throws IOException {
    os.write(id.getBytes(StandardCharsets.UTF_8));
    os.write("\n".getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Log the end of the snapshot.
   *
   * @throws IOException
   */
  public void logEnd() throws IOException {
    os.write(END_FLAG.getBytes(StandardCharsets.UTF_8));
  }

  public void cleanUpWhenFailed() throws IOException {
    os.close();
    Files.delete(logFile.toPath());
  }
}
