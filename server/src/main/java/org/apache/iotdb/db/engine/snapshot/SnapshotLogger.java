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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class SnapshotLogger implements AutoCloseable {
  public static final String SNAPSHOT_LOG_NAME = "snapshot.log";
  public static final String SPLIT_CHAR = "#";

  private File logFile;
  private BufferedOutputStream os;

  public SnapshotLogger(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    os = new BufferedOutputStream(new FileOutputStream(logFile));
  }

  @Override
  public void close() throws Exception {
    os.close();
  }

  public void logFile(String sourceFile, String linkFile) throws IOException {
    os.write(sourceFile.getBytes(StandardCharsets.UTF_8));
    os.write(SPLIT_CHAR.getBytes(StandardCharsets.UTF_8));
    os.write(linkFile.getBytes(StandardCharsets.UTF_8));
    os.write("\n".getBytes(StandardCharsets.UTF_8));
    os.flush();
  }

  public void cleanUpWhenFailed() throws IOException {
    os.close();
    Files.delete(logFile.toPath());
  }
}
