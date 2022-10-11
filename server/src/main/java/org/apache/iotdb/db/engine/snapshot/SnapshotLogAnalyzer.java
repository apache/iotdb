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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class SnapshotLogAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLogAnalyzer.class);
  private final File snapshotLogFile;
  private final BufferedReader reader;
  private final String snapshotId;
  private Set<String> fileInfoSet = new HashSet<>();

  public SnapshotLogAnalyzer(File snapshotLogFile) throws IOException {
    this.snapshotLogFile = snapshotLogFile;
    this.reader = new BufferedReader(new FileReader(snapshotLogFile));
    this.snapshotId = reader.readLine();
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurs when closing log analyzer", e);
    }
  }

  public boolean hasNext() {
    try {
      return reader != null && reader.ready();
    } catch (Exception e) {
      return false;
    }
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  /**
   * Return the total num of file in this snapshot.
   *
   * @return
   */
  public int getTotalFileCountInSnapshot() throws IOException {
    reader.reset();
    String currLine;
    int cnt = 0;
    while ((currLine = reader.readLine()) != null && !currLine.equals(SnapshotLogger.END_FLAG)) {
      fileInfoSet.add(currLine);
    }
    return cnt;
  }

  public Set<String> getFileInfoSet() {
    return fileInfoSet;
  }

  /**
   * Read the tail of the log file to see if the snapshot is complete.
   *
   * @return
   */
  public boolean isSnapshotComplete() throws IOException {
    char[] endFlagInChar = new char[SnapshotLogger.END_FLAG.length()];
    long fileLength = snapshotLogFile.length();
    int endFlagLength = SnapshotLogger.END_FLAG.getBytes(StandardCharsets.UTF_8).length;
    if (fileLength < endFlagLength) {
      // this snapshot cannot be complete
      return false;
    }
    reader.mark((int) fileLength);
    reader.skip(
        (int)
            (fileLength
                - endFlagLength
                - snapshotId.getBytes(StandardCharsets.UTF_8).length
                - "\n".getBytes(StandardCharsets.UTF_8).length));
    int offset = 0;
    do {
      offset += reader.read(endFlagInChar, offset, endFlagLength - offset);
    } while (offset < endFlagLength);
    String fileEndStr = new String(endFlagInChar);
    return fileEndStr.equals(SnapshotLogger.END_FLAG);
  }
}
