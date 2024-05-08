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
import java.util.HashSet;
import java.util.Set;

public class SnapshotLogAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLogAnalyzer.class);
  private final File snapshotLogFile;
  private final BufferedReader reader;
  private String snapshotId;
  private boolean complete;
  private Set<String> fileInfoSet = new HashSet<>();

  public SnapshotLogAnalyzer(File snapshotLogFile) throws IOException {
    this.snapshotLogFile = snapshotLogFile;
    this.reader = new BufferedReader(new FileReader(snapshotLogFile));
    this.analyze();
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurs when closing log analyzer", e);
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
    return fileInfoSet.size();
  }

  public Set<String> getFileInfoSet() {
    return fileInfoSet;
  }

  private void analyze() throws IOException {
    try {
      snapshotId = reader.readLine();
      String line;
      while ((line = reader.readLine()) != null && !line.equals(SnapshotLogger.END_FLAG)) {
        fileInfoSet.add(line);
      }
      complete = line != null;
    } finally {
      reader.close();
    }
  }

  public boolean isSnapshotComplete() throws IOException {
    return complete;
  }
}
