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

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SnapshotLogAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLogAnalyzer.class);
  private File snapshotLogFile;
  private BufferedReader reader;

  public SnapshotLogAnalyzer(File snapshotLogFile) throws FileNotFoundException {
    this.snapshotLogFile = snapshotLogFile;
    this.reader = new BufferedReader(new FileReader(snapshotLogFile));
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

  /**
   * @return The next pair of files recorded in the log. The left one is the path of source file,
   *     the right one is the path of target file
   */
  public Pair<String, String> getNextPairs() {
    if (reader == null) {
      return null;
    }
    try {
      String fileInfo = reader.readLine();
      String[] filesPath = fileInfo.split(SnapshotLogger.SPLIT_CHAR);
      if (filesPath.length != 2) {
        LOGGER.warn("Illegal file info: {} in snapshot log", fileInfo);
        return null;
      }
      return new Pair<>(filesPath[0], filesPath[1]);
    } catch (IOException e) {
      LOGGER.error("Exception occurs when analyzing snapshot log", e);
      return null;
    }
  }
}
