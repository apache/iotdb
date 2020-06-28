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

package org.apache.iotdb.db.engine.flush;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.tsfile.utils.Pair;

public class VmLogAnalyzer {

  static final String STR_DEVICE_OFFSET_SEPERATOR = " ";

  private File logFile;

  public VmLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /**
   * @return (written device set, last offset)
   */
  public Pair<Set<String>, Long> analyze() throws IOException {
    Set<String> deviceSet = new HashSet<>();
    long offset = 0;
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        String[] resultList = currLine.split(STR_DEVICE_OFFSET_SEPERATOR);
        deviceSet.add(resultList[0]);
        offset = Long.parseLong(resultList[1]);
      }
    }
    return new Pair<>(deviceSet, offset);
  }
}
