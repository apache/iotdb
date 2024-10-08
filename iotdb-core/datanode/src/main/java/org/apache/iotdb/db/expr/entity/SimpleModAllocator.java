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

package org.apache.iotdb.db.expr.entity;

import org.apache.iotdb.db.expr.conf.SimulationConfig;
import org.apache.iotdb.db.expr.simulator.SimpleSimulator.SimpleContext;

public class SimpleModAllocator {

  private SimulationConfig config;
  private SimpleContext context;

  public SimpleModAllocator(SimulationConfig config, SimpleContext context) {
    this.config = config;
    this.context = context;
  }

  private SimModFile tryAllocate(SimTsFile tsFile) {
    if (tsFile.hasModFile()) {
      SimModFile prevModFile = tsFile.getModFileMayAllocate();
      if (context.modFileManager.modFileList.size() < config.modFileCntThreshold) {
        // can allocate more mod file
        long totalSize = prevModFile.mods.size() * config.deletionSizeInByte;
        if (totalSize > config.modFileSizeThreshold) {
          //          System.out.printf(
          //              "When allocating new Mod File, there are %d partial deletion and %d full
          // deletion%n",
          //              prevModFile.partialDeletionCnt, prevModFile.fullDeletionCnt);
          // the previous one is already large enough, allocate a new one
          return allocateNew();
        }
      }
      // share the previous one
      return prevModFile;
    }
    return null;
  }

  private SimModFile allocateNew() {
    SimModFile newModFile = new SimModFile();
    context.modFileManager.modFileList.add(newModFile);
    context.getStatistics().modFileGeneratedCnt++;
    return newModFile;
  }

  public SimModFile allocate(SimTsFile tsFile) {
    int filePos = context.tsFileManager.tsFileList.indexOf(tsFile);

    int forwardIndex = filePos + 1;
    int backwardIndex = filePos - 1;
    while (forwardIndex < context.tsFileManager.tsFileList.size() || backwardIndex >= 0) {
      if (forwardIndex < context.tsFileManager.tsFileList.size()) {
        SimTsFile nextFile = context.tsFileManager.tsFileList.get(forwardIndex);
        SimModFile simModFile = tryAllocate(nextFile);
        if (simModFile != null) {
          return simModFile;
        }
        forwardIndex++;
      }

      if (backwardIndex >= 0) {
        SimTsFile prevFile = context.tsFileManager.tsFileList.get(backwardIndex);
        SimModFile simModFile = tryAllocate(prevFile);
        if (simModFile != null) {
          return simModFile;
        }
        backwardIndex--;
      }
    }

    // no mod File
    return allocateNew();
  }
}
