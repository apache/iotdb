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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.compaction.inner.utils.SizeTieredCompactionLogger;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import java.io.File;
import java.io.IOException;

public class CompactionClearUtils {

  /** Clear all generated and merged files in the test directory */
  public static void clearAllCompactionFiles() throws IOException {
    deleteAllFilesInOneDirBySuffix("target", ".tsfile");
    deleteAllFilesInOneDirBySuffix("target", ".resource");
    deleteAllFilesInOneDirBySuffix("target", ".mods");
    deleteAllFilesInOneDirBySuffix("target", ".target");
    deleteAllFilesInOneDirBySuffix("target", SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
    //    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    //    for (File file : files) {
    //      FileUtils.delete(file);
    //    }
    //    File[] resourceFiles =
    //        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    //    for (File resourceFile : resourceFiles) {
    //      FileUtils.delete(resourceFile);
    //    }
    //    File[] mergeFiles = FSFactoryProducer.getFSFactory().listFilesBySuffix("target",
    // ".tsfile");
    //    for (File mergeFile : mergeFiles) {
    //      FileUtils.delete(mergeFile);
    //    }
    //    File[] compactionLogFiles =
    //        FSFactoryProducer.getFSFactory()
    //            .listFilesBySuffix("target", SizeTieredCompactionLogger.COMPACTION_LOG_NAME);
    //    for (File compactionLogFile : compactionLogFiles) {
    //      FileUtils.delete(compactionLogFile);
    //    }
    //    File[] modsFiles = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".mods");
    //    for (File modsFile : modsFiles) {
    //      FileUtils.delete(modsFile);
    //    }
    //    File[] targetFiles = FSFactoryProducer.getFSFactory().listFilesBySuffix("target",
    // ".target");
    //    for (File targetFile : targetFiles) {
    //      FileUtils.delete(targetFile);
    //    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  private static void deleteAllFilesInOneDirBySuffix(String dirPath, String suffix)
      throws IOException {
    File dir = new File(dirPath);
    if (!dir.isDirectory()) {
      return;
    }
    if (!dir.exists()) {
      return;
    }
    for (File f : FSFactoryProducer.getFSFactory().listFilesBySuffix(dirPath, suffix)) {
      FileUtils.delete(f);
    }
    File[] tmpFiles = dir.listFiles();
    if (tmpFiles != null) {
      for (File f : tmpFiles) {
        if (f.isDirectory()) {
          deleteAllFilesInOneDirBySuffix(f.getAbsolutePath(), suffix);
        }
      }
    }
  }
}
