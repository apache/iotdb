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
package org.apache.iotdb.lsm.manager;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaRegionConstant;
import org.apache.iotdb.lsm.engine.IRecoverable;
import org.apache.iotdb.lsm.request.IRequest;
import org.apache.iotdb.lsm.util.DiskFileNameDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** for memory structure recovery */
public class RecoverManager<T extends IRecoverable> {

  private String flushDirPath;
  private String walDirPath;
  private WALManager walManager;
  private boolean enableFlush;

  public RecoverManager(WALManager walManager, boolean enableFlush, String flushDirPath)
      throws IOException {
    this.walManager = walManager;
    this.flushDirPath = flushDirPath;
    this.walDirPath = walManager.getWalDirPath();
    this.enableFlush = enableFlush;
    initRecover();
  }

  private void initRecover() throws IOException {
    if (enableFlush) {
      checkPoint();
    }
    walManager.initRecover();
    walManager.setRecover(true);
  }

  private void checkPoint() {
    File flushDir = new File(flushDirPath);
    flushDir.mkdirs();
    File[] flushFiles = flushDir.listFiles();
    String[] flushTmpFileNames =
        Arrays.stream(flushFiles)
            .map(File::getName)
            .filter(name -> name.endsWith(TagSchemaRegionConstant.TMP))
            .toArray(String[]::new);
    Integer[] flushTmpIDs =
        Arrays.stream(flushTmpFileNames)
            .map(DiskFileNameDescriptor::getTmpFlushFileID)
            .toArray(Integer[]::new);
    File walDir = new File(walDirPath);
    walDir.mkdirs();
    File[] walFiles = walDir.listFiles();
    Set<Integer> walFileIDs =
        Arrays.stream(walFiles).map(walManager::getWalFileID).collect(Collectors.toSet());
    for (int i = 0; i < flushTmpFileNames.length; i++) {
      // have tifile tmp don't have wal, tifile is complete, need rename tifile.
      File flushTmpFile = new File(flushDirPath + File.separator + flushTmpFileNames[i]);
      String flushFileName =
          flushTmpFileNames[i].substring(
              0, flushTmpFileNames[i].length() - TagSchemaRegionConstant.TMP.length());
      if (!walFileIDs.contains(flushTmpIDs[i])) {
        File flushFile = new File(flushDirPath + File.separator + flushFileName);
        flushTmpFile.renameTo(flushFile);
      }
      // have tifile tmp and have wal, tifile is incomplete,delete tifile tmp and delete file.
      else {
        flushTmpFile.delete();
        String flushDeleteFileName =
            DiskFileNameDescriptor.getFlushDeleteFileNameFromFlushFileName(flushFileName);
        File flushDeleteFile = new File(flushDirPath + File.separator + flushDeleteFileName);
        if (flushDeleteFile.exists()) {
          flushDeleteFile.delete();
        }
      }
    }
  }

  /**
   * recover
   *
   * @param t extends IRecoverable
   */
  public void recover(T t) throws IOException {
    while (true) {
      IRequest request = walManager.recover();
      if (request == null) {
        walManager.setRecover(false);
        return;
      }
      t.recover(request);
    }
  }
}
