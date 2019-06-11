/**
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
package org.apache.iotdb.db.writelog.recover;

import java.io.File;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessor;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageGroupRecoverPerformer implements RecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupRecoverPerformer.class);

  /**
   * If the storage group is "root.a.b", then the identifier of a bufferwrite processor will be
   * "root.a.b-bufferwrite", and the identifier of an overflow processor will be
   * "root.a.b-overflow".
   */
  private String identifier;

  private String storageGroupName;

  public StorageGroupRecoverPerformer(String identifier) {
    this.identifier = identifier;
    this.storageGroupName = identifier.split("-")[0];
  }

  @Override
  public void recover() {
    recoverBufferWrite();
  }

  private void recoverBufferWrite() {
    List<String> bufferWritePathList = Directories.getInstance().getAllTsFileFolders();
    File logDirectory = new File(IoTDBDescriptor.getInstance().getConfig().getWalFolder()
        + File.separator + this.identifier);
    for (String bufferWritePath : bufferWritePathList) {
      File bufferDir = new File(bufferWritePath, storageGroupName);
      // free and close the streams under this bufferwrite directory
      if (!bufferDir.exists()) {
        continue;
      }
      // only TsFiles with a restore file should be recovered
      File[] restoreFiles = bufferDir.listFiles((fileName) ->
          fileName.getName().contains(FileNodeProcessor.RESTORE_FILE_SUFFIX));
      if (restoreFiles != null) {
        for (File restoreFile : restoreFiles) {
          File tsFile = new File(restoreFile.getParent(), restoreFile.getName()
              .replace(FileNodeProcessor.RESTORE_FILE_SUFFIX, ""));
          File[] walFiles = null;
          if (logDirectory.exists()) {
            walFiles = logDirectory.listFiles((filename) -> filename.getName().contains(restoreFile.getName()));
          }
        }
      }
    }
  }

  public String getStorageGroupName() {
    return identifier.split("-")[0];
  }
}
